package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// =========================================================================
// CENTRAL REPLICATION QUEUE & BACKGROUND SCHEDULING
// =========================================================================

var schedulerMu sync.Mutex
var hashInFlight sync.Map // string (hash) -> chan struct{} (closes when done)

type ReplicationTask struct {
	ChunkHash string
	ChunkSize int64
	AddedAt   time.Time
}

type ReplicationQueue struct {
	mu    sync.Mutex
	tasks []ReplicationTask
}

func NewReplicationQueue() *ReplicationQueue {
	return &ReplicationQueue{}
}

var routerNotify = make(chan struct{}, 1000)
var chunkReadyNotify = make(chan string, 1000) // Signal when a chunk reaches "ready" status

func (q *ReplicationQueue) Push(task ReplicationTask) {
	q.mu.Lock()
	exists := false
	for _, t := range q.tasks {
		if t.ChunkHash == task.ChunkHash {
			exists = true
			break
		}
	}
	if !exists {
		q.tasks = append(q.tasks, task)
	}
	q.mu.Unlock()

	select {
	case routerNotify <- struct{}{}:
	default:
	}
}

var globalReplicationQueue = NewReplicationQueue()

// Concurrency tracking structures for the central router
var activeUploadsMu sync.Mutex
var chunkActiveUploads = make(map[string][]string)     // chunkHash -> list of provider names currently uploading it
var providerActiveChunks = make(map[string][]string)   // providerName -> list of chunk hashes currently being uploaded by this provider
var providerActiveCounts = make(map[string]int)        // providerName -> count of active uploads

func getProviderConcurrencyLimit(p Provider) int {
	if p.Type() == "telegram" {
		return 2
	}
	return 5 // Default concurrency limit for Discord bot API
}

func startCentralReplicationRouter(ctx context.Context, q *ReplicationQueue) {
	log.Printf("[Router] Starting central replication router...")
	ticker := time.NewTicker(1 * time.Second) // Periodic fallback scan
	defer ticker.Stop()

	// Initial scan at startup
	processQueue(ctx, q)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Router] Stopping central replication router due to context cancellation.")
			return
		case <-routerNotify:
			processQueue(ctx, q)
		case <-ticker.C:
			processQueue(ctx, q)
		}
	}
}

// selectBestProvider picks the provider with the most available capacity for load-balancing.
func selectBestProvider(task ReplicationTask, chunkMeta ChunkMeta) Provider {
	var bestProvider Provider
	maxScore := int64(-1)

	for _, p := range activeProviders {
		// Skip if chunk is too large for this provider
		if p.MaxChunkSize() != -1 && task.ChunkSize > p.MaxChunkSize() {
			continue
		}

		// Skip if provider already has a replica of this chunk
		if _, exists := chunkMeta.Replicas[p.Name()]; exists {
			continue
		}

		// Skip if already uploading this chunk to this provider
		isUploadingThis := false
		for _, hash := range providerActiveChunks[p.Name()] {
			if hash == task.ChunkHash {
				isUploadingThis = true
				break
			}
		}
		if isUploadingThis {
			continue
		}

		// Calculate available slots for this provider
		activeCount := providerActiveCounts[p.Name()]
		limit := getProviderConcurrencyLimit(p)
		availableSlots := int64(limit - activeCount)

		// Calculate max chunk size for this provider (use default if unlimited)
		maxChunkSize := p.MaxChunkSize()
		if maxChunkSize == -1 {
			maxChunkSize = DefaultChunkSize
		}

		// Score: availableSlots * maxChunkSize (balance both concurrency and capacity)
		score := availableSlots * maxChunkSize

		// Select provider with highest score (load balancing)
		if score >= maxScore {
			maxScore = score
			bestProvider = p
		}
	}

	return bestProvider
}

func processQueue(ctx context.Context, q *ReplicationQueue) {
	q.mu.Lock()
	defer q.mu.Unlock()

	requiredReplication := getReplicationFactor()

	for idx := 0; idx < len(q.tasks); {
		task := q.tasks[idx]

		chunkMeta, err := chunksDB.Get(task.ChunkHash)
		if err != nil {
			idx++
			continue
		}

		// If chunk has met replication factor requirements, remove it from queue
		if len(chunkMeta.Replicas) >= requiredReplication {
			q.tasks = append(q.tasks[:idx], q.tasks[idx+1:]...)
			if chunkMeta.Status != "ready" {
				chunkMeta.Status = "ready"
				_ = chunksDB.Put(task.ChunkHash, chunkMeta)
			}
			continue
		}

		activeUploadsMu.Lock()
		activeProvs := chunkActiveUploads[task.ChunkHash]
		
		// If replicas + current active uploads meet requirement, skip dispatching another for now
		if len(chunkMeta.Replicas)+len(activeProvs) >= requiredReplication {
			activeUploadsMu.Unlock()
			idx++
			continue
		}

		// Select the best provider based on available capacity for load-balancing
		selectedProvider := selectBestProvider(task, chunkMeta)

		if selectedProvider != nil {
			group := []ReplicationTask{task}
			combinedSize := task.ChunkSize
			maxSz := selectedProvider.MaxChunkSize()
			if maxSz == -1 {
				maxSz = DefaultChunkSize
			}

			tasksToRemove := []int{idx}

			// Greedy look-ahead for aggregation
			for j := idx + 1; j < len(q.tasks); j++ {
				nextTask := q.tasks[j]
				
				// Check if nextTask already has a replica on selectedProvider
				nextMeta, errMeta := chunksDB.Get(nextTask.ChunkHash)
				if errMeta != nil {
					continue
				}
				if _, exists := nextMeta.Replicas[selectedProvider.Name()]; exists {
					continue
				}

				// Check size limit
				if combinedSize+nextTask.ChunkSize > maxSz {
					break // Stop aggregating if we exceed provider limit
				}

				// Check if it's already being uploaded to this provider
				isUploading := false
				for _, h := range providerActiveChunks[selectedProvider.Name()] {
					if h == nextTask.ChunkHash {
						isUploading = true
						break
					}
				}
				if isUploading {
					continue
				}

				// Add to group
				group = append(group, nextTask)
				combinedSize += nextTask.ChunkSize
				tasksToRemove = append(tasksToRemove, j)
			}

			// Register in-flight metrics for the whole group
			for _, t := range group {
				chunkActiveUploads[t.ChunkHash] = append(chunkActiveUploads[t.ChunkHash], selectedProvider.Name())
				providerActiveChunks[selectedProvider.Name()] = append(providerActiveChunks[selectedProvider.Name()], t.ChunkHash)
				providerActiveCounts[selectedProvider.Name()]++
			}
			activeUploadsMu.Unlock()

			log.Printf("[Router] Dispatching group of %d chunks to provider %s (aggregated upload)", len(group), selectedProvider.Name())

			// Dispatch aggregated upload asynchronously
			go performRouterUpload(ctx, selectedProvider, group, q)

			// Remove aggregated tasks from queue in reverse order
			sort.Ints(tasksToRemove)
			for i := len(tasksToRemove) - 1; i >= 0; i-- {
				remIdx := tasksToRemove[i]
				q.tasks = append(q.tasks[:remIdx], q.tasks[remIdx+1:]...)
			}
		} else {
			activeUploadsMu.Unlock()
			idx++
		}
	}
}

func performRouterUpload(ctx context.Context, p Provider, group []ReplicationTask, q *ReplicationQueue) {
	defer func() {
		activeUploadsMu.Lock()
		providerActiveCounts[p.Name()]--

		for _, t := range group {
			// Remove from chunkActiveUploads
			provs := chunkActiveUploads[t.ChunkHash]
			for i, name := range provs {
				if name == p.Name() {
					chunkActiveUploads[t.ChunkHash] = append(provs[:i], provs[i+1:]...)
					break
				}
			}
			if len(chunkActiveUploads[t.ChunkHash]) == 0 {
				delete(chunkActiveUploads, t.ChunkHash)
			}

			// Remove from providerActiveChunks
			chunks := providerActiveChunks[p.Name()]
			for i, hash := range chunks {
				if hash == t.ChunkHash {
					providerActiveChunks[p.Name()] = append(chunks[:i], chunks[i+1:]...)
					break
				}
			}
		}
		activeUploadsMu.Unlock()

		// Trigger router to check if there are other compatible tasks now that a slot is free
		select {
		case routerNotify <- struct{}{}:
		default:
		}
	}()

	var combinedData []byte
	type taskRange struct {
		task   ReplicationTask
		offset int64
		length int64
	}
	var ranges []taskRange

	for _, task := range group {
		cachePath := filepath.Join("./cache", task.ChunkHash)
		chunkData, errRead := os.ReadFile(cachePath)
		if errRead != nil {
			log.Printf("[Router %s] Cache miss for chunk %s. Searching surviving replicas...", p.Name(), task.ChunkHash[:8])
			chunkMeta, errGet := chunksDB.Get(task.ChunkHash)
			if errGet == nil {
				for _, replicaMeta := range chunkMeta.Replicas {
					provName, _ := replicaMeta["provider"].(string)
					var survivingProvider Provider
					for _, ap := range activeProviders {
						if ap.Name() == provName {
							survivingProvider = ap
							break
						}
					}
					if survivingProvider != nil {
						log.Printf("[Router %s] Downloading surviving replica from %s...", p.Name(), provName)
						chunkData, errRead = survivingProvider.Download(ctx, replicaMeta)
						if errRead == nil && len(chunkData) > 0 {
							evictCacheIfNecessary(int64(len(chunkData)))
							_ = os.WriteFile(cachePath, chunkData, 0644)
							break
						}
					}
				}
			}
		}

		if len(chunkData) == 0 {
			log.Printf("[Router %s] ERROR: Chunk %s has no cache and no surviving replicas! Aborting group upload.", p.Name(), task.ChunkHash[:8])
			return
		}

		ranges = append(ranges, taskRange{
			task:   task,
			offset: int64(len(combinedData)),
			length: int64(len(chunkData)),
		})
		combinedData = append(combinedData, chunkData...)
	}

	log.Printf("[Router %s] Replicating aggregated group of %d chunks (%d bytes)...", p.Name(), len(group), len(combinedData))
	
	// Use the hash of the first chunk as the identifier for the aggregated blob
	replMeta, errUpload := p.Upload(ctx, group[0].ChunkHash, combinedData)
	if errUpload != nil {
		log.Printf("[Router %s] ERROR: Failed to upload aggregated group: %v. Retrying later.", p.Name(), errUpload)
		p.ReportError(errUpload)
		time.Sleep(3 * time.Second)
		return
	}

	for _, r := range ranges {
		chunkMeta, errGet := chunksDB.Get(r.task.ChunkHash)
		if errGet == nil {
			if chunkMeta.Replicas == nil {
				chunkMeta.Replicas = make(map[string]map[string]interface{})
			}
			
			// Map the replica with offset and length
			meta := make(map[string]interface{})
			for k, v := range replMeta {
				meta[k] = v
			}
			meta["offset"] = r.offset
			meta["length"] = r.length
			
			chunkMeta.Replicas[p.Name()] = meta
			
			if chunkMeta.LastChecked == nil {
				chunkMeta.LastChecked = make(map[string]time.Time)
			}
			chunkMeta.LastChecked[p.Name()] = time.Now()

			// Increment reference count for the aggregated container
			containerID := fmt.Sprintf("%s:%v", p.Name(), replMeta["msg_id"])
			if errRef := chunksDB.IncRef(containerID); errRef != nil {
				log.Printf("[Router %s] ERROR: Failed to increment ref count for %s: %v", p.Name(), containerID, errRef)
			}

			requiredReplication := getReplicationFactor()
			if len(chunkMeta.Replicas) >= requiredReplication {
				chunkMeta.Status = "ready"
				select {
				case chunkReadyNotify <- r.task.ChunkHash:
				default:
				}
			}
			_ = chunksDB.Put(r.task.ChunkHash, chunkMeta)
		}
	}

	log.Printf("[Router %s] Successfully replicated aggregated group of %d chunks!", p.Name(), len(group))
}
