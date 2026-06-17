package main

import (
	"context"
	"log"
	"time"
)

// =========================================================================
// HEALTH CHECKING & SELF-HEALING REPAIR SERVICE
// =========================================================================

func runHealthChecker(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			chunks, err := chunksDB.List()
			if err != nil {
				continue
			}

			replicationFactor := getReplicationFactor()

			for hash, chunkMeta := range chunks {
				if chunkMeta.Status == "replicating" || chunkMeta.Status == "deleting" {
					continue
				}

				dirty := false
				skipHeal := false

				for provName, replicaMeta := range chunkMeta.Replicas {
					var provider Provider
					for _, p := range activeProviders {
						if p.Name() == provName {
							provider = p
							break
						}
					}

					if provider == nil {
						continue
					}

					exists := provider.Check(context.Background(), replicaMeta)
					chunkMeta.LastChecked[provName] = time.Now()
					dirty = true

					if !exists {
						if currentMeta, err := chunksDB.Get(hash); err != nil || currentMeta.Status == "deleting" {
							skipHeal = true
							break
						}
						log.Printf("[HealthCheck] ALARM: Chunk %s is missing from provider %s!", hash, provName)
						delete(chunkMeta.Replicas, provName)
						delete(chunkMeta.LastChecked, provName)
					}
				}

				if skipHeal {
					continue
				}

				if len(chunkMeta.Replicas) < replicationFactor {
					log.Printf("[Self-Healing] Alarm triggered for chunk %s. Replicas: %d/%d", hash, len(chunkMeta.Replicas), replicationFactor)
					
					chunkMeta.Status = "replicating"
					_ = chunksDB.Put(hash, chunkMeta)

					globalReplicationQueue.Push(ReplicationTask{
						ChunkHash: hash,
						ChunkSize: chunkMeta.Size,
						AddedAt:   time.Now(),
					})
				} else if dirty {
					_ = chunksDB.Put(hash, chunkMeta)
				}
			}
		}
	}
}
