package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bazil.org/fuse"
	"github.com/gorilla/websocket"
)

// =========================================================================
// WEBSOCKET BACKEND SERVER IMPLEMENTATION
// =========================================================================

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4 * 1024 * 1024,
	WriteBufferSize: 4 * 1024 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type ServerWriteBuffer struct {
	mu    sync.Mutex
	data  []byte
	dirty bool
}

var serverActiveBuffers = struct {
	sync.Mutex
	buffers map[string]*ServerWriteBuffer
}{buffers: make(map[string]*ServerWriteBuffer)}

func getServerWriteBuffer(path string) (*ServerWriteBuffer, error) {
	serverActiveBuffers.Lock()
	buf, exists := serverActiveBuffers.buffers[path]
	if !exists {
		// Initialize by loading existing file data if any exists in db
		existingData, err := readRawBytes(path, 0, -1)
		if err != nil && err != fuse.ENOENT {
			serverActiveBuffers.Unlock()
			return nil, err
		}
		buf = &ServerWriteBuffer{
			data: existingData,
		}
		serverActiveBuffers.buffers[path] = buf
	}
	serverActiveBuffers.Unlock()
	return buf, nil
}

func StartWebSocketServer(port string, serverPassword string) {
	log.Printf("[Server] Starting backend WebSocket server on :%s...", port)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[Server] WebSocket Upgrade error: %v", err)
			return
		}
		go handleConnection(conn, serverPassword)
	})

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleConnection(conn *websocket.Conn, serverPassword string) {
	defer conn.Close()
	authenticated := false

	log.Printf("[Server] New connection established from %s", conn.RemoteAddr())

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[Server] Connection closed or read error: %v", err)
			break
		}

		if messageType != websocket.TextMessage {
			continue
		}

		var req WSRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			log.Printf("[Server] Error unmarshaling request: %v", err)
			continue
		}

		// Enforce authentication
		if !authenticated && req.Type != "auth" {
			sendErrorResponse(conn, req.ID, "Unauthenticated")
			continue
		}

		var respPayload interface{}
		var respErr error

		switch req.Type {
		case "auth":
			var authReq AuthRequest
			if err := json.Unmarshal(req.Payload, &authReq); err != nil {
				respErr = err
			} else if authReq.Password != serverPassword {
				respErr = fmt.Errorf("Invalid password")
			} else {
				authenticated = true
				respPayload = "Authenticated successfully"
			}

		case "get_attr":
			var getReq GetAttrRequest
			if err := json.Unmarshal(req.Payload, &getReq); err != nil {
				respErr = err
			} else {
				meta, err := db.Get(getReq.Path)
				if err != nil {
					respErr = err
				} else {
					respPayload = meta
				}
			}

		case "set_attr":
			var setReq SetAttrRequest
			if err := json.Unmarshal(req.Payload, &setReq); err != nil {
				respErr = err
			} else {
				meta, err := db.Get(setReq.Path)
				if err != nil && err != fuse.ENOENT {
					respErr = err
				} else {
					if err == fuse.ENOENT {
						meta = NodeMeta{}
					}
					meta.Mode = setReq.Mode
					meta.Size = setReq.Size
					meta.Uid = setReq.Uid
					meta.Gid = setReq.Gid
					respErr = db.Put(setReq.Path, meta)
				}
			}

		case "read_dir":
			var dirReq ReadDirRequest
			if err := json.Unmarshal(req.Payload, &dirReq); err != nil {
				respErr = err
			} else {
				items, err := db.List()
				if err != nil {
					respErr = err
				} else {
					prefix := ""
					if dirReq.Path != "" {
						prefix = dirReq.Path + "/"
					}

					var entries []DirEntry
					seen := make(map[string]bool)

					for path, meta := range items {
						if prefix != "" && !strings.HasPrefix(path, prefix) {
							continue
						}
						if prefix == "" && strings.Contains(path, "/") {
							continue
						}

						rel := path
						if prefix != "" {
							rel = path[len(prefix):]
						}

						if isIgnored(rel) || strings.Contains(rel, "/") {
							continue
						}

						if seen[rel] {
							continue
						}
						seen[rel] = true

						entries = append(entries, DirEntry{
							Name: rel,
							Type: meta.Type,
						})
					}
					respPayload = ReadDirResponse{Entries: entries}
				}
			}

		case "create_node":
			var createReq CreateNodeRequest
			if err := json.Unmarshal(req.Payload, &createReq); err != nil {
				respErr = err
			} else {
				meta := NodeMeta{
					Type:   createReq.Type,
					Mode:   createReq.Mode,
					Target: createReq.Target,
					Size:   0,
					Uid:    createReq.Uid,
					Gid:    createReq.Gid,
				}
				respErr = db.Put(createReq.Path, meta)
			}

		case "remove_node":
			var removeReq RemoveNodeRequest
			if err := json.Unmarshal(req.Payload, &removeReq); err != nil {
				respErr = err
			} else {
				meta, err := db.Get(removeReq.Path)
				if err != nil {
					respErr = err
				} else {
					if meta.Type == "dir" {
						items, _ := db.List()
						prefix := removeReq.Path + "/"
						for path := range items {
							if strings.HasPrefix(path, prefix) {
								childMeta, errChild := db.Get(path)
								if errChild == nil {
									cleanUpNodeChunksServer(path, childMeta)
									_ = db.Delete(path)
								}
							}
						}
					} else {
						cleanUpNodeChunksServer(removeReq.Path, meta)
					}
					respErr = db.Delete(removeReq.Path)
					if respErr == nil {
						log.Printf("[Server] Successfully deleted: %s", removeReq.Path)
					}
				}
			}

		case "rename_node":
			var renameReq RenameNodeRequest
			if err := json.Unmarshal(req.Payload, &renameReq); err != nil {
				respErr = err
			} else {
				respErr = db.Rename(renameReq.OldPath, renameReq.NewPath)
			}

		case "read_file":
			var readReq ReadFileRequest
			if err := json.Unmarshal(req.Payload, &readReq); err != nil {
				sendResponse(conn, req.ID, nil, err)
			} else {
				// First check active un-flushed write buffer
				var data []byte
				serverActiveBuffers.Lock()
				buf, exists := serverActiveBuffers.buffers[readReq.Path]
				if exists {
					buf.mu.Lock()
					if int64(len(buf.data)) > readReq.Offset {
						end := readReq.Offset + int64(readReq.Size)
						if end > int64(len(buf.data)) {
							end = int64(len(buf.data))
						}
						data = make([]byte, end-readReq.Offset)
						copy(data, buf.data[readReq.Offset:end])
					}
					buf.mu.Unlock()
				}
				serverActiveBuffers.Unlock()

				if data == nil {
					// Fall back to reading committed chunks
					data, respErr = readRawBytes(readReq.Path, readReq.Offset, readReq.Size)
				}

				if respErr != nil {
					sendResponse(conn, req.ID, nil, respErr)
				} else {
					// Highly optimized binary transfer for FUSE file reads:
					// [32 bytes: Request ID, padded] [1 byte: Status (0 = success)] [Raw Bytes]
					buf := make([]byte, 32+1+len(data))
					copy(buf[0:32], []byte(fmt.Sprintf("%-32s", req.ID)))
					buf[32] = 0 // Success
					copy(buf[33:], data)
					
					_ = conn.WriteMessage(websocket.BinaryMessage, buf)
				}
			}
			continue

		case "write_file":
			var writeReq WriteFileRequest
			if err := json.Unmarshal(req.Payload, &writeReq); err != nil {
				respErr = err
			} else {
				decoded, err := base64.StdEncoding.DecodeString(writeReq.Data)
				if err != nil {
					respErr = err
				} else {
					buf, err := getServerWriteBuffer(writeReq.Path)
					if err != nil {
						respErr = err
					} else {
						buf.mu.Lock()
						buf.dirty = true
						end := writeReq.Offset + int64(len(decoded))
						if end > int64(len(buf.data)) {
							newData := make([]byte, end)
							copy(newData, buf.data)
							buf.data = newData
						}
						copy(buf.data[writeReq.Offset:end], decoded)
						buf.mu.Unlock()
					}
				}
			}

		case "flush_file":
			var flushReq FlushFileRequest
			if err := json.Unmarshal(req.Payload, &flushReq); err != nil {
				respErr = err
			} else {
				respErr = flushFile(flushReq.Path)
			}

		case "get_chunk_size":
			respPayload = GetChunkSizeResponse{ChunkSize: DefaultChunkSize}

		case "check_chunks":
			var checkReq CheckChunksRequest
			if err := json.Unmarshal(req.Payload, &checkReq); err != nil {
				respErr = err
			} else {
				var missing []string
				for _, h := range checkReq.Hashes {
					_, err := chunksDB.Get(h)
					if err != nil { // missing
						missing = append(missing, h)
					}
				}
				respPayload = CheckChunksResponse{Missing: missing}
			}

		case "upload_chunk":
			var uploadReq UploadChunkRequest
			if err := json.Unmarshal(req.Payload, &uploadReq); err != nil {
				respErr = err
			} else {
				decoded, err := base64.StdEncoding.DecodeString(uploadReq.Data)
				if err != nil {
					respErr = err
				} else {
					existingChunk, errChunk := chunksDB.Get(uploadReq.Hash)
					if errChunk != nil {
						existingChunk = ChunkMeta{
							Hash:        uploadReq.Hash,
							Size:        int64(len(decoded)),
							Replicas:    make(map[string]map[string]interface{}),
							LastChecked: make(map[string]time.Time),
							Status:      "replicating",
						}
						_ = chunksDB.Put(uploadReq.Hash, existingChunk)
					}
					evictCacheIfNecessary(int64(len(decoded)))
					cachePath := filepath.Join("./cache", uploadReq.Hash)
					_ = os.WriteFile(cachePath, decoded, 0644)

					requiredReplication := getReplicationFactor()
					if len(existingChunk.Replicas) < requiredReplication {
						globalReplicationQueue.Push(ReplicationTask{
							ChunkHash: uploadReq.Hash,
							ChunkSize: int64(len(decoded)),
							AddedAt:   time.Now(),
						})
					}
				}
			}

		case "commit_file":
			var commitReq CommitFileRequest
			if err := json.Unmarshal(req.Payload, &commitReq); err != nil {
				respErr = err
			} else {
				meta, err := db.Get(commitReq.Path)
				if err == nil {
					oldSources := meta.Sources
					meta.Size = commitReq.Size
					meta.Mode = commitReq.Mode
					meta.Uid = commitReq.Uid
					meta.Gid = commitReq.Gid
					meta.Sources = commitReq.Sources

					if errPut := db.Put(commitReq.Path, meta); errPut == nil {
						cleanUpOrphanedHashesServer(oldSources, commitReq.Sources, commitReq.Path)
					} else {
						respErr = errPut
					}
				} else if err == fuse.ENOENT {
					meta = NodeMeta{
						Type:    "file",
						Mode:    commitReq.Mode,
						Size:    commitReq.Size,
						Uid:     commitReq.Uid,
						Gid:     commitReq.Gid,
						Sources: commitReq.Sources,
					}
					respErr = db.Put(commitReq.Path, meta)
				} else {
					respErr = err
				}
			}

		default:
			respErr = fmt.Errorf("Unknown request type: %s", req.Type)
		}

		sendResponse(conn, req.ID, respPayload, respErr)
	}
}

func sendResponse(conn *websocket.Conn, requestID string, payload interface{}, err error) {
	var resp WSResponse
	resp.ID = requestID
	if err != nil {
		resp.Error = err.Error()
	} else if payload != nil {
		b, _ := json.Marshal(payload)
		resp.Payload = b
	}

	b, _ := json.Marshal(resp)
	_ = conn.WriteMessage(websocket.TextMessage, b)
}

func sendErrorResponse(conn *websocket.Conn, requestID string, errStr string) {
	resp := WSResponse{
		ID:    requestID,
		Error: errStr,
	}
	b, _ := json.Marshal(resp)
	_ = conn.WriteMessage(websocket.TextMessage, b)
}

func cleanUpNodeChunksServer(fullPath string, meta NodeMeta) {
	var wg sync.WaitGroup
	for _, chunkHash := range meta.Sources {
		if !isChunkReferenced(chunkHash, fullPath) {
			chunkMeta, errChunk := chunksDB.Get(chunkHash)
			if errChunk == nil {
				chunkMeta.Status = "deleting"
				_ = chunksDB.Put(chunkHash, chunkMeta)

				for provName, replMeta := range chunkMeta.Replicas {
					for _, p := range activeProviders {
						if p.Name() == provName {
							wg.Add(1)
							go func(prov Provider, m map[string]interface{}, h string) {
								defer wg.Done()
								if prov.Type() == "telegram" {
									if msgID, ok := m["msg_id"]; ok {
										containerID := fmt.Sprintf("%s:%v", prov.Name(), msgID)
										refCount, errRef := chunksDB.DecRef(containerID)
										if errRef == nil && refCount > 0 {
											return
										}
									}
								}
								_ = prov.Delete(context.Background(), m)
							}(p, replMeta, chunkHash)
						}
					}
				}
				_ = chunksDB.Delete(chunkHash)
			}
			cachePath := filepath.Join("./cache", chunkHash)
			_ = os.Remove(cachePath)
		}
	}
	wg.Wait()
}

func readRawBytes(path string, offset int64, size int) ([]byte, error) {
	meta, err := db.Get(path)
	if err != nil {
		return nil, err
	}
	if meta.Type != "file" {
		return nil, fmt.Errorf("not a file")
	}

	targetSize := int64(size)
	if size < 0 {
		targetSize = int64(meta.Size) - offset
	}
	if targetSize <= 0 {
		return []byte{}, nil
	}

	type chunkOverlap struct {
		index     int
		hash      string
		srcStart  int64
		srcEnd    int64
		dstOffset int64
	}
	var overlaps []chunkOverlap

	var currentOffset int64 = 0
	for idx, hash := range meta.Sources {
		chunkMeta, err := chunksDB.Get(hash)
		if err != nil {
			return nil, err
		}
		chunkSize := chunkMeta.Size
		chunkStart := currentOffset
		chunkEnd := currentOffset + chunkSize

		readStart := offset
		if readStart < chunkStart {
			readStart = chunkStart
		}
		readEnd := offset + targetSize
		if readEnd > chunkEnd {
			readEnd = chunkEnd
		}

		if readStart < readEnd {
			overlaps = append(overlaps, chunkOverlap{
				index:     idx,
				hash:      hash,
				srcStart:  readStart - chunkStart,
				srcEnd:    readEnd - chunkStart,
				dstOffset: readStart - offset,
			})
		}
		currentOffset += chunkSize
	}

	if len(overlaps) == 0 {
		return []byte{}, nil
	}

	readBuffer := make([]byte, targetSize)
	for _, ov := range overlaps {
		var chunkData []byte
		cachePath := filepath.Join("./cache", ov.hash)

		// Attempt to read ONLY the requested slice directly from the cached file on disk
		if _, errStat := os.Stat(cachePath); errStat == nil {
			file, errOpen := os.Open(cachePath)
			if errOpen == nil {
				length := ov.srcEnd - ov.srcStart
				sliceBuf := make([]byte, length)
				n, errRead := file.ReadAt(sliceBuf, ov.srcStart)
				file.Close()
				if errRead == nil || (errRead == io.EOF && int64(n) == length) {
					copy(readBuffer[ov.dstOffset:ov.dstOffset+int64(n)], sliceBuf[:n])
					continue // Successfully loaded this slice directly, skip downloading/whole-file parsing
				}
			}
		}

		// Cache miss or read failure: Download the entire chunk from the provider
		chunkMeta, errChunk := chunksDB.Get(ov.hash)
		if errChunk == nil {
			for provName, replicaMeta := range chunkMeta.Replicas {
				var provider Provider
				for _, p := range activeProviders {
					if p.Name() == provName {
						provider = p
						break
					}
				}
				if provider != nil {
					downloaded, errDownload := provider.Download(context.Background(), replicaMeta)
					if errDownload == nil && len(downloaded) > 0 {
						chunkData = downloaded
						evictCacheIfNecessary(int64(len(chunkData)))
						_ = os.WriteFile(cachePath, chunkData, 0644)
						break
					}
				}
			}
		}

		if chunkData == nil {
			return nil, fmt.Errorf("failed to read chunk %s", ov.hash)
		}

		// Robust defense-in-depth bounds checking to prevent out-of-bounds panics on truncated chunks
		srcStart := ov.srcStart
		srcEnd := ov.srcEnd
		if srcStart < 0 {
			srcStart = 0
		}
		if srcStart > int64(len(chunkData)) {
			srcStart = int64(len(chunkData))
		}
		if srcEnd < srcStart {
			srcEnd = srcStart
		}
		if srcEnd > int64(len(chunkData)) {
			srcEnd = int64(len(chunkData))
		}

		copyLen := srcEnd - srcStart
		dstStart := ov.dstOffset
		dstEnd := dstStart + copyLen
		if dstEnd > int64(len(readBuffer)) {
			dstEnd = int64(len(readBuffer))
			copyLen = dstEnd - dstStart
			if copyLen < 0 {
				copyLen = 0
			}
			srcEnd = srcStart + copyLen
		}

		copy(readBuffer[dstStart:dstEnd], chunkData[srcStart:srcEnd])
	}

	return readBuffer, nil
}

func flushFile(path string) error {
	serverActiveBuffers.Lock()
	buf, exists := serverActiveBuffers.buffers[path]
	if !exists || !buf.dirty {
		serverActiveBuffers.Unlock()
		return nil
	}
	buf.mu.Lock()
	totalBytes := len(buf.data)
	fileData := make([]byte, totalBytes)
	copy(fileData, buf.data)
	buf.dirty = false
	buf.mu.Unlock()
	serverActiveBuffers.Unlock()

	type chunkPrep struct {
		hash string
		data []byte
	}
	var preppedChunks []chunkPrep

	minLimit := getMinProviderLimit()
	var currentOffset int64 = 0
	for currentOffset < int64(totalBytes) {
		chunkSize := minLimit
		if currentOffset+chunkSize > int64(totalBytes) {
			chunkSize = int64(totalBytes) - currentOffset
		}

		chunkData := make([]byte, chunkSize)
		copy(chunkData, fileData[currentOffset:currentOffset+chunkSize])

		hasher := sha256.New()
		hasher.Write(chunkData)
		contentHash := hex.EncodeToString(hasher.Sum(nil))

		preppedChunks = append(preppedChunks, chunkPrep{
			hash: contentHash,
			data: chunkData,
		})
		currentOffset += chunkSize
	}

	requiredReplication := getReplicationFactor()
	var chunkHashes []string

	for _, pc := range preppedChunks {
		chunkHashes = append(chunkHashes, pc.hash)

		existingChunk, errChunk := chunksDB.Get(pc.hash)
		if errChunk != nil {
			existingChunk = ChunkMeta{
				Hash:        pc.hash,
				Size:        int64(len(pc.data)),
				Replicas:    make(map[string]map[string]interface{}),
				LastChecked: make(map[string]time.Time),
				Status:      "replicating",
			}
			_ = chunksDB.Put(pc.hash, existingChunk)
		} else {
			if len(existingChunk.Replicas) >= requiredReplication && existingChunk.Status != "ready" {
				existingChunk.Status = "ready"
				_ = chunksDB.Put(pc.hash, existingChunk)
			}
		}

		evictCacheIfNecessary(int64(len(pc.data)))
		cachePath := filepath.Join("./cache", pc.hash)
		_ = os.WriteFile(cachePath, pc.data, 0644)

		if len(existingChunk.Replicas) < requiredReplication {
			globalReplicationQueue.Push(ReplicationTask{
				ChunkHash: pc.hash,
				ChunkSize: int64(len(pc.data)),
				AddedAt:   time.Now(),
			})
		}
	}

	meta, err := db.Get(path)
	if err == nil {
		meta.Size = uint64(totalBytes)
		oldSources := meta.Sources

		sources := make([]string, len(preppedChunks))
		for i, pc := range preppedChunks {
			sources[i] = pc.hash
		}
		meta.Sources = sources

		if err := db.Put(path, meta); err == nil {
			cleanUpOrphanedHashesServer(oldSources, sources, path)
		}
	}

	log.Printf("[Server Flush] %s is now recorded in database. Background replication started for %d chunks.", path, len(chunkHashes))
	return nil
}

func cleanUpOrphanedHashesServer(oldSources, newSources []string, currentPath string) {
	newSet := make(map[string]bool)
	for _, s := range newSources {
		newSet[s] = true
	}

	var wg sync.WaitGroup
	for _, oldHash := range oldSources {
		if newSet[oldHash] {
			continue
		}

		if !isChunkReferenced(oldHash, currentPath) {
			chunkMeta, errChunk := chunksDB.Get(oldHash)
			if errChunk == nil {
				chunkMeta.Status = "deleting"
				_ = chunksDB.Put(oldHash, chunkMeta)

				for provName, replMeta := range chunkMeta.Replicas {
					for _, p := range activeProviders {
						if p.Name() == provName {
							wg.Add(1)
							go func(provider Provider, m map[string]interface{}, hash string) {
								defer wg.Done()
								log.Printf("[Server Flush] Deleting orphan chunk %s from provider %s (%s)...", hash, provider.Name(), provider.Type())
								if provider.Type() == "telegram" {
									if msgID, ok := m["msg_id"]; ok {
										containerID := fmt.Sprintf("%s:%v", provider.Name(), msgID)
										refCount, errRef := chunksDB.DecRef(containerID)
										if errRef == nil && refCount > 0 {
											return
										}
									}
								}
								_ = provider.Delete(context.Background(), m)
							}(p, replMeta, oldHash)
						}
					}
				}
				_ = chunksDB.Delete(oldHash)
			}
			cachePath := filepath.Join("./cache", oldHash)
			_ = os.Remove(cachePath)
		}
	}
	wg.Wait()
}
