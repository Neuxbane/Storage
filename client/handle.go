package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"sync"

	"bazil.org/fuse"
)

// =========================================================================
// CLIENT-SIDE BUFFERING & DEDUPLICATED FILE HANDLES
// =========================================================================

type FileHandle struct {
	Name  string
	mu    sync.Mutex
	data  []byte
	dirty bool
}

func (h *FileHandle) initBuffer() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.data != nil {
		return nil
	}

	// Fetch existing file attributes to check size
	payload, err := wsClient.Request("get_attr", GetAttrRequest{Path: h.Name})
	if err != nil {
		if err == fuse.ENOENT {
			h.data = []byte{}
			return nil
		}
		return err
	}
	var meta NodeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return err
	}

	if meta.Size == 0 {
		h.data = []byte{}
		return nil
	}

	// Read existing file data
	data, err := wsClient.RequestBinary("read_file", ReadFileRequest{
		Path:   h.Name,
		Offset: 0,
		Size:   int(meta.Size),
	})
	if err != nil {
		return err
	}
	h.data = data
	return nil
}

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if len(req.Data) == 0 {
		return nil
	}

	if err := h.initBuffer(); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.dirty = true
	end := req.Offset + int64(len(req.Data))
	if end > int64(len(h.data)) {
		newData := make([]byte, end)
		copy(newData, h.data)
		h.data = newData
	}
	copy(h.data[req.Offset:end], req.Data)

	resp.Size = len(req.Data)
	return nil
}

func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	h.mu.Lock()
	dirty := h.dirty
	fileData := h.data
	h.mu.Unlock()

	if !dirty {
		return nil
	}

	log.Printf("[FUSE Flush] Starting client-side hashing and upload for %s...", h.Name)

	chunkSize := wsClient.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 * 1024 * 1024
	}

	totalBytes := len(fileData)
	var chunkHashes []string
	var chunksToUpload []struct {
		hash string
		data []byte
	}

	var currentOffset int64 = 0
	for currentOffset < int64(totalBytes) {
		sz := chunkSize
		if currentOffset+sz > int64(totalBytes) {
			sz = int64(totalBytes) - currentOffset
		}

		cData := fileData[currentOffset : currentOffset+sz]
		hasher := sha256.New()
		hasher.Write(cData)
		hVal := hex.EncodeToString(hasher.Sum(nil))

		chunkHashes = append(chunkHashes, hVal)
		chunksToUpload = append(chunksToUpload, struct {
			hash string
			data []byte
		}{hash: hVal, data: cData})

		currentOffset += sz
	}

	// Ask server which hashes are missing
	var missingHashes []string
	if len(chunkHashes) > 0 {
		payload, err := wsClient.Request("check_chunks", CheckChunksRequest{Hashes: chunkHashes})
		if err != nil {
			return err
		}
		var checkResp CheckChunksResponse
		if errJSON := json.Unmarshal(payload, &checkResp); errJSON != nil {
			return errJSON
		}
		missingHashes = checkResp.Missing
	}

	log.Printf("[FUSE Flush] File %s: out of %d total chunks, %d are missing on server.", h.Name, len(chunkHashes), len(missingHashes))

	missingMap := make(map[string]bool)
	for _, m := range missingHashes {
		missingMap[m] = true
	}

	// Upload missing chunks
	for _, c := range chunksToUpload {
		if missingMap[c.hash] {
			log.Printf("[FUSE Flush] Uploading missing chunk %s (%d bytes)...", c.hash[:8], len(c.data))
			encoded := base64.StdEncoding.EncodeToString(c.data)
			_, err := wsClient.Request("upload_chunk", UploadChunkRequest{
				Hash: c.hash,
				Data: encoded,
			})
			if err != nil {
				return err
			}
		}
	}

	// Fetch file meta from server to get UID/GID/Mode if we have them
	var uid, gid uint32
	var mode uint32 = 0644
	attrPayload, errAttr := wsClient.Request("get_attr", GetAttrRequest{Path: h.Name})
	if errAttr == nil {
		var meta NodeMeta
		if json.Unmarshal(attrPayload, &meta) == nil {
			uid = meta.Uid
			gid = meta.Gid
			mode = meta.Mode
		}
	}

	// Commit file references
	log.Printf("[FUSE Flush] Committing file reference for %s with %d chunks...", h.Name, len(chunkHashes))
	_, err := wsClient.Request("commit_file", CommitFileRequest{
		Path:    h.Name,
		Size:    uint64(totalBytes),
		Mode:    mode,
		Uid:     uid,
		Gid:     gid,
		Sources: chunkHashes,
	})
	if err != nil {
		return err
	}

	h.mu.Lock()
	h.dirty = false
	h.mu.Unlock()

	log.Printf("[FUSE Flush] Successfully committed %s.", h.Name)
	return nil
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	h.mu.Lock()
	isDirty := h.dirty
	var data []byte
	if h.data != nil {
		if req.Offset < int64(len(h.data)) {
			end := req.Offset + int64(req.Size)
			if end > int64(len(h.data)) {
				end = int64(len(h.data))
			}
			data = make([]byte, end-req.Offset)
			copy(data, h.data[req.Offset:end])
		}
	}
	h.mu.Unlock()

	if data != nil || isDirty {
		resp.Data = data
		return nil
	}

	binaryPayload, err := wsClient.RequestBinary("read_file", ReadFileRequest{
		Path:   h.Name,
		Offset: req.Offset,
		Size:   req.Size,
	})
	if err != nil {
		return err
	}

	resp.Data = binaryPayload
	return nil
}
