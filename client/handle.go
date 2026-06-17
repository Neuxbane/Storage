package main

import (
	"context"
	"encoding/base64"
	"log"

	"bazil.org/fuse"
)

// =========================================================================
// CHUNK-LESS CLIENT READ/WRITE HANDLES (fully routed via WebSocket)
// =========================================================================

// FileHandle implements both reading and writing interfaces for FUSE files.
type FileHandle struct {
	Name string
}

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if len(req.Data) == 0 {
		return nil
	}

	encoded := base64.StdEncoding.EncodeToString(req.Data)
	_, err := wsClient.Request("write_file", WriteFileRequest{
		Path:   h.Name,
		Offset: req.Offset,
		Data:   encoded,
	})
	if err != nil {
		return err
	}

	resp.Size = len(req.Data)
	return nil
}

func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.Printf("[FUSE Flush] Requesting server-side flush/chunking for %s", h.Name)
	_, err := wsClient.Request("flush_file", FlushFileRequest{Path: h.Name})
	return err
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
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
