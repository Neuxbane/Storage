package main

import (
	"encoding/json"
	"strings"
)

// NodeMeta stores FUSE attributes and chunk map pointers for a file/symlink
type NodeMeta struct {
	Type    string   `json:"type"` // "file", "symlink", "dir"
	Mode    uint32   `json:"mode"`
	Size    uint64   `json:"size"`
	Uid     uint32   `json:"uid"`
	Gid     uint32   `json:"gid"`
	Target  string   `json:"target,omitempty"`  // Used if Type == "symlink"
	Sources []string `json:"sources,omitempty"` // List of chunk hashes
}

func isIgnored(name string) bool {
	ignoredPatterns := []string{
		".Trash-",
		".DS_Store",
		".xdg-volume-info",
		"autorun.inf",
		"desktop.ini",
	}
	for _, p := range ignoredPatterns {
		if strings.HasPrefix(name, p) || strings.Contains(name, "/"+p) {
			return true
		}
	}
	return false
}

// =========================================================================
// WEBSOCKET COMMUNICATION PROTOCOL MESSAGES
// =========================================================================

type WSRequest struct {
	Type    string          `json:"type"` // "auth", "get_attr", "set_attr", "read_dir", "create_node", "remove_node", "rename_node", "read_file", "write_file", "flush_file"
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type WSResponse struct {
	ID            string          `json:"id"`
	Error         string          `json:"error,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	BinaryPayload []byte          `json:"-"`
}

type AuthRequest struct {
	Password string `json:"password"`
}

type GetAttrRequest struct {
	Path string `json:"path"`
}

type SetAttrRequest struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode"`
	Size uint64 `json:"size"`
	Uid  uint32 `json:"uid"`
	Gid  uint32 `json:"gid"`
}

type ReadDirRequest struct {
	Path string `json:"path"`
}

type ReadDirResponse struct {
	Entries []DirEntry `json:"entries"`
}

type DirEntry struct {
	Name string `json:"name"`
	Type string `json:"type"` // "file", "dir", "symlink"
}

type CreateNodeRequest struct {
	Path   string `json:"path"`
	Mode   uint32 `json:"mode"`
	Type   string `json:"type"` // "file", "dir", "symlink"
	Target string `json:"target,omitempty"` // For symlink
	Uid    uint32 `json:"uid"`
	Gid    uint32 `json:"gid"`
}

type RemoveNodeRequest struct {
	Path string `json:"path"`
}

type RenameNodeRequest struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

type ReadFileRequest struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Size   int    `json:"size"`
}

type ReadFileResponse struct {
	Data string `json:"data"` // base64 encoded
}

type WriteFileRequest struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Data   string `json:"data"` // base64 encoded
}

type FlushFileRequest struct {
	Path string `json:"path"`
}

type GetChunkSizeResponse struct {
	ChunkSize int64 `json:"chunk_size"`
}

type CheckChunksRequest struct {
	Hashes []string `json:"hashes"`
}

type CheckChunksResponse struct {
	Missing []string `json:"missing"`
}

type UploadChunkRequest struct {
	Hash string `json:"hash"`
	Data string `json:"data"` // base64 encoded
}

type CommitFileRequest struct {
	Path    string   `json:"path"`
	Size    uint64   `json:"size"`
	Mode    uint32   `json:"mode"`
	Uid     uint32   `json:"uid"`
	Gid     uint32   `json:"gid"`
	Sources []string `json:"sources"`
}
