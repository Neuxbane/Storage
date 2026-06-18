package main

import (
	"sync"
)

type QueuedOp struct {
	Type       string      // e.g. "create_node", "remove_node", "rename_node", "write_file", "flush_file", "set_attr"
	Payload    interface{} // The raw payload struct
	ResultChan chan error  // FUSE thread blocks on this channel waiting for result
}

type OperationQueue struct {
	mu  sync.Mutex
	ops []*QueuedOp
}

func NewOperationQueue() *OperationQueue {
	return &OperationQueue{
		ops: make([]*QueuedOp, 0),
	}
}

func (q *OperationQueue) Add(reqType string, payload interface{}) chan error {
	q.mu.Lock()
	defer q.mu.Unlock()

	resChan := make(chan error, 1)
	op := &QueuedOp{
		Type:       reqType,
		Payload:    payload,
		ResultChan: resChan,
	}

	// Try to collapse the new operation with existing ones in the queue.
	if q.tryCollapse(op) {
		return resChan
	}

	q.ops = append(q.ops, op)
	return resChan
}

func (q *OperationQueue) GetOps() []*QueuedOp {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	// Return a copy of the slice to avoid race conditions.
	copied := make([]*QueuedOp, len(q.ops))
	copy(copied, q.ops)
	return copied
}

func (q *OperationQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ops = make([]*QueuedOp, 0)
}

func (q *OperationQueue) tryCollapse(newOp *QueuedOp) bool {
	switch newOp.Type {
	case "remove_node":
		req := newOp.Payload.(RemoveNodeRequest)
		path := req.Path

		// Check if there is a create_node for this path in the queue.
		hasCreate := false
		for _, op := range q.ops {
			if op.Type == "create_node" {
				cReq := op.Payload.(CreateNodeRequest)
				if cReq.Path == path {
					hasCreate = true
					break
				}
			}
		}

		if hasCreate {
			// Collapse completely:
			// 1. Remove all operations on path (or under path if it's a directory) from the queue.
			// 2. Unblock all their threads with nil.
			// 3. Unblock the new remove_node thread with nil.
			// 4. Do not add the new remove_node thread.
			var newOps []*QueuedOp
			for _, op := range q.ops {
				if isOpTargetedByRemove(op, path) {
					op.ResultChan <- nil // unblock with success
				} else {
					newOps = append(newOps, op)
				}
			}
			q.ops = newOps
			newOp.ResultChan <- nil
			return true
		} else {
			// No create_node in the queue (the file existed on the server).
			// However, we should still cancel any intermediate writes, flushes, or setattr on this path
			// because the file is being deleted anyway!
			var newOps []*QueuedOp
			for _, op := range q.ops {
				if isOpTargetedByRemove(op, path) {
					op.ResultChan <- nil // unblock with success (since it's deleted)
				} else {
					newOps = append(newOps, op)
				}
			}
			q.ops = newOps
			// We still need to send the remove_node to the server, so return false (do not discard).
			return false
		}

	case "rename_node":
		req := newOp.Payload.(RenameNodeRequest)
		oldPath := req.OldPath
		newPath := req.NewPath

		// Check if create_node for oldPath is in the queue.
		hasCreate := false
		for _, op := range q.ops {
			if op.Type == "create_node" {
				cReq := op.Payload.(CreateNodeRequest)
				if cReq.Path == oldPath {
					hasCreate = true
					break
				}
			}
		}

		if hasCreate {
			// Since oldPath was created locally, we can just rewrite oldPath to newPath for all queued ops.
			for _, op := range q.ops {
				rewriteOpPath(op, oldPath, newPath)
			}
			// Unblock rename with success immediately and don't queue it.
			newOp.ResultChan <- nil
			return true
		}
	}
	return false
}

func getOpPath(op *QueuedOp) string {
	switch op.Type {
	case "create_node":
		return op.Payload.(CreateNodeRequest).Path
	case "remove_node":
		return op.Payload.(RemoveNodeRequest).Path
	case "rename_node":
		return op.Payload.(RenameNodeRequest).OldPath
	case "write_file":
		return op.Payload.(WriteFileRequest).Path
	case "flush_file":
		return op.Payload.(FlushFileRequest).Path
	case "set_attr":
		return op.Payload.(SetAttrRequest).Path
	}
	return ""
}

func isSameOrChild(p, parent string) bool {
	if p == parent {
		return true
	}
	return len(p) > len(parent) && p[len(parent)] == '/' && p[:len(parent)] == parent
}

func isOpTargetedByRemove(op *QueuedOp, removePath string) bool {
	if op.Type == "rename_node" {
		req := op.Payload.(RenameNodeRequest)
		return isSameOrChild(req.OldPath, removePath) || isSameOrChild(req.NewPath, removePath)
	}
	return isSameOrChild(getOpPath(op), removePath)
}

func rewriteOpPath(op *QueuedOp, oldPath, newPath string) {
	switch op.Type {
	case "create_node":
		req := op.Payload.(CreateNodeRequest)
		if isSameOrChild(req.Path, oldPath) {
			req.Path = replacePrefix(req.Path, oldPath, newPath)
			op.Payload = req
		}
	case "remove_node":
		req := op.Payload.(RemoveNodeRequest)
		if isSameOrChild(req.Path, oldPath) {
			req.Path = replacePrefix(req.Path, oldPath, newPath)
			op.Payload = req
		}
	case "rename_node":
		req := op.Payload.(RenameNodeRequest)
		changed := false
		if isSameOrChild(req.OldPath, oldPath) {
			req.OldPath = replacePrefix(req.OldPath, oldPath, newPath)
			changed = true
		}
		if isSameOrChild(req.NewPath, oldPath) {
			req.NewPath = replacePrefix(req.NewPath, oldPath, newPath)
			changed = true
		}
		if changed {
			op.Payload = req
		}
	case "write_file":
		req := op.Payload.(WriteFileRequest)
		if isSameOrChild(req.Path, oldPath) {
			req.Path = replacePrefix(req.Path, oldPath, newPath)
			op.Payload = req
		}
	case "flush_file":
		req := op.Payload.(FlushFileRequest)
		if isSameOrChild(req.Path, oldPath) {
			req.Path = replacePrefix(req.Path, oldPath, newPath)
			op.Payload = req
		}
	case "set_attr":
		req := op.Payload.(SetAttrRequest)
		if isSameOrChild(req.Path, oldPath) {
			req.Path = replacePrefix(req.Path, oldPath, newPath)
			op.Payload = req
		}
	}
}

func replacePrefix(path, oldPrefix, newPrefix string) string {
	if path == oldPrefix {
		return newPrefix
	}
	return newPrefix + path[len(oldPrefix):]
}

func base64DecodedLen(s string) int {
	l := len(s)
	if l == 0 {
		return 0
	}
	padding := 0
	if s[l-1] == '=' {
		padding++
	}
	if l > 1 && s[l-2] == '=' {
		padding++
	}
	return (l * 3 / 4) - padding
}

func (q *OperationQueue) FindQueuedMeta(path string) (*NodeMeta, bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// 1. Check if any parent directory of path is deleted in the queue.
	// If so, the path is deleted.
	for _, op := range q.ops {
		if op.Type == "remove_node" {
			req := op.Payload.(RemoveNodeRequest)
			if path != req.Path && isSameOrChild(path, req.Path) {
				return nil, false, true
			}
		}
	}

	// 2. Track metadata of the target path by scanning the queue in chronological order.
	var meta *NodeMeta
	found := false
	exists := false

	for _, op := range q.ops {
		switch op.Type {
		case "create_node":
			req := op.Payload.(CreateNodeRequest)
			if req.Path == path {
				meta = &NodeMeta{
					Type:   req.Type,
					Mode:   req.Mode,
					Uid:    req.Uid,
					Gid:    req.Gid,
					Target: req.Target,
				}
				found = true
				exists = true
			}
		case "remove_node":
			req := op.Payload.(RemoveNodeRequest)
			if req.Path == path {
				meta = nil
				found = true
				exists = false
			}
		case "write_file":
			req := op.Payload.(WriteFileRequest)
			if req.Path == path && exists && meta != nil {
				writeLen := uint64(base64DecodedLen(req.Data))
				newSize := uint64(req.Offset) + writeLen
				if newSize > meta.Size {
					meta.Size = newSize
				}
			}
		case "set_attr":
			req := op.Payload.(SetAttrRequest)
			if req.Path == path && exists && meta != nil {
				if req.Mode != 0 {
					meta.Mode = req.Mode
				}
				if req.Uid != 0 {
					meta.Uid = req.Uid
				}
				if req.Gid != 0 {
					meta.Gid = req.Gid
				}
				if req.Size != 0 {
					meta.Size = req.Size
				}
			}
		}
	}

	return meta, exists, found
}

func (q *OperationQueue) FindQueuedDirEntries(parent string) ([]DirEntry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if parent directory itself is in the queue to be created.
	parentExists := false
	if parent == "" {
		parentExists = true // root directory
	} else {
		for _, op := range q.ops {
			if op.Type == "create_node" {
				req := op.Payload.(CreateNodeRequest)
				if req.Path == parent && req.Type == "dir" {
					parentExists = true
				}
			}
			if op.Type == "remove_node" {
				req := op.Payload.(RemoveNodeRequest)
				if req.Path == parent {
					parentExists = false
				}
			}
		}
	}

	if !parentExists {
		return nil, false
	}

	// Scan queue for entries in this directory
	entriesMap := make(map[string]string) // name -> type
	for _, op := range q.ops {
		switch op.Type {
		case "create_node":
			req := op.Payload.(CreateNodeRequest)
			dir, name := splitPath(req.Path)
			if dir == parent {
				entriesMap[name] = req.Type
			}
		case "remove_node":
			req := op.Payload.(RemoveNodeRequest)
			dir, name := splitPath(req.Path)
			if dir == parent {
				delete(entriesMap, name)
			}
		}
	}

	var entries []DirEntry
	for name, t := range entriesMap {
		entries = append(entries, DirEntry{
			Name: name,
			Type: t,
		})
	}
	return entries, true
}

func splitPath(path string) (string, string) {
	idx := lastSlash(path)
	if idx == -1 {
		return "", path
	}
	return path[:idx], path[idx+1:]
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}
