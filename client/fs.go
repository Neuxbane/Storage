package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// =========================================================================
// CHUNK-LESS CLIENT FUSE CORE BINDINGS & NODES (routed via WebSocket)
// =========================================================================

type FS struct{}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{Path: ""}, nil
}

func (f *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	resp.Bsize = 4096
	resp.Frsize = 4096

	// Client-side hardcoded capacity representation of 4 EiB
	const capacityBytes = uint64(4) << 60
	resp.Blocks = capacityBytes / uint64(resp.Bsize)
	resp.Bfree = resp.Blocks
	resp.Bavail = resp.Blocks
	resp.Files = 1 << 32
	resp.Ffree = resp.Files
	return nil
}

// --- Directory Node ---
type Dir struct {
	Path string
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0755
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())

	if d.Path != "" {
		payload, err := wsClient.Request("get_attr", GetAttrRequest{Path: d.Path})
		if err == nil {
			var meta NodeMeta
			if errJSON := json.Unmarshal(payload, &meta); errJSON == nil {
				a.Mode = os.ModeDir | os.FileMode(meta.Mode)
				if meta.Uid != 0 {
					a.Uid = meta.Uid
				}
				if meta.Gid != 0 {
					a.Gid = meta.Gid
				}
			}
		}
	}
	return nil
}

func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	log.Printf("[FUSE Setattr] Dir.Setattr path='%s' Valid=%+v Uid=%d Gid=%d Mode=%v", d.Path, req.Valid, req.Uid, req.Gid, req.Mode)
	payload, err := wsClient.Request("get_attr", GetAttrRequest{Path: d.Path})
	if err != nil {
		log.Printf("[FUSE Setattr] Dir.Setattr path='%s' failed to get current attributes: %v", d.Path, err)
		return err
	}
	var meta NodeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		log.Printf("[FUSE Setattr] Dir.Setattr path='%s' failed to unmarshal attributes: %v", d.Path, err)
		return err
	}

	modified := false
	if req.Valid.Mode() {
		meta.Mode = uint32(req.Mode)
		modified = true
	}
	if req.Valid.Uid() {
		meta.Uid = req.Uid
		modified = true
	}
	if req.Valid.Gid() {
		meta.Gid = req.Gid
		modified = true
	}

	if modified {
		_, err = wsClient.Request("set_attr", SetAttrRequest{
			Path: d.Path,
			Mode: meta.Mode,
			Size: meta.Size,
			Uid:  meta.Uid,
			Gid:  meta.Gid,
		})
		if err != nil {
			log.Printf("[FUSE Setattr] Dir.Setattr path='%s' failed on server: %v", d.Path, err)
			return err
		}
	}
	
	err = d.Attr(ctx, &resp.Attr)
	if err != nil {
		log.Printf("[FUSE Setattr] Dir.Setattr path='%s' failed to populate response attributes: %v", d.Path, err)
	}
	return err
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if isIgnored(name) {
		return nil, fuse.ENOENT
	}
	fullPath := name
	if d.Path != "" {
		fullPath = d.Path + "/" + name
	}
	
	payload, err := wsClient.Request("get_attr", GetAttrRequest{Path: fullPath})
	if err != nil {
		return nil, err
	}
	var meta NodeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return nil, fuse.ENOENT
	}

	if meta.Type == "symlink" {
		return &Symlink{Name: fullPath, Meta: meta}, nil
	}
	if meta.Type == "dir" {
		return &Dir{Path: fullPath}, nil
	}
	return &File{Name: fullPath, Meta: meta}, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	payload, err := wsClient.Request("read_dir", ReadDirRequest{Path: d.Path})
	if err != nil {
		return nil, err
	}
	var resp ReadDirResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, err
	}

	var entries []fuse.Dirent
	for _, entry := range resp.Entries {
		t := fuse.DT_File
		if entry.Type == "symlink" {
			t = fuse.DT_Link
		} else if entry.Type == "dir" {
			t = fuse.DT_Dir
		}
		entries = append(entries, fuse.Dirent{Name: entry.Name, Type: t})
	}
	return entries, nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if isIgnored(req.Name) {
		return nil, nil, fuse.EPERM
	}
	fullPath := req.Name
	if d.Path != "" {
		fullPath = d.Path + "/" + req.Name
	}
	log.Printf("[FUSE Create] Creating file: %s with mode: %v", fullPath, req.Mode)
	
	_, err := wsClient.Request("create_node", CreateNodeRequest{
		Path: fullPath,
		Mode: uint32(req.Mode),
		Type: "file",
		Uid:  req.Uid,
		Gid:  req.Gid,
	})
	if err != nil {
		return nil, nil, fuse.EIO
	}

	meta := NodeMeta{
		Type: "file",
		Mode: uint32(req.Mode),
		Size: 0,
		Uid:  req.Uid,
		Gid:  req.Gid,
	}
	
	handle := &FileHandle{
		Name: fullPath,
	}

	resp.Attr.Mode = req.Mode
	resp.Attr.Size = 0
	resp.Attr.Uid = req.Uid
	resp.Attr.Gid = req.Gid

	return &File{Name: fullPath, Meta: meta}, handle, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fullPath := req.Name
	if d.Path != "" {
		fullPath = d.Path + "/" + req.Name
	}
	log.Printf("[FUSE Remove] Deleting entry: %s", fullPath)

	_, err := wsClient.Request("remove_node", RemoveNodeRequest{Path: fullPath})
	return err
}

func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	fullPath := req.NewName
	if d.Path != "" {
		fullPath = d.Path + "/" + req.NewName
	}
	log.Printf("[FUSE Symlink] Creating symlink from %s to target %s", fullPath, req.Target)

	_, err := wsClient.Request("create_node", CreateNodeRequest{
		Path:   fullPath,
		Mode:   uint32(os.ModeSymlink | 0777),
		Type:   "symlink",
		Target: req.Target,
		Uid:    req.Uid,
		Gid:    req.Gid,
	})
	if err != nil {
		return nil, fuse.EIO
	}

	meta := NodeMeta{
		Type:   "symlink",
		Mode:   uint32(os.ModeSymlink | 0777),
		Target: req.Target,
		Uid:    req.Uid,
		Gid:    req.Gid,
	}
	return &Symlink{Name: fullPath, Meta: meta}, nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*Dir)
	if !ok {
		return fuse.ENOTSUP
	}

	oldPath := req.OldName
	if d.Path != "" {
		oldPath = d.Path + "/" + req.OldName
	}

	newPath := req.NewName
	if dstDir.Path != "" {
		newPath = dstDir.Path + "/" + req.NewName
	}

	log.Printf("[FUSE Rename] Renaming %s to %s", oldPath, newPath)

	_, err := wsClient.Request("rename_node", RenameNodeRequest{
		OldPath: oldPath,
		NewPath: newPath,
	})
	return err
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if isIgnored(req.Name) {
		return nil, fuse.EPERM
	}
	fullPath := req.Name
	if d.Path != "" {
		fullPath = d.Path + "/" + req.Name
	}
	log.Printf("[FUSE Mkdir] Creating directory: %s", fullPath)

	_, err := wsClient.Request("create_node", CreateNodeRequest{
		Path: fullPath,
		Mode: uint32(req.Mode),
		Type: "dir",
		Uid:  req.Uid,
		Gid:  req.Gid,
	})
	if err != nil {
		return nil, fuse.EIO
	}
	return &Dir{Path: fullPath}, nil
}

// --- File Node ---
type File struct {
	Name string
	Meta NodeMeta
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	payload, err := wsClient.Request("get_attr", GetAttrRequest{Path: f.Name})
	if err == nil {
		var meta NodeMeta
		if errJSON := json.Unmarshal(payload, &meta); errJSON == nil {
			f.Meta = meta
		}
	}
	a.Mode = os.FileMode(f.Meta.Mode)
	a.Size = f.Meta.Size
	a.Blocks = (f.Meta.Size + 511) / 512
	if f.Meta.Uid != 0 {
		a.Uid = f.Meta.Uid
	} else {
		a.Uid = uint32(os.Getuid())
	}
	if f.Meta.Gid != 0 {
		a.Gid = f.Meta.Gid
	} else {
		a.Gid = uint32(os.Getgid())
	}
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	log.Printf("[FUSE Setattr] File.Setattr path='%s' Valid=%+v Uid=%d Gid=%d Mode=%v", f.Name, req.Valid, req.Uid, req.Gid, req.Mode)
	modified := false
	if req.Valid.Mode() {
		f.Meta.Mode = uint32(req.Mode)
		modified = true
	}
	if req.Valid.Size() {
		f.Meta.Size = req.Size
		modified = true
	}
	if req.Valid.Uid() {
		f.Meta.Uid = req.Uid
		modified = true
	}
	if req.Valid.Gid() {
		f.Meta.Gid = req.Gid
		modified = true
	}
	if modified {
		_, err := wsClient.Request("set_attr", SetAttrRequest{
			Path: f.Name,
			Mode: f.Meta.Mode,
			Size: f.Meta.Size,
			Uid:  f.Meta.Uid,
			Gid:  f.Meta.Gid,
		})
		if err != nil {
			log.Printf("[FUSE Setattr] File.Setattr path='%s' failed on server: %v", f.Name, err)
			return err
		}
	}
	err := f.Attr(ctx, &resp.Attr)
	if err != nil {
		log.Printf("[FUSE Setattr] File.Setattr path='%s' failed to populate attributes: %v", f.Name, err)
	}
	return err
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return &FileHandle{
		Name: f.Name,
	}, nil
}

// --- Symlink Node ---
type Symlink struct {
	Name string
	Meta NodeMeta
}

func (s *Symlink) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeSymlink | os.FileMode(s.Meta.Mode)
	a.Size = uint64(len(s.Meta.Target))
	a.Blocks = (a.Size + 511) / 512
	if s.Meta.Uid != 0 {
		a.Uid = s.Meta.Uid
	} else {
		a.Uid = uint32(os.Getuid())
	}
	if s.Meta.Gid != 0 {
		a.Gid = s.Meta.Gid
	} else {
		a.Gid = uint32(os.Getgid())
	}
	return nil
}

func (s *Symlink) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	log.Printf("[FUSE Setattr] Symlink.Setattr path='%s' Valid=%+v Uid=%d Gid=%d Mode=%v", s.Name, req.Valid, req.Uid, req.Gid, req.Mode)
	payload, err := wsClient.Request("get_attr", GetAttrRequest{Path: s.Name})
	if err != nil {
		log.Printf("[FUSE Setattr] Symlink.Setattr path='%s' failed to get attributes: %v", s.Name, err)
		return err
	}
	var meta NodeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		log.Printf("[FUSE Setattr] Symlink.Setattr path='%s' failed to unmarshal attributes: %v", s.Name, err)
		return err
	}

	modified := false
	if req.Valid.Mode() {
		meta.Mode = uint32(req.Mode)
		modified = true
	}
	if req.Valid.Uid() {
		meta.Uid = req.Uid
		modified = true
	}
	if req.Valid.Gid() {
		meta.Gid = req.Gid
		modified = true
	}

	if modified {
		s.Meta = meta
		_, err = wsClient.Request("set_attr", SetAttrRequest{
			Path: s.Name,
			Mode: meta.Mode,
			Size: meta.Size,
			Uid:  meta.Uid,
			Gid:  meta.Gid,
		})
		if err != nil {
			log.Printf("[FUSE Setattr] Symlink.Setattr path='%s' failed on server: %v", s.Name, err)
			return err
		}
	}
	err = s.Attr(ctx, &resp.Attr)
	if err != nil {
		log.Printf("[FUSE Setattr] Symlink.Setattr path='%s' failed to populate attributes: %v", s.Name, err)
	}
	return err
}

func (s *Symlink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return s.Meta.Target, nil
}
