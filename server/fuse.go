package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// safeUnmount attempts a normal unmount, and falls back to a lazy unmount if the device is busy.
func safeUnmount(mountpoint string) {
	err := fuse.Unmount(mountpoint)
	if err == nil {
		log.Println("[FUSE] Successfully unmounted active mount.")
		return
	}

	errStr := err.Error()
	if strings.Contains(errStr, "not found in /etc/mtab") || strings.Contains(errStr, "not mounted") || strings.Contains(errStr, "Invalid argument") {
		return
	}

	log.Printf("[FUSE] Standard unmount failed: %v. Retrying lazy unmount...", err)
	cmd := exec.Command("fusermount", "-z", "-u", mountpoint)
	if errLazy := cmd.Run(); errLazy == nil {
		log.Println("[FUSE] Lazy unmount successful.")
		return
	}

	cmd3 := exec.Command("fusermount3", "-z", "-u", mountpoint)
	if errLazy3 := cmd3.Run(); errLazy3 == nil {
		log.Println("[FUSE] Lazy unmount successful.")
		return
	}
}

type ServerFS struct{}

func (f *ServerFS) Root() (fs.Node, error) {
	return &ServerDir{Path: ""}, nil
}

func (f *ServerFS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	resp.Bsize = 4096
	const infiniteSize = 1 << 62
	resp.Blocks = infiniteSize / uint64(resp.Bsize)
	resp.Bfree = infiniteSize / uint64(resp.Bsize)
	resp.Bavail = resp.Bfree
	resp.Files = 1 << 32
	resp.Ffree = resp.Files
	return nil
}

type ServerDir struct {
	Path string
}

func (d *ServerDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0755
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())

	if d.Path != "" {
		meta, err := db.Get(d.Path)
		if err == nil {
			a.Mode = os.ModeDir | os.FileMode(meta.Mode)
			if meta.Uid != 0 {
				a.Uid = meta.Uid
			}
			if meta.Gid != 0 {
				a.Gid = meta.Gid
			}
		}
	}
	return nil
}

func (d *ServerDir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	meta, err := db.Get(d.Path)
	if err != nil {
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
		err := db.Put(d.Path, meta)
		if err != nil {
			return err
		}
	}
	return d.Attr(ctx, &resp.Attr)
}

func (d *ServerDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if isIgnored(name) {
		return nil, fuse.ENOENT
	}
	fullPath := name
	if d.Path != "" {
		fullPath = d.Path + "/" + name
	}
	
	meta, err := db.Get(fullPath)
	if err != nil {
		return nil, err
	}

	if meta.Type == "symlink" {
		return &ServerSymlink{Name: fullPath, Meta: meta}, nil
	}
	if meta.Type == "dir" {
		return &ServerDir{Path: fullPath}, nil
	}
	return &ServerFile{Name: fullPath, Meta: meta}, nil
}

func (d *ServerDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	items, err := db.List()
	if err != nil {
		return nil, err
	}

	prefix := ""
	if d.Path != "" {
		prefix = d.Path + "/"
	}

	var entries []fuse.Dirent
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

		t := fuse.DT_File
		if meta.Type == "symlink" {
			t = fuse.DT_Link
		} else if meta.Type == "dir" {
			t = fuse.DT_Dir
		}

		entries = append(entries, fuse.Dirent{Name: rel, Type: t})
	}
	return entries, nil
}

func (d *ServerDir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if isIgnored(req.Name) {
		return nil, nil, fuse.EPERM
	}
	fullPath := req.Name
	if d.Path != "" {
		fullPath = d.Path + "/" + req.Name
	}
	
	meta := NodeMeta{
		Type: "file",
		Mode: uint32(req.Mode),
		Size: 0,
		Uid:  req.Uid,
		Gid:  req.Gid,
	}
	err := db.Put(fullPath, meta)
	if err != nil {
		return nil, nil, fuse.EIO
	}
	
	handle := &ServerFileHandle{
		Name: fullPath,
	}

	resp.Attr.Mode = req.Mode
	resp.Attr.Size = 0
	resp.Attr.Uid = req.Uid
	resp.Attr.Gid = req.Gid

	return &ServerFile{Name: fullPath, Meta: meta}, handle, nil
}

func (d *ServerDir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fullPath := req.Name
	if d.Path != "" {
		fullPath = d.Path + "/" + req.Name
	}
	meta, err := db.Get(fullPath)
	if err != nil {
		return err
	}
	if meta.Type == "dir" {
		items, _ := db.List()
		prefix := fullPath + "/"
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
		cleanUpNodeChunksServer(fullPath, meta)
	}
	return db.Delete(fullPath)
}

func (d *ServerDir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	fullPath := req.NewName
	if d.Path != "" {
		fullPath = d.Path + "/" + req.NewName
	}

	meta := NodeMeta{
		Type:   "symlink",
		Mode:   uint32(os.ModeSymlink | 0777),
		Target: req.Target,
		Uid:    req.Uid,
		Gid:    req.Gid,
	}
	err := db.Put(fullPath, meta)
	if err != nil {
		return nil, fuse.EIO
	}
	return &ServerSymlink{Name: fullPath, Meta: meta}, nil
}

func (d *ServerDir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dstDir, ok := newDir.(*ServerDir)
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

	return db.Rename(oldPath, newPath)
}

func (d *ServerDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if isIgnored(req.Name) {
		return nil, fuse.EPERM
	}
	fullPath := req.Name
	if d.Path != "" {
		fullPath = d.Path + "/" + req.Name
	}

	meta := NodeMeta{
		Type: "dir",
		Mode: uint32(req.Mode),
		Uid:  req.Uid,
		Gid:  req.Gid,
	}
	err := db.Put(fullPath, meta)
	if err != nil {
		return nil, fuse.EIO
	}
	return &ServerDir{Path: fullPath}, nil
}

type ServerFile struct {
	Name string
	Meta NodeMeta
}

func (f *ServerFile) Attr(ctx context.Context, a *fuse.Attr) error {
	meta, err := db.Get(f.Name)
	if err == nil {
		f.Meta = meta
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

func (f *ServerFile) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
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
		err := db.Put(f.Name, f.Meta)
		if err != nil {
			return err
		}
	}
	return f.Attr(ctx, &resp.Attr)
}

func (f *ServerFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return &ServerFileHandle{
		Name: f.Name,
	}, nil
}

type ServerSymlink struct {
	Name string
	Meta NodeMeta
}

func (s *ServerSymlink) Attr(ctx context.Context, a *fuse.Attr) error {
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

func (s *ServerSymlink) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	meta, err := db.Get(s.Name)
	if err != nil {
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
		err := db.Put(s.Name, meta)
		if err != nil {
			return err
		}
	}
	return s.Attr(ctx, &resp.Attr)
}

func (s *ServerSymlink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return s.Meta.Target, nil
}

type ServerFileHandle struct {
	Name string
}

func (h *ServerFileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if len(req.Data) == 0 {
		return nil
	}

	buf, err := getServerWriteBuffer(h.Name)
	if err != nil {
		return err
	}

	buf.mu.Lock()
	buf.dirty = true
	end := req.Offset + int64(len(req.Data))
	if end > int64(len(buf.data)) {
		newData := make([]byte, end)
		copy(newData, buf.data)
		buf.data = newData
	}
	copy(buf.data[req.Offset:end], req.Data)
	buf.mu.Unlock()

	resp.Size = len(req.Data)
	return nil
}

func (h *ServerFileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return flushFile(h.Name)
}

func (h *ServerFileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

func (h *ServerFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// First check active un-flushed write buffer
	var data []byte
	serverActiveBuffers.Lock()
	buf, exists := serverActiveBuffers.buffers[h.Name]
	if exists {
		buf.mu.Lock()
		if int64(len(buf.data)) > req.Offset {
			end := req.Offset + int64(req.Size)
			if end > int64(len(buf.data)) {
				end = int64(len(buf.data))
			}
			data = make([]byte, end-req.Offset)
			copy(data, buf.data[req.Offset:end])
		}
		buf.mu.Unlock()
	}
	serverActiveBuffers.Unlock()

	if data == nil {
		// Fall back to reading committed chunks (with optimized direct slice loading!)
		var err error
		data, err = readRawBytes(h.Name, req.Offset, req.Size)
		if err != nil {
			return err
		}
	}

	resp.Data = data
	return nil
}

func startServerFUSE(mountpoint string) {
	log.Printf("[FUSE] Initializing local FUSE mount at directory '%s'...", mountpoint)
	safeUnmount(mountpoint)

	c, err := fuse.Mount(mountpoint, fuse.FSName("multistorage"), fuse.Subtype("genericfs"))
	if err != nil {
		log.Fatalf("[FUSE] Failed to mount locally: %v", err)
	}

	go func() {
		serveErr := fs.Serve(c, &ServerFS{})
		c.Close()
		if serveErr != nil {
			log.Printf("[FUSE] Local serve finished with error: %v", serveErr)
		} else {
			log.Println("[FUSE] Local serve finished cleanly.")
		}
	}()
	log.Printf("[FUSE] Mounted locally successfully at '%s'.", mountpoint)
}
