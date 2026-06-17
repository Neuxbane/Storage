package main

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"bazil.org/fuse"
)

// =========================================================================
// DATABASE MODELS & DUAL JSON KV STORAGE
// =========================================================================

type Metadata = map[string]interface{}

// NodeMeta stores FUSE attributes and chunk map pointers for a file/symlink
type NodeMeta struct {
	Type    string   `json:"type"` // "file", "symlink", "dir"
	Mode    uint32   `json:"mode"`
	Size    uint64   `json:"size"`
	Mtime   uint64   `json:"mtime,omitempty"`
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

// ChunkMeta tracks the state and location of each content-addressed replica
type ChunkMeta struct {
	Hash        string                            `json:"hash"`
	Size        int64                             `json:"size"`
	Replicas    map[string]map[string]interface{} `json:"replicas"`     // providerName -> specific metadata
	LastChecked map[string]time.Time              `json:"last_checked"` // providerName -> last checked timestamp
	Status      string                            `json:"status"`       // "ready", "replicating"
}

type KVStore interface {
	Get(key string) (NodeMeta, error)
	Put(key string, meta NodeMeta) error
	Delete(key string) error
	List() (map[string]NodeMeta, error)
	Rename(oldPath, newPath string) error
}

type JSONDB struct {
	sqlDB *sql.DB
}

func splitParentAndName(path string) (string, string) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", ""
	}
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		return "", path
	}
	return path[:idx], path[idx+1:]
}

func NewJSONDB(sqlDB *sql.DB, chunksDB *ChunksDB) (*JSONDB, error) {
	// Enable foreign key support for cascade deletes
	_, _ = sqlDB.Exec("PRAGMA foreign_keys = ON;")

	// Check if old nodes table exists and lacks parent_id column
	var tableExists bool
	var hasParentID bool
	rows, err := sqlDB.Query("PRAGMA table_info(nodes)")
	if err == nil {
		for rows.Next() {
			tableExists = true
			var cid int
			var name, ctype string
			var notnull int
			var dfltVal interface{}
			var pk int
			if errScan := rows.Scan(&cid, &name, &ctype, &notnull, &dfltVal, &pk); errScan == nil {
				if name == "parent_id" {
					hasParentID = true
				}
			}
		}
		rows.Close()
	}

	if tableExists && !hasParentID {
		// Execute schema migration
		_, err = sqlDB.Exec("ALTER TABLE nodes RENAME TO nodes_old;")
		if err != nil {
			return nil, err
		}

		_, err = sqlDB.Exec(`CREATE TABLE nodes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			parent_id INTEGER REFERENCES nodes(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			mode INTEGER NOT NULL,
			size INTEGER NOT NULL,
			target TEXT,
			sources TEXT,
			mtime INTEGER,
			uid INTEGER DEFAULT 0,
			gid INTEGER DEFAULT 0,
			UNIQUE(parent_id, name)
		);`)
		if err != nil {
			return nil, err
		}

		rowsOld, err := sqlDB.Query("SELECT path, type, mode, size, target, sources, mtime FROM nodes_old ORDER BY length(path) ASC")
		if err != nil {
			return nil, err
		}
		defer rowsOld.Close()

		pathToID := make(map[string]int64)
		pathToID[""] = 0

		for rowsOld.Next() {
			var oldPath, nodeType string
			var mode uint32
			var size uint64
			var targetNull, sourcesNull sql.NullString
			var mtimeNull sql.NullInt64

			errScan := rowsOld.Scan(&oldPath, &nodeType, &mode, &size, &targetNull, &sourcesNull, &mtimeNull)
			if errScan != nil {
				return nil, errScan
			}

			parentPath, nodeName := splitParentAndName(oldPath)
			parentID := pathToID[parentPath]

			var parentArg interface{} = parentID
			if parentID == 0 {
				parentArg = nil
			}

			res, errInsert := sqlDB.Exec(
				`INSERT INTO nodes (parent_id, name, type, mode, size, target, sources, mtime, uid, gid) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)`,
				parentArg, nodeName, nodeType, mode, size, targetNull, sourcesNull, mtimeNull,
			)
			if errInsert != nil {
				return nil, errInsert
			}

			newID, _ := res.LastInsertId()
			pathToID[oldPath] = newID
		}

		_, err = sqlDB.Exec("DROP TABLE nodes_old;")
		if err != nil {
			return nil, err
		}
	} else {
		// Just create table if not exists
		_, err = sqlDB.Exec(`CREATE TABLE IF NOT EXISTS nodes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			parent_id INTEGER REFERENCES nodes(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			mode INTEGER NOT NULL,
			size INTEGER NOT NULL,
			target TEXT,
			sources TEXT,
			mtime INTEGER,
			uid INTEGER DEFAULT 0,
			gid INTEGER DEFAULT 0,
			UNIQUE(parent_id, name)
		);`)
		if err != nil {
			return nil, err
		}
	}

	// Dynamic migration to add uid and gid columns to existing DB if missing
	var hasUID, hasGID bool
	rowsInfo, errInfo := sqlDB.Query("PRAGMA table_info(nodes)")
	if errInfo == nil {
		for rowsInfo.Next() {
			var cid int
			var name, ctype string
			var notnull int
			var dfltVal interface{}
			var pk int
			if errScan := rowsInfo.Scan(&cid, &name, &ctype, &notnull, &dfltVal, &pk); errScan == nil {
				if name == "uid" {
					hasUID = true
				}
				if name == "gid" {
					hasGID = true
				}
			}
		}
		rowsInfo.Close()
	}
	if !hasUID {
		_, err = sqlDB.Exec("ALTER TABLE nodes ADD COLUMN uid INTEGER DEFAULT 0;")
		if err != nil {
			return nil, err
		}
	}
	if !hasGID {
		_, err = sqlDB.Exec("ALTER TABLE nodes ADD COLUMN gid INTEGER DEFAULT 0;")
		if err != nil {
			return nil, err
		}
	}

	return &JSONDB{sqlDB: sqlDB}, nil
}

func (db *JSONDB) ResolvePath(path string) (int64, NodeMeta, error) {
	path = strings.Trim(path, "/")
	if path == "" {
		var id int64
		var lastMeta NodeMeta
		var targetNull sql.NullString
		var sourcesNull sql.NullString
		var mtimeNull sql.NullInt64
		var uidNull, gidNull sql.NullInt64

		query := "SELECT id, type, mode, size, target, sources, mtime, uid, gid FROM nodes WHERE parent_id IS NULL AND name = ''"
		err := db.sqlDB.QueryRow(query).Scan(
			&id, &lastMeta.Type, &lastMeta.Mode, &lastMeta.Size, &targetNull, &sourcesNull, &mtimeNull, &uidNull, &gidNull,
		)
		if err == sql.ErrNoRows {
			return 0, NodeMeta{Type: "dir", Mode: 0755}, nil
		} else if err != nil {
			return 0, NodeMeta{}, err
		}

		if targetNull.Valid {
			lastMeta.Target = targetNull.String
		}
		if sourcesNull.Valid && sourcesNull.String != "" {
			if err := json.Unmarshal([]byte(sourcesNull.String), &lastMeta.Sources); err != nil {
				return 0, NodeMeta{}, err
			}
		}
		if mtimeNull.Valid {
			lastMeta.Mtime = uint64(mtimeNull.Int64)
		}
		if uidNull.Valid {
			lastMeta.Uid = uint32(uidNull.Int64)
		}
		if gidNull.Valid {
			lastMeta.Gid = uint32(gidNull.Int64)
		}
		return id, lastMeta, nil
	}

	segments := strings.Split(path, "/")
	var currentParentID interface{} = nil

	var lastID int64
	var lastMeta NodeMeta

	for _, seg := range segments {
		var query string
		var args []interface{}
		if currentParentID == nil {
			query = "SELECT id, type, mode, size, target, sources, mtime, uid, gid FROM nodes WHERE parent_id IS NULL AND name = ?"
			args = []interface{}{seg}
		} else {
			query = "SELECT id, type, mode, size, target, sources, mtime, uid, gid FROM nodes WHERE parent_id = ? AND name = ?"
			args = []interface{}{currentParentID, seg}
		}

		var id int64
		var targetNull sql.NullString
		var sourcesNull sql.NullString
		var mtimeNull sql.NullInt64
		var uidNull, gidNull sql.NullInt64

		err := db.sqlDB.QueryRow(query, args...).Scan(
			&id, &lastMeta.Type, &lastMeta.Mode, &lastMeta.Size, &targetNull, &sourcesNull, &mtimeNull, &uidNull, &gidNull,
		)
		if err == sql.ErrNoRows {
			return 0, NodeMeta{}, fuse.ENOENT
		} else if err != nil {
			return 0, NodeMeta{}, err
		}

		if targetNull.Valid {
			lastMeta.Target = targetNull.String
		}
		if sourcesNull.Valid && sourcesNull.String != "" {
			if err := json.Unmarshal([]byte(sourcesNull.String), &lastMeta.Sources); err != nil {
				return 0, NodeMeta{}, err
			}
		}
		if mtimeNull.Valid {
			lastMeta.Mtime = uint64(mtimeNull.Int64)
		}
		if uidNull.Valid {
			lastMeta.Uid = uint32(uidNull.Int64)
		}
		if gidNull.Valid {
			lastMeta.Gid = uint32(gidNull.Int64)
		}

		currentParentID = id
		lastID = id
	}

	return lastID, lastMeta, nil
}

func (db *JSONDB) Get(key string) (NodeMeta, error) {
	_, meta, err := db.ResolvePath(key)
	return meta, err
}

func (db *JSONDB) Put(key string, meta NodeMeta) error {
	if key == "" {
		var existingID int64
		queryExist := "SELECT id FROM nodes WHERE parent_id IS NULL AND name = ''"
		errExist := db.sqlDB.QueryRow(queryExist).Scan(&existingID)
		var err error
		if errExist == sql.ErrNoRows {
			_, err = db.sqlDB.Exec(
				`INSERT INTO nodes (parent_id, name, type, mode, size, target, sources, mtime, uid, gid) 
				VALUES (NULL, '', ?, ?, ?, ?, '', ?, ?, ?)`,
				meta.Type, meta.Mode, meta.Size, meta.Target, meta.Mtime, meta.Uid, meta.Gid,
			)
		} else if errExist == nil {
			_, err = db.sqlDB.Exec(
				`UPDATE nodes SET type = ?, mode = ?, size = ?, target = ?, sources = '', mtime = ?, uid = ?, gid = ? 
				WHERE id = ?`,
				meta.Type, meta.Mode, meta.Size, meta.Target, meta.Mtime, meta.Uid, meta.Gid, existingID,
			)
		} else {
			err = errExist
		}
		return err
	}

	parentPath, nodeName := splitParentAndName(key)
	parentID, _, err := db.ResolvePath(parentPath)
	if err != nil {
		return err
	}

	var parentArg interface{} = parentID
	if parentID == 0 {
		parentArg = nil
	}

	var sourcesJSON string
	if len(meta.Sources) > 0 {
		b, err := json.Marshal(meta.Sources)
		if err != nil {
			return err
		}
		sourcesJSON = string(b)
	}

	var existingID int64
	var queryExist string
	var existArgs []interface{}
	if parentArg == nil {
		queryExist = "SELECT id FROM nodes WHERE parent_id IS NULL AND name = ?"
		existArgs = []interface{}{nodeName}
	} else {
		queryExist = "SELECT id FROM nodes WHERE parent_id = ? AND name = ?"
		existArgs = []interface{}{parentArg, nodeName}
	}

	errExist := db.sqlDB.QueryRow(queryExist, existArgs...).Scan(&existingID)
	if errExist == sql.ErrNoRows {
		_, err = db.sqlDB.Exec(
			`INSERT INTO nodes (parent_id, name, type, mode, size, target, sources, mtime, uid, gid) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			parentArg, nodeName, meta.Type, meta.Mode, meta.Size, meta.Target, sourcesJSON, meta.Mtime, meta.Uid, meta.Gid,
		)
	} else if errExist == nil {
		_, err = db.sqlDB.Exec(
			`UPDATE nodes SET type = ?, mode = ?, size = ?, target = ?, sources = ?, mtime = ?, uid = ?, gid = ? 
			WHERE id = ?`,
			meta.Type, meta.Mode, meta.Size, meta.Target, sourcesJSON, meta.Mtime, meta.Uid, meta.Gid, existingID,
		)
	} else {
		err = errExist
	}

	return err
}

func (db *JSONDB) Delete(key string) error {
	id, _, err := db.ResolvePath(key)
	if err != nil {
		return err
	}

	_, err = db.sqlDB.Exec("DELETE FROM nodes WHERE id = ?", id)
	return err
}

func (db *JSONDB) List() (map[string]NodeMeta, error) {
	rows, err := db.sqlDB.Query("SELECT id, parent_id, name, type, mode, size, target, sources, mtime, uid, gid FROM nodes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type nodeInfo struct {
		id       int64
		parentID int64
		name     string
		meta     NodeMeta
	}

	var nodes []nodeInfo
	for rows.Next() {
		var ni nodeInfo
		var parentIDNull sql.NullInt64
		var targetNull sql.NullString
		var sourcesNull sql.NullString
		var mtimeNull sql.NullInt64
		var uidNull, gidNull sql.NullInt64

		errScan := rows.Scan(
			&ni.id, &parentIDNull, &ni.name, &ni.meta.Type, &ni.meta.Mode, &ni.meta.Size,
			&targetNull, &sourcesNull, &mtimeNull, &uidNull, &gidNull,
		)
		if errScan != nil {
			return nil, errScan
		}

		if parentIDNull.Valid {
			ni.parentID = parentIDNull.Int64
		}
		if targetNull.Valid {
			ni.meta.Target = targetNull.String
		}
		if sourcesNull.Valid && sourcesNull.String != "" {
			_ = json.Unmarshal([]byte(sourcesNull.String), &ni.meta.Sources)
		}
		if mtimeNull.Valid {
			ni.meta.Mtime = uint64(mtimeNull.Int64)
		}
		if uidNull.Valid {
			ni.meta.Uid = uint32(uidNull.Int64)
		}
		if gidNull.Valid {
			ni.meta.Gid = uint32(gidNull.Int64)
		}

		nodes = append(nodes, ni)
	}

	nodeMap := make(map[int64]nodeInfo)
	for _, n := range nodes {
		nodeMap[n.id] = n
	}

	var buildPath func(id int64) string
	buildPath = func(id int64) string {
		n, exists := nodeMap[id]
		if !exists {
			return ""
		}
		if n.parentID == 0 {
			return n.name
		}
		parentPath := buildPath(n.parentID)
		if parentPath == "" {
			return n.name
		}
		return parentPath + "/" + n.name
	}

	results := make(map[string]NodeMeta)
	for _, n := range nodes {
		fullPath := buildPath(n.id)
		results[fullPath] = n.meta
	}

	return results, nil
}

func (db *JSONDB) Rename(oldPath, newPath string) error {
	id, _, err := db.ResolvePath(oldPath)
	if err != nil {
		return err
	}

	newParentPath, newName := splitParentAndName(newPath)
	newParentID, _, err := db.ResolvePath(newParentPath)
	if err != nil {
		return err
	}

	var parentArg interface{} = newParentID
	if newParentID == 0 {
		parentArg = nil
	}

	_, err = db.sqlDB.Exec("UPDATE nodes SET name = ?, parent_id = ? WHERE id = ?", newName, parentArg, id)
	return err
}

// ChunksDB tracks replica coordinates globally
type ChunksDB struct {
	sqlDB *sql.DB
}

func NewChunksDB(sqlDB *sql.DB) (*ChunksDB, error) {
	_, err := sqlDB.Exec(`CREATE TABLE IF NOT EXISTS chunks (
		hash TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		replicas TEXT NOT NULL,
		last_checked TEXT NOT NULL,
		status TEXT NOT NULL
	);`)
	if err != nil {
		return nil, err
	}

	_, err = sqlDB.Exec(`CREATE TABLE IF NOT EXISTS container_refs (
		container_id TEXT PRIMARY KEY,
		ref_count INTEGER NOT NULL
	);`)
	if err != nil {
		return nil, err
	}

	return &ChunksDB{sqlDB: sqlDB}, nil
}

func (db *ChunksDB) IncRef(containerID string) error {
	_, err := db.sqlDB.Exec(`
		INSERT INTO container_refs (container_id, ref_count) 
		VALUES (?, 1) 
		ON CONFLICT(container_id) DO UPDATE SET ref_count = ref_count + 1;`,
		containerID)
	return err
}

func (db *ChunksDB) DecRef(containerID string) (int, error) {
	var refCount int
	err := db.sqlDB.QueryRow(`
		UPDATE container_refs 
		SET ref_count = ref_count - 1 
		WHERE container_id = ? 
		RETURNING ref_count;`,
		containerID).Scan(&refCount)
	if err != nil {
		return 0, err
	}
	return refCount, nil
}

func (db *ChunksDB) Get(hash string) (ChunkMeta, error) {
	var cm ChunkMeta
	var replicasJSON, lastCheckedJSON string
	err := db.sqlDB.QueryRow("SELECT hash, size, replicas, last_checked, status FROM chunks WHERE hash = ?", hash).Scan(
		&cm.Hash, &cm.Size, &replicasJSON, &lastCheckedJSON, &cm.Status,
	)
	if err == sql.ErrNoRows {
		return ChunkMeta{}, fuse.ENOENT
	} else if err != nil {
		return ChunkMeta{}, err
	}

	if err := json.Unmarshal([]byte(replicasJSON), &cm.Replicas); err != nil {
		return ChunkMeta{}, err
	}
	if err := json.Unmarshal([]byte(lastCheckedJSON), &cm.LastChecked); err != nil {
		return ChunkMeta{}, err
	}
	return cm, nil
}

func (db *ChunksDB) Put(hash string, meta ChunkMeta) error {
	if meta.Replicas == nil {
		meta.Replicas = make(map[string]map[string]interface{})
	}
	if meta.LastChecked == nil {
		meta.LastChecked = make(map[string]time.Time)
	}
	replicasJSON, err := json.Marshal(meta.Replicas)
	if err != nil {
		return err
	}
	lastCheckedJSON, err := json.Marshal(meta.LastChecked)
	if err != nil {
		return err
	}

	_, err = db.sqlDB.Exec(
		"INSERT OR REPLACE INTO chunks (hash, size, replicas, last_checked, status) VALUES (?, ?, ?, ?, ?)",
		hash, meta.Size, string(replicasJSON), string(lastCheckedJSON), meta.Status,
	)
	return err
}

func (db *ChunksDB) Delete(hash string) error {
	_, err := db.sqlDB.Exec("DELETE FROM chunks WHERE hash = ?", hash)
	return err
}

func (db *ChunksDB) List() (map[string]ChunkMeta, error) {
	rows, err := db.sqlDB.Query("SELECT hash, size, replicas, last_checked, status FROM chunks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make(map[string]ChunkMeta)
	for rows.Next() {
		var cm ChunkMeta
		var replicasJSON, lastCheckedJSON string
		if err := rows.Scan(&cm.Hash, &cm.Size, &replicasJSON, &lastCheckedJSON, &cm.Status); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(replicasJSON), &cm.Replicas); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(lastCheckedJSON), &cm.LastChecked); err != nil {
			return nil, err
		}
		results[cm.Hash] = cm
	}
	return results, nil
}

func isChunkReferenced(chunkHash string, currentFile string) bool {
	items, err := db.List()
	if err != nil {
		return false
	}
	for path, meta := range items {
		if path == currentFile {
			continue
		}
		if meta.Type == "file" {
			for _, h := range meta.Sources {
				if h == chunkHash {
					return true
				}
			}
		}
	}
	return false
}
