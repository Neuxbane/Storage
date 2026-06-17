package main

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"multistorage/server/providers"
)

// =========================================================================
// FUSE FILESYSTEM CORE CONFIGURATION & SCHEDULER VARS
// =========================================================================

type Config struct {
	MountPoint  string                         `json:"mountPoint"`
	Replication int                            `json:"replication"`
	CacheSize   string                         `json:"cacheSize"`
	ChunkSize   string                         `json:"chunkSize"`
	Providers   []map[string]map[string]string `json:"providers"`
}

var db KVStore
var chunksDB *ChunksDB
var activeProviders []Provider
var globalConfig Config

type cacheFile struct {
	path  string
	size  int64
	mtime time.Time
}

var DefaultChunkSize int64 = 10 * 1024 * 1024
var DefaultChunkSizeInt int = 10 * 1024 * 1024

func getReplicationFactor() int {
	if globalConfig.Replication > 0 {
		return globalConfig.Replication
	}
	return 2
}

func loadProviders() {
	for _, entry := range globalConfig.Providers {
		for pType, cfg := range entry {
			pObj, err := providers.CreateProvider(pType, cfg)
			if err != nil {
				log.Fatalf("Failed to initialize provider %s: %v", pType, err)
			}
			
			p, ok := pObj.(Provider)
			if !ok {
				log.Fatalf("Provider of type %s does not satisfy the Provider interface", pType)
			}

			activeProviders = append(activeProviders, p)
			log.Printf("[Core] Registered provider: %s (%s)", p.Name(), pType)
		}
	}

	if len(activeProviders) == 0 {
		log.Fatalf("Fatal: No valid active providers configured in config.json")
	}
	
	if globalConfig.Replication == 0 {
		globalConfig.Replication = 2
	}
	
	if globalConfig.CacheSize == "" {
		globalConfig.CacheSize = "2GB"
	}
	
	if globalConfig.ChunkSize != "" {
		parsed := parseCacheSize(globalConfig.ChunkSize)
		if parsed > 0 {
			DefaultChunkSize = parsed
			DefaultChunkSizeInt = int(parsed)
		}
	} else {
		// Auto-calculate optimal DefaultChunkSize based on active providers
		// We prefer a base chunk size of 10MB to allow for aggregation on providers that support more.
		var preferredSize int64 = 10 * 1024 * 1024
		var minMax int64 = -1
		for _, p := range activeProviders {
			maxSz := p.MaxChunkSize()
			if maxSz > 0 {
				if minMax == -1 || maxSz < minMax {
					minMax = maxSz
				}
			}
		}
		if minMax > 0 {
			if minMax < preferredSize {
				DefaultChunkSize = minMax
			} else {
				DefaultChunkSize = preferredSize
			}
			DefaultChunkSizeInt = int(DefaultChunkSize)
		}
	}
	
	log.Printf("[Core] Configuration loaded: MountPoint=%s, Replication=%d, CacheSize=%s, Providers=%d", 
		globalConfig.MountPoint, globalConfig.Replication, globalConfig.CacheSize, len(activeProviders))
}

func parseCacheSize(s string) int64 {
	if s == "" {
		return 2 * 1024 * 1024 * 1024 // Default 2GB
	}
	s = strings.ToUpper(strings.TrimSpace(s))
	var multiplier int64 = 1
	if strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		if strings.HasSuffix(s, "GB") {
			s = s[:len(s)-2]
		} else {
			s = s[:len(s)-1]
		}
	} else if strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		if strings.HasSuffix(s, "MB") {
			s = s[:len(s)-2]
		} else {
			s = s[:len(s)-1]
		}
	}
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 2 * 1024 * 1024 * 1024
	}
	return val * multiplier
}

func evictCacheIfNecessary(extraBytes int64) {
	cacheDir := "./cache"
	_ = os.MkdirAll(cacheDir, 0755)

	maxSize := parseCacheSize(globalConfig.CacheSize)
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		return
	}

	var totalSize int64
	var cacheFiles []cacheFile

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		info, err := file.Info()
		if err != nil {
			continue
		}
		totalSize += info.Size()
		cacheFiles = append(cacheFiles, cacheFile{
			path:  filepath.Join(cacheDir, file.Name()),
			size:  info.Size(),
			mtime: info.ModTime(),
		})
	}

	if totalSize+extraBytes <= maxSize {
		return
	}

	log.Printf("[Cache] Evicting oldest files inside limit %d bytes...", maxSize)
	sort.Slice(cacheFiles, func(i, j int) bool {
		return cacheFiles[i].mtime.Before(cacheFiles[j].mtime)
	})

	for _, cf := range cacheFiles {
		_ = os.Remove(cf.path)
		totalSize -= cf.size
		if totalSize+extraBytes <= maxSize {
			break
		}
	}
}

func getMinProviderLimit() int64 {
	var minLimit int64 = -1
	for _, p := range activeProviders {
		limit := p.MaxChunkSize()
		if limit == -1 {
			continue
		}
		if minLimit == -1 || limit < minLimit {
			minLimit = limit
		}
	}
	if minLimit == -1 {
		return DefaultChunkSize
	}
	return minLimit
}
