package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// =========================================================================
// BACKEND SERVER MAIN ENTRY POINT
// =========================================================================

func main() {
	port := flag.String("port", "8080", "Port to run backend server on")
	pass := flag.String("pass", "secret", "Security password for backend authentication")
	mount := flag.String("mount", "", "Optional local FUSE mount directory")
	flag.Parse()

	configFile, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read configuration config.json: %v", err)
	}
	
	if err := json.Unmarshal(configFile, &globalConfig); err != nil {
		log.Fatalf("Failed to parse config.json: %v", err)
	}
	
	sqlDB, err := sql.Open("sqlite3", "multistorage.db?cache=shared")
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)

	chunksDB, err = NewChunksDB(sqlDB)
	if err != nil {
		log.Fatalf("Failed to initialize chunk catalog: %v", err)
	}

	db, err = NewJSONDB(sqlDB, chunksDB)
	if err != nil {
		log.Fatalf("Failed to initialize KV db: %v", err)
	}
	
	loadProviders()

	// Queue any replication tasks left in progress
	if chunks, errList := chunksDB.List(); errList == nil {
		for hash, cm := range chunks {
			if cm.Status == "replicating" {
				globalReplicationQueue.Push(ReplicationTask{
					ChunkHash: hash,
					ChunkSize: cm.Size,
					AddedAt:   time.Now(),
				})
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startCentralReplicationRouter(ctx, globalReplicationQueue)
	go runHealthChecker(ctx)

	if *mount != "" {
		startServerFUSE(*mount)
	}

	StartWebSocketServer(*port, *pass)
}
