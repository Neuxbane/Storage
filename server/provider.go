package main

import (
	"context"
	"time"
)

type Provider interface {
	Name() string
	Type() string
	MaxChunkSize() int64
	Upload(ctx context.Context, chunkHash string, data []byte) (map[string]interface{}, error)
	Delete(ctx context.Context, metadata map[string]interface{}) error
	Check(ctx context.Context, metadata map[string]interface{}) bool
	Download(ctx context.Context, metadata map[string]interface{}) ([]byte, error)
	TotalSpace() int64                                  // Returns total space in bytes, -1 for infinite
	ReadyAt() time.Time                                 // Returns when the provider will be ready for next upload
	Reserve(chunkHash string, fileSize int64) time.Time // Reserves a slot and returns predicted finish time
	ReportError(err error)                              // Notifies the provider of an error to adjust backoff
	Close()
}
