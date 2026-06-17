package providers

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FilebinProvider struct {
	BaseProvider
	mu           sync.Mutex
	currentBinID string
	binCreatedAt time.Time
	client       *http.Client
}

func randString(n int) string {
	b := make([]byte, n/2+1)
	if _, err := rand.Read(b); err != nil {
		return "abc123xyz"
	}
	return hex.EncodeToString(b)[:n]
}

func generateBinID() string {
	timestamp := time.Now().Unix()
	randomSuffix := randString(12)
	return fmt.Sprintf("ms-%d-%s", timestamp, randomSuffix)
}

func getBinAge(binID string) time.Duration {
	parts := strings.Split(binID, "-")
	if len(parts) < 3 || parts[0] != "ms" {
		return 0 // Unknown format
	}
	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0
	}
	return time.Since(time.Unix(ts, 0))
}

func NewFilebinProvider(name string) (*FilebinProvider, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}
	p := &FilebinProvider{
		currentBinID: generateBinID(),
		binCreatedAt: time.Now(),
		client: &http.Client{
			Timeout: 60 * time.Second,
			Jar:     jar,
		},
	}
	// Init base provider with 5 concurrency slots and 1s cooldown
	p.Init(name, 5, 1*time.Second)
	log.Printf("[Filebin] Initialized provider %s with bin ID: %s", name, p.currentBinID)
	return p, nil
}

func NewFilebinProviderFromConfig(cfg map[string]string) (*FilebinProvider, error) {
	name := cfg["name"]
	if name == "" {
		return nil, fmt.Errorf("missing required filebin config (name)")
	}
	return NewFilebinProvider(name)
}

func (p *FilebinProvider) Type() string {
	return "filebin"
}

func (p *FilebinProvider) MaxChunkSize() int64 {
	// Standard Filebin has generous file limits, let's say 100MB max per chunk
	return 100 * 1024 * 1024
}

func (p *FilebinProvider) TotalSpace() int64 {
	return -1 // Unlimited capacity
}

func (p *FilebinProvider) getOrRotateBin() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Rotate bin ID every 24 hours to keep active uploads fresh
	if time.Since(p.binCreatedAt) > 24*time.Hour {
		oldBin := p.currentBinID
		p.currentBinID = generateBinID()
		p.binCreatedAt = time.Now()
		log.Printf("[Filebin] Rotated bin ID for %s: %s -> %s", p.Name(), oldBin, p.currentBinID)
	}
	return p.currentBinID
}

func (p *FilebinProvider) Upload(ctx context.Context, chunkHash string, data []byte) (map[string]interface{}, error) {
	binID := p.getOrRotateBin()

	res, err := p.ProcessUpload(ctx, chunkHash, func() (map[string]interface{}, error) {
		url := fmt.Sprintf("https://filebin.net/%s/%s", binID, chunkHash)
		log.Printf("[Filebin %s] Uploading chunk %s to bin %s (%d bytes)...", p.Name(), chunkHash[:8], binID, len(data))

		uploadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(uploadCtx, "POST", url, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := p.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("upload failed with status %s: %s", resp.Status, string(body))
		}

		return map[string]interface{}{
			"provider":   p.Name(),
			"bin_id":     binID,
			"chunk_hash": chunkHash,
			"chunk_size": len(data),
		}, nil
	})

	if err != nil {
		return nil, err
	}
	return res, nil
}

func (p *FilebinProvider) Download(ctx context.Context, metadata map[string]interface{}) ([]byte, error) {
	binID, _ := metadata["bin_id"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)
	if binID == "" || chunkHash == "" {
		return nil, fmt.Errorf("missing bin_id or chunk_hash in filebin metadata")
	}

	return p.ProcessDownload(ctx, chunkHash, func() ([]byte, error) {
		url := fmt.Sprintf("https://filebin.net/%s/%s", binID, chunkHash)
		log.Printf("[Filebin %s] Downloading chunk %s from bin %s...", p.Name(), chunkHash[:8], binID)

		downloadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(downloadCtx, "GET", url, nil)
		if err != nil {
			return nil, err
		}

		// Handle range requests if client provides offsets/length
		if offsetVal, okOff := metadata["offset"]; okOff {
			if lengthVal, okLen := metadata["length"]; okLen {
				var offset, length int64
				switch v := offsetVal.(type) {
				case float64: offset = int64(v)
				case int: offset = int64(v)
				case int64: offset = v
				}
				switch v := lengthVal.(type) {
				case float64: length = int64(v)
				case int: length = int64(v)
				case int64: length = v
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
			}
		}

		resp, err := p.client.Do(req)
		if err != nil {
			return nil, err
		}

		// Check if we hit the Filebin disclaimer warning page ("Heads up!")
		contentType := resp.Header.Get("Content-Type")
		if strings.Contains(contentType, "text/html") {
			bodyBytes, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}

			bodyStr := string(bodyBytes)
			if strings.Contains(bodyStr, "Heads up!") && strings.Contains(bodyStr, "Proceed to download") {
				log.Printf("[Filebin %s] Encountered 'Heads up!' warning page. Retrying request to set verification cookie...", p.Name())

				retryCtx, retryCancel := context.WithTimeout(ctx, 60*time.Second)
				defer retryCancel()

				retryReq, err := http.NewRequestWithContext(retryCtx, "GET", url, nil)
				if err != nil {
					return nil, err
				}
				if rangeHeader := req.Header.Get("Range"); rangeHeader != "" {
					retryReq.Header.Set("Range", rangeHeader)
				}

				resp, err = p.client.Do(retryReq)
				if err != nil {
					return nil, err
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
					return nil, fmt.Errorf("download failed after retry with status %s", resp.Status)
				}

				// Double check to ensure we didn't receive the HTML disclaimer page again
				contentType = resp.Header.Get("Content-Type")
				if strings.Contains(contentType, "text/html") {
					retryBody, _ := io.ReadAll(resp.Body)
					if strings.Contains(string(retryBody), "Heads up!") {
						return nil, fmt.Errorf("filebin warning page bypass failed, got warning page again")
					}
					return retryBody, nil
				}

				return io.ReadAll(resp.Body)
			}

			return bodyBytes, nil
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("download failed with status %s", resp.Status)
		}

		return io.ReadAll(resp.Body)
	})
}

func (p *FilebinProvider) Delete(ctx context.Context, metadata map[string]interface{}) error {
	binID, _ := metadata["bin_id"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)
	if binID == "" || chunkHash == "" {
		return fmt.Errorf("missing bin_id or chunk_hash in filebin metadata")
	}

	url := fmt.Sprintf("https://filebin.net/%s/%s", binID, chunkHash)
	deleteCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(deleteCtx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete failed with status %s", resp.Status)
	}
	return nil
}

func (p *FilebinProvider) Check(ctx context.Context, metadata map[string]interface{}) bool {
	binID, _ := metadata["bin_id"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)
	if binID == "" || chunkHash == "" {
		return false
	}

	// PROACTIVE ROTATION MIGRATION:
	// Filebin links expire after 6 days. If this chunk's bin is older than 4 days,
	// we pro-actively report that it "does not exist" so the background replication 
	// checker immediately enqueues it to migrate into the new active bin.
	if getBinAge(binID) > 4*24*time.Hour {
		log.Printf("[Filebin %s] Chunk %s is in an old bin (%s) older than 4 days. Simulating miss to trigger proactive self-healing.", p.Name(), chunkHash[:8], binID)
		return false
	}

	url := fmt.Sprintf("https://filebin.net/%s/%s", binID, chunkHash)
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(checkCtx, "HEAD", url, nil)
	if err != nil {
		return false
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func (p *FilebinProvider) Close() {}
