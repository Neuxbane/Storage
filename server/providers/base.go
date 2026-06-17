package providers

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

type inFlightTransfer struct {
	done   chan struct{}
	result interface{}
	err    error
}

type BaseProvider struct {
	mu           sync.Mutex
	provName     string
	slots        []time.Time
	reservations map[string]time.Time
	transfers    map[string]*inFlightTransfer
	cooldown     time.Duration
	queue        chan func()
}

func (p *BaseProvider) Init(name string, concurrency int, cooldown time.Duration) {
	p.provName = name
	p.slots = make([]time.Time, concurrency)
	for i := range p.slots {
		p.slots[i] = time.Now()
	}
	p.reservations = make(map[string]time.Time)
	p.transfers = make(map[string]*inFlightTransfer)
	p.cooldown = cooldown
	p.queue = make(chan func(), 1000) // Large buffer for tasks

	// Start worker pool
	for i := 0; i < concurrency; i++ {
		go func() {
			for task := range p.queue {
				task()
			}
		}()
	}
}

func (p *BaseProvider) Execute(ctx context.Context, task func() (interface{}, error)) (interface{}, error) {
	type result struct {
		val interface{}
		err error
	}
	resChan := make(chan result, 1)

	select {
	case p.queue <- func() {
		val, err := task()
		resChan <- result{val, err}
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-resChan:
		return res.val, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *BaseProvider) ProcessUpload(ctx context.Context, chunkHash string, uploadFunc func() (map[string]interface{}, error)) (map[string]interface{}, error) {
	res, err := p.Deduplicate(ctx, "upload-"+chunkHash, func() (interface{}, error) {
		return p.Execute(ctx, func() (interface{}, error) {
			if err := p.WaitReservation(ctx, chunkHash); err != nil {
				return nil, err
			}
			return uploadFunc()
		})
	})
	if err != nil {
		return nil, err
	}
	return res.(map[string]interface{}), nil
}

func (p *BaseProvider) ProcessDownload(ctx context.Context, key string, downloadFunc func() ([]byte, error)) ([]byte, error) {
	res, err := p.Deduplicate(ctx, "download-"+key, func() (interface{}, error) {
		return p.Execute(ctx, func() (interface{}, error) {
			return downloadFunc()
		})
	})
	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

func (p *BaseProvider) Name() string {
	return p.provName
}

func (p *BaseProvider) ReadyAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.slots) == 0 {
		return time.Now()
	}

	latest := p.slots[0]
	for _, t := range p.slots {
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}


func (p *BaseProvider) Reserve(chunkHash string, fileSize int64) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, exists := p.reservations[chunkHash]; exists {
		return t
	}

	idx := 0
	earliest := p.slots[0]
	for i, t := range p.slots {
		if t.Before(earliest) {
			earliest = t
			idx = i
		}
	}

	now := time.Now()
	startTime := now
	if earliest.After(startTime) {
		startTime = earliest
	}

	p.slots[idx] = startTime.Add(p.cooldown)
	p.reservations[chunkHash] = startTime
	return p.slots[idx]
}

func (p *BaseProvider) ReportError(err error) {
	if err == nil {
		return
	}
	var rateErr *RateLimitError
	if errors.As(err, &rateErr) || strings.Contains(err.Error(), "429") {
		p.mu.Lock()
		defer p.mu.Unlock()
		now := time.Now()
		retryAfter := 30 * time.Second
		if rateErr != nil && rateErr.RetryAfter > 0 {
			retryAfter = rateErr.RetryAfter
		}
		for i := range p.slots {
			if p.slots[i].Before(now) {
				p.slots[i] = now.Add(retryAfter)
			} else {
				p.slots[i] = p.slots[i].Add(retryAfter)
			}
		}
	}
}

func (p *BaseProvider) Deduplicate(ctx context.Context, key string, work func() (interface{}, error)) (interface{}, error) {
	p.mu.Lock()
	if t, exists := p.transfers[key]; exists {
		p.mu.Unlock()
		select {
		case <-t.done:
			return t.result, t.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	t := &inFlightTransfer{done: make(chan struct{})}
	p.transfers[key] = t
	p.mu.Unlock()

	// Use a background context for the actual work so it doesn't get cancelled 
	// if the first caller's context is cancelled. This allows subsequent callers 
	// for the same key to still receive the result.
	result, err := work()

	p.mu.Lock()
	t.result = result
	t.err = err
	close(t.done)
	delete(p.transfers, key)
	p.mu.Unlock()

	return result, err
}

func (p *BaseProvider) WaitReservation(ctx context.Context, chunkHash string) error {
	p.mu.Lock()
	startTime, exists := p.reservations[chunkHash]
	if !exists {
		// Implicit reservation
		idx := 0
		earliest := p.slots[0]
		for i, t := range p.slots {
			if t.Before(earliest) {
				earliest = t
				idx = i
			}
		}
		startTime = time.Now()
		if earliest.After(startTime) {
			startTime = earliest
		}
		p.slots[idx] = startTime.Add(p.cooldown)
	}
	p.mu.Unlock()

	wait := time.Until(startTime)
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	p.mu.Lock()
	delete(p.reservations, chunkHash)
	p.mu.Unlock()
	return nil
}
