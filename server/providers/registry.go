package providers

import (
	"fmt"
	"time"
)

type RateLimitError struct {
	RetryAfter time.Duration
	Message    string
}

func (e *RateLimitError) Error() string {
	return e.Message
}

// Factory is a generic way to create providers from a config map.
// Since Provider interface is in package main, we return interface{} here
// and let the caller cast it. This breaks direct compile-time dependency on main.
func CreateProvider(pType string, cfg map[string]string) (interface{}, error) {
	switch pType {
	case "discord":
		return NewDiscordProviderFromConfig(cfg)
	case "telegram":
		return NewTelegramProviderFromConfig(cfg)
	case "filebin":
		return NewFilebinProviderFromConfig(cfg)
	default:
		return nil, fmt.Errorf("unknown provider type: %s", pType)
	}
}
