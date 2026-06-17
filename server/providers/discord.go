package providers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

type DiscordProvider struct {
	BaseProvider
	token        string
	channelID    string
	session      *discordgo.Session
}

func NewDiscordProvider(name, token, channelID string) (*DiscordProvider, error) {
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, err
	}
	err = session.Open()
	if err != nil {
		return nil, err
	}
	
	p := &DiscordProvider{
		token:     token,
		channelID: channelID,
		session:   session,
	}
	p.Init(name, 5, 3*time.Second) // 5 concurrent slots, 3s cooldown
	return p, nil
}

func NewDiscordProviderFromConfig(cfg map[string]string) (*DiscordProvider, error) {
	name := cfg["name"]
	token := cfg["token"]
	channelID := cfg["channel_id"]
	if name == "" || token == "" || channelID == "" {
		return nil, fmt.Errorf("missing required discord config (name, token, channel_id)")
	}
	return NewDiscordProvider(name, token, channelID)
}

func (p *DiscordProvider) Type() string {
	return "discord"
}

func (p *DiscordProvider) MaxChunkSize() int64 {
	// Discord limits attachment uploads to 10MB
	return 10 * 1024 * 1024
}

func (p *DiscordProvider) TotalSpace() int64 {
	// Discord channels have no strictly defined storage limit
	return -1
}

func (p *DiscordProvider) Upload(ctx context.Context, chunkHash string, data []byte) (map[string]interface{}, error) {
	return p.ProcessUpload(ctx, chunkHash, func() (map[string]interface{}, error) {
		discordFile := &discordgo.File{
			Name:   chunkHash,
			Reader: bytes.NewReader(data),
		}

		msg, err := p.session.ChannelMessageSendComplex(p.channelID, &discordgo.MessageSend{
			Content: "",
			Files:   []*discordgo.File{discordFile},
		})

		if err != nil {
			if strings.Contains(err.Error(), "429") {
				return nil, &RateLimitError{RetryAfter: 5 * time.Second, Message: "discord rate limit exceeded"}
			}
			return nil, err
		}

		log.Printf("[%s (%s)] Successfully uploaded chunk %s! MsgID: %s",
			p.Name(), p.Type(), chunkHash, msg.ID)

		return map[string]interface{}{
			"provider":   p.Name(),
			"msg_id":     msg.ID,
			"channel_id": p.channelID,
			"chunk_size": len(data),
			"chunk_hash": chunkHash,
		}, nil
	})
}

func (p *DiscordProvider) Delete(ctx context.Context, metadata map[string]interface{}) error {
	msgID, _ := metadata["msg_id"].(string)
	if msgID == "" {
		return errors.New("missing msg_id in metadata")
	}
	return p.session.ChannelMessageDelete(p.channelID, msgID)
}

func (p *DiscordProvider) Check(ctx context.Context, metadata map[string]interface{}) bool {
	msgID, _ := metadata["msg_id"].(string)
	if msgID == "" {
		return false
	}
	_, err := p.session.ChannelMessage(p.channelID, msgID)
	return err == nil
}

func (p *DiscordProvider) Download(ctx context.Context, metadata map[string]interface{}) ([]byte, error) {
	msgID, _ := metadata["msg_id"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)
	if msgID == "" {
		return nil, errors.New("missing msg_id in metadata")
	}

	// Use chunkHash as deduplication key if available, else msgID
	dedupKey := chunkHash
	if dedupKey == "" {
		dedupKey = msgID
	}

	return p.ProcessDownload(ctx, dedupKey, func() ([]byte, error) {
		msg, err := p.session.ChannelMessage(p.channelID, msgID)
		if err != nil || len(msg.Attachments) == 0 {
			return nil, fmt.Errorf("failed to fetch message details: %v", err)
		}

		attachmentURL := msg.Attachments[0].URL

		// Use a dedicated client with a reasonable timeout for chunk downloads
		client := &http.Client{Timeout: 60 * time.Second}

		req, err := http.NewRequest("GET", attachmentURL, nil)
		if err != nil {
			return nil, err
		}

		httpResp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP error: %s", httpResp.Status)
		}

		return io.ReadAll(httpResp.Body)
	})
}

func (p *DiscordProvider) Close() {
	if p.session != nil {
		p.session.Close()
	}
}
