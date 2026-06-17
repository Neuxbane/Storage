package providers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type TelegramProvider struct {
	BaseProvider
	token   string
	chatID  int64
	bot     *tgbotapi.BotAPI
}

// NewTelegramProvider creates a new Telegram storage provider.
// chatID can be a user ID or a negative group/channel ID.
func NewTelegramProvider(name, token string, chatID int64) (*TelegramProvider, error) {
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, err
	}

	p := &TelegramProvider{
		token:  token,
		chatID: chatID,
		bot:    bot,
	}
	
	// Telegram limits to ~20 msgs per minute in groups/channels.
	// 2 concurrent slots with a 3s cooldown keeps us at ~20/min safely.
	p.Init(name, 2, 3*time.Second) 
	return p, nil
}

func NewTelegramProviderFromConfig(cfg map[string]string) (*TelegramProvider, error) {
	name := cfg["name"]
	token := cfg["token"]
	chatIDStr := cfg["chat_id"] // Note: Config uses chat_id instead of channel_id
	
	if name == "" || token == "" || chatIDStr == "" {
		return nil, fmt.Errorf("missing required telegram config (name, token, chat_id)")
	}

	chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid chat_id format, must be int64: %v", err)
	}

	return NewTelegramProvider(name, token, chatID)
}

func (p *TelegramProvider) Type() string {
	return "telegram"
}

func (p *TelegramProvider) MaxChunkSize() int64 {
	// Standard Telegram bots support up to 50MB upload, but GetFile/GetFileDirectURL retrieval is capped at 20MB.
	return 20 * 1024 * 1024
}

func (p *TelegramProvider) TotalSpace() int64 {
	// Telegram channels/chats have unlimited storage
	return -1
}

func (p *TelegramProvider) Upload(ctx context.Context, chunkHash string, data []byte) (map[string]interface{}, error) {
	return p.ProcessUpload(ctx, chunkHash, func() (map[string]interface{}, error) {
		// Prepare the file as a Telegram Document
		file := tgbotapi.FileBytes{
			Name:  chunkHash,
			Bytes: data,
		}

		msgConfig := tgbotapi.NewDocument(p.chatID, file)

		msg, err := p.bot.Send(msgConfig)
		if err != nil {
			if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "Too Many Requests") {
				// BaseProvider will catch the "429" string and back off automatically
				return nil, err
			}
			return nil, err
		}

		if msg.Document == nil {
			return nil, errors.New("uploaded successfully but no document returned in message")
		}

		log.Printf("[%s (%s)] Successfully uploaded chunk %s! MsgID: %d, FileID: %s",
			p.Name(), p.Type(), chunkHash, msg.MessageID, msg.Document.FileID)

		return map[string]interface{}{
			"provider":   p.Name(),
			"msg_id":     msg.MessageID,     // Int for Telegram
			"file_id":    msg.Document.FileID, // Unique file identifier on Telegram servers
			"chat_id":    p.chatID,
			"chunk_size": len(data),
			"chunk_hash": chunkHash,
		}, nil
	})
}

func (p *TelegramProvider) Delete(ctx context.Context, metadata map[string]interface{}) error {
	msgIDFloat, ok := metadata["msg_id"].(float64) // JSON unmarshals numbers to float64 often
	var msgID int
	if ok {
		msgID = int(msgIDFloat)
	} else if id, ok := metadata["msg_id"].(int); ok {
		msgID = id
	} else {
		return errors.New("missing or invalid msg_id in metadata")
	}

	deleteMsg := tgbotapi.NewDeleteMessage(p.chatID, msgID)
	_, err := p.bot.Request(deleteMsg)
	return err
}

func (p *TelegramProvider) Check(ctx context.Context, metadata map[string]interface{}) bool {
	fileID, _ := metadata["file_id"].(string)
	if fileID == "" {
		return false
	}

	// The cleanest way to check if a file exists on Telegram is to attempt to fetch its File object.
	// If it fails, the file is either deleted or inaccessible.
	fileConfig := tgbotapi.FileConfig{FileID: fileID}
	_, err := p.bot.GetFile(fileConfig)
	return err == nil
}

func (p *TelegramProvider) Download(ctx context.Context, metadata map[string]interface{}) ([]byte, error) {
	fileID, _ := metadata["file_id"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)

	if fileID == "" {
		return nil, errors.New("missing file_id in metadata")
	}

	// Use chunkHash as deduplication key if available, else fileID
	dedupKey := chunkHash
	if dedupKey == "" {
		dedupKey = fileID
	}

	return p.ProcessDownload(ctx, dedupKey, func() ([]byte, error) {
		// 1. Get the direct download URL for the file
		fileURL, err := p.bot.GetFileDirectURL(fileID)
		if err != nil {
			return nil, fmt.Errorf("failed to get direct file url: %v", err)
		}

		// 2. Download the file using a dedicated HTTP client
		client := &http.Client{Timeout: 60 * time.Second}

		req, err := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
		if err != nil {
			return nil, err
		}

		// Handle partial download if offset and length are provided
		if offsetVal, okOff := metadata["offset"]; okOff {
			if lengthVal, okLen := metadata["length"]; okLen {
				var offset, length int64
				switch v := offsetVal.(type) {
				case float64: offset = int64(v)
				case int: offset = int64(v)
				case int64: offset = v
				default: return nil, fmt.Errorf("invalid offset type: %T", offsetVal)
				}
				switch v := lengthVal.(type) {
				case float64: length = int64(v)
				case int: length = int64(v)
				case int64: length = v
				default: return nil, fmt.Errorf("invalid length type: %T", lengthVal)
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
			}
		}

		httpResp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != http.StatusOK && httpResp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("HTTP error during download: %s", httpResp.Status)
		}

		return io.ReadAll(httpResp.Body)
	})
}

func (p *TelegramProvider) Close() {
	// The standard go-telegram-bot-api does not hold open a persistent 
	// websocket connection like Discordgo does, so there is nothing to close here.
}