package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/gorilla/websocket"
)

// safeUnmount attempts a normal unmount, and falls back to a lazy unmount if the device is busy.
func safeUnmount(mountpoint string) {
	err := fuse.Unmount(mountpoint)
	if err == nil {
		log.Println("Successfully unmounted stale/active mount.")
		return
	}

	errStr := err.Error()
	if strings.Contains(errStr, "not found in /etc/mtab") || strings.Contains(errStr, "not mounted") || strings.Contains(errStr, "Invalid argument") {
		return
	}

	log.Printf("Standard unmount failed: %v. Retrying with lazy unmount...", err)
	cmd := exec.Command("fusermount", "-z", "-u", mountpoint)
	if errLazy := cmd.Run(); errLazy == nil {
		log.Println("Lazy unmount successful via fusermount.")
		return
	}

	cmd3 := exec.Command("fusermount3", "-z", "-u", mountpoint)
	if errLazy3 := cmd3.Run(); errLazy3 == nil {
		log.Println("Lazy unmount successful via fusermount3.")
		return
	}

	log.Printf("ERROR: All unmount methods failed. Directory '%s' might still be locked.", mountpoint)
}

// =========================================================================
// WEBSOCKET CLIENT MANAGER
// =========================================================================

type WSClient struct {
	conn    *websocket.Conn
	mu      sync.Mutex
	chans   map[string]chan WSResponse
	writeMu sync.Mutex
}

var wsClient *WSClient

func (c *WSClient) Request(reqType string, payload interface{}) (json.RawMessage, error) {
	c.mu.Lock()
	if c.chans == nil {
		c.chans = make(map[string]chan WSResponse)
	}
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	ch := make(chan WSResponse, 1)
	c.chans[id] = ch
	c.mu.Unlock()

	var req WSRequest
	req.Type = reqType
	req.ID = id
	if payload != nil {
		b, _ := json.Marshal(payload)
		req.Payload = b
	}

	b, _ := json.Marshal(req)
	
	c.writeMu.Lock()
	err := c.conn.WriteMessage(websocket.TextMessage, b)
	c.writeMu.Unlock()

	if err != nil {
		c.mu.Lock()
		delete(c.chans, id)
		c.mu.Unlock()
		return nil, err
	}

	resp := <-ch

	c.mu.Lock()
	delete(c.chans, id)
	c.mu.Unlock()

	if resp.Error != "" {
		if strings.Contains(resp.Error, "no such file") || strings.Contains(resp.Error, "file does not exist") || strings.Contains(resp.Error, "entry not found") {
			return nil, fuse.ENOENT
		}
		return nil, fmt.Errorf(resp.Error)
	}

	return resp.Payload, nil
}

func (c *WSClient) RequestBinary(reqType string, payload interface{}) ([]byte, error) {
	c.mu.Lock()
	if c.chans == nil {
		c.chans = make(map[string]chan WSResponse)
	}
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	ch := make(chan WSResponse, 1)
	c.chans[id] = ch
	c.mu.Unlock()

	var req WSRequest
	req.Type = reqType
	req.ID = id
	if payload != nil {
		b, _ := json.Marshal(payload)
		req.Payload = b
	}

	b, _ := json.Marshal(req)
	
	c.writeMu.Lock()
	err := c.conn.WriteMessage(websocket.TextMessage, b)
	c.writeMu.Unlock()

	if err != nil {
		c.mu.Lock()
		delete(c.chans, id)
		c.mu.Unlock()
		return nil, err
	}

	resp := <-ch

	c.mu.Lock()
	delete(c.chans, id)
	c.mu.Unlock()

	if resp.Error != "" {
		if strings.Contains(resp.Error, "no such file") || strings.Contains(resp.Error, "file does not exist") || strings.Contains(resp.Error, "entry not found") {
			return nil, fuse.ENOENT
		}
		return nil, fmt.Errorf(resp.Error)
	}

	return resp.BinaryPayload, nil
}

func (c *WSClient) Listen() {
	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("[Client] Read error: %v. Reconnect required.", err)
			break
		}
		var resp WSResponse
		if messageType == websocket.BinaryMessage {
			if len(payload) < 33 {
				continue
			}
			reqID := strings.TrimSpace(string(payload[0:32]))
			status := payload[32]
			resp.ID = reqID
			if status == 0 {
				resp.BinaryPayload = payload[33:]
			} else {
				resp.Error = string(payload[33:])
			}
		} else {
			if err := json.Unmarshal(payload, &resp); err != nil {
				continue
			}
		}
		c.mu.Lock()
		ch, exists := c.chans[resp.ID]
		c.mu.Unlock()
		if exists {
			ch <- resp
		}
	}
}

// =========================================================================
// CHUNK-LESS CLIENT MAIN ENTRY POINT
// =========================================================================

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter backend WebSocket URL [ws://localhost:8080/ws]: ")
	wsURL, _ := reader.ReadString('\n')
	wsURL = strings.TrimSpace(wsURL)
	if wsURL == "" {
		wsURL = "ws://localhost:8080/ws"
	}

	fmt.Print("Enter backend authentication password: ")
	password, _ := reader.ReadString('\n')
	password = strings.TrimSpace(password)

	log.Printf("[Client] Connecting to backend at %s...", wsURL)
	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
		ReadBufferSize:   4 * 1024 * 1024,
		WriteBufferSize:  4 * 1024 * 1024,
	}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatalf("Failed to connect to backend: %v", err)
	}

	wsClient = &WSClient{
		conn:  conn,
		chans: make(map[string]chan WSResponse),
	}
	go wsClient.Listen()

	// Authenticate
	log.Printf("[Client] Authenticating...")
	_, err = wsClient.Request("auth", AuthRequest{Password: password})
	if err != nil {
		log.Fatalf("Authentication failed: %v", err)
	}
	log.Printf("[Client] Authentication successful!")

	// Lightweight local FUSE client config
	mountpoint := "mnt"
	configFile, err := os.ReadFile("config.json")
	if err == nil {
		var localCfg struct {
			MountPoint string `json:"mountPoint"`
		}
		if errJSON := json.Unmarshal(configFile, &localCfg); errJSON == nil && localCfg.MountPoint != "" {
			mountpoint = localCfg.MountPoint
		}
	}

	safeUnmount(mountpoint)

	c, err := fuse.Mount(mountpoint, fuse.FSName("multistorage"), fuse.Subtype("genericfs"))
	if err != nil {
		log.Fatalf("Mount fail: %v", err)
	}

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Signal received. Unmounting safely...")
		safeUnmount(mountpoint)
		os.Exit(0)
	}()

	log.Printf("Mounted successfully at %s. Press Ctrl+C to unmount safely.", mountpoint)

	serveErr := fs.Serve(c, &FS{})
	
	c.Close()
	if serveErr != nil {
		log.Printf("FUSE Serve finished: %v", serveErr)
	} else {
		log.Println("FUSE Serve finished cleanly.")
	}
}
