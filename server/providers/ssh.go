package providers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SSHProvider struct {
	BaseProvider
	host     string
	user     string
	password string
	path     string
}

func NewSSHProvider(name, host, user, password, path string) (*SSHProvider, error) {
	p := &SSHProvider{
		host:     host,
		user:     user,
		password: password,
		path:     path,
	}
	p.Init(name, 5, 500*time.Millisecond)
	return p, nil
}

func NewSSHProviderFromConfig(cfg map[string]string) (*SSHProvider, error) {
	name := cfg["name"]
	host := cfg["host"]
	user := cfg["user"]
	password := cfg["password"]
	path := cfg["path"]
	if name == "" || host == "" || user == "" {
		return nil, fmt.Errorf("missing required ssh config (name, host, user)")
	}
	return NewSSHProvider(name, host, user, password, path)
}

func (p *SSHProvider) Type() string {
	return "ssh"
}

func (p *SSHProvider) MaxChunkSize() int64 {
	return 1024 * 1024 * 1024 // 1GB
}

func (p *SSHProvider) TotalSpace() int64 {
	return -1
}

func (p *SSHProvider) getClient() (*ssh.Client, *sftp.Client, error) {
	config := &ssh.ClientConfig{
		User: p.user,
		Auth: []ssh.AuthMethod{
			ssh.Password(p.password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	conn, err := ssh.Dial("tcp", p.host, config)
	if err != nil {
		return nil, nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, client, nil
}

func (p *SSHProvider) Upload(ctx context.Context, chunkHash string, data []byte) (map[string]interface{}, error) {
	res, err := p.ProcessUpload(ctx, chunkHash, func() (map[string]interface{}, error) {
		conn, client, err := p.getClient()
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		defer client.Close()

		filePath := fmt.Sprintf("%s/%s", p.path, chunkHash)
		f, err := client.Create(filePath)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		_, err = io.Copy(f, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"provider":   p.Name(),
			"path":       filePath,
			"chunk_hash": chunkHash,
		}, nil
	})

	if err != nil {
		return nil, err
	}
	return res, nil
}

func (p *SSHProvider) Download(ctx context.Context, metadata map[string]interface{}) ([]byte, error) {
	filePath, _ := metadata["path"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)

	return p.ProcessDownload(ctx, chunkHash, func() ([]byte, error) {
		conn, client, err := p.getClient()
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		defer client.Close()

		f, err := client.Open(filePath)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		return io.ReadAll(f)
	})
}

func (p *SSHProvider) Delete(ctx context.Context, metadata map[string]interface{}) error {
	filePath, _ := metadata["path"].(string)
	conn, client, err := p.getClient()
	if err != nil {
		return err
	}
	defer conn.Close()
	defer client.Close()

	return client.Remove(filePath)
}

func (p *SSHProvider) Check(ctx context.Context, metadata map[string]interface{}) bool {
	filePath, _ := metadata["path"].(string)
	conn, client, err := p.getClient()
	if err != nil {
		return false
	}
	defer conn.Close()
	defer client.Close()

	_, err = client.Stat(filePath)
	return err == nil
}

func (p *SSHProvider) Close() {}
