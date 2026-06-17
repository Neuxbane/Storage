package providers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jlaffaye/ftp"
)

type FTPProvider struct {
	BaseProvider
	host     string
	user     string
	password string
	path     string
}

func NewFTPProvider(name, host, user, password, path string) (*FTPProvider, error) {
	p := &FTPProvider{
		host:     host,
		user:     user,
		password: password,
		path:     path,
	}
	p.Init(name, 5, 500*time.Millisecond)
	return p, nil
}

func NewFTPProviderFromConfig(cfg map[string]string) (*FTPProvider, error) {
	name := cfg["name"]
	host := cfg["host"]
	user := cfg["user"]
	password := cfg["password"]
	path := cfg["path"]
	if name == "" || host == "" || user == "" {
		return nil, fmt.Errorf("missing required ftp config (name, host, user)")
	}
	return NewFTPProvider(name, host, user, password, path)
}

func (p *FTPProvider) Type() string {
	return "ftp"
}

func (p *FTPProvider) MaxChunkSize() int64 {
	return 1024 * 1024 * 1024 // 1GB
}

func (p *FTPProvider) TotalSpace() int64 {
	return -1
}

func (p *FTPProvider) getClient() (*ftp.ServerConn, error) {
	c, err := ftp.Dial(p.host, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}

	err = c.Login(p.user, p.password)
	if err != nil {
		c.Quit()
		return nil, err
	}

	return c, nil
}

func (p *FTPProvider) Upload(ctx context.Context, chunkHash string, data []byte) (map[string]interface{}, error) {
	res, err := p.ProcessUpload(ctx, chunkHash, func() (map[string]interface{}, error) {
		c, err := p.getClient()
		if err != nil {
			return nil, err
		}
		defer c.Quit()

		filePath := fmt.Sprintf("%s/%s", p.path, chunkHash)
		err = c.Stor(filePath, bytes.NewReader(data))
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

func (p *FTPProvider) Download(ctx context.Context, metadata map[string]interface{}) ([]byte, error) {
	filePath, _ := metadata["path"].(string)
	chunkHash, _ := metadata["chunk_hash"].(string)

	return p.ProcessDownload(ctx, chunkHash, func() ([]byte, error) {
		c, err := p.getClient()
		if err != nil {
			return nil, err
		}
		defer c.Quit()

		resp, err := c.Retr(filePath)
		if err != nil {
			return nil, err
		}
		defer resp.Close()

		return io.ReadAll(resp)
	})
}

func (p *FTPProvider) Delete(ctx context.Context, metadata map[string]interface{}) error {
	filePath, _ := metadata["path"].(string)
	c, err := p.getClient()
	if err != nil {
		return err
	}
	defer c.Quit()

	return c.Delete(filePath)
}

func (p *FTPProvider) Check(ctx context.Context, metadata map[string]interface{}) bool {
	filePath, _ := metadata["path"].(string)
	c, err := p.getClient()
	if err != nil {
		return false
	}
	defer c.Quit()

	_, err = c.FileSize(filePath)
	return err == nil
}

func (p *FTPProvider) Close() {}
