# MultiStorage

MultiStorage is a highly modular distributed storage system that treats any service as a potential storage layer. Its architecture is built around dynamic providers, allowing it to transform messaging platforms, web services, and remote servers into a unified, redundant, and reliable storage backend.

By splitting files into chunks and replicating them across an adaptable ecosystem of providers—ranging from Discord and Telegram to SSH and FTP—MultiStorage ensures your data remains available and resilient. The entire system is presented through a FUSE interface, letting you interact with this distributed network as a standard local filesystem.

## How it Works

MultiStorage operates on a Client-Server architecture:

- **Server**: Orchestrates storage providers, manages replication, maintains a metadata catalog in SQLite, and can optionally mount the storage locally.
- **Client**: Connects to the server via WebSockets and mounts the distributed storage as a local filesystem using FUSE, allowing you to interact with your cloud-backed files as if they were on your hard drive.

## Key Features

- **Multi-Cloud Backends**: Use Discord channels, Telegram bots, or Filebin as storage nodes.
- **Automatic Replication**: Configure the number of replicas for each file chunk to ensure high availability.
- **FUSE Integration**: Mount your storage directly into your file system on Linux/macOS.
- **Health Monitoring**: Background processes monitor the availability of chunks and re-replicate them if they become unreachable.
- **WebSocket Protocol**: Efficient communication between clients and the storage server.
- **Rate-Limit Awareness**: Intelligent providers that handle service-specific rate limits and backoffs.

## Prerequisites

- **Go**: Version 1.22 or higher.
- **FUSE**: You must have `fuse` (or `fuse3`) installed on your system.
  - **Linux**: `sudo apt install fuse3` (or equivalent for your distro).
  - **macOS**: Install [macFUSE](https://osxfuse.github.io/).

## Getting Started

### 1. Build the Project

Use the provided build scripts for each component:

```bash
# Build the server
cd server
./build.sh

# Build the client
cd ../client
./build.sh
```

### 2. Configure the Server

The server requires a `config.json` file. Here is an example showing how to configure different providers:

```json
{
  "mountPoint": "mnt",
  "replication": 2,
  "cacheSize": "100MB",
  "providers": [
    {
      "filebin": { "name": "FileBin-1" }
    },
    {
      "discord": {
        "name": "Discord-Storage",
        "token": "YOUR_BOT_TOKEN",
        "channel_id": "YOUR_CHANNEL_ID"
      }
    },
    {
      "telegram": {
        "name": "Telegram-Storage",
        "token": "YOUR_BOT_TOKEN",
        "chat_id": "YOUR_CHAT_ID"
      }
    },
    {
      "ssh": {
        "name": "SSH-Server",
        "host": "example.com:22",
        "user": "username",
        "password": "password",
        "path": "/path/to/storage"
      }
    },
    {
      "ftp": {
        "name": "FTP-Server",
        "host": "ftp.example.com:21",
        "user": "username",
        "password": "password",
        "path": "/path/to/storage"
      }
    }
  ]
}
```

- **Replication**: The number of copies created for each file chunk.
- **CacheSize**: Local cache size for chunk data.
- **Providers**: Array of storage backend configurations.

### 3. Run the Server

```bash
cd server
./multistorage-server -port 8080 -pass your-secret-password
```

Optional flags:
- `-mount <path>`: Mount the storage locally on the server as well.

### 4. Run the Client

Connect your client to the running server:

```bash
cd client
./multistorage-client -addr localhost:8080 -pass your-secret-password -mount ./my-storage
```

Now you can access your files at `./my-storage`!

## Supported Providers

- **Discord**: Uses channel messages to store binary chunks. Requires a Bot Token and Channel ID.
- **Telegram**: Uses bot API to store chunks as documents. Requires a Bot Token and Chat ID.
- **Filebin**: Anonymized file hosting for temporary or test storage.
- **SSH (SFTP)**: Securely stores chunks on a remote server via SFTP.
- **FTP**: Stores chunks on a remote server via traditional FTP.

## Project Structure

- `server/`: Backend logic, provider implementations, database management, and FUSE server.
- `client/`: FUSE client that talks to the server over WebSockets.
- `protocol.go`: Shared communication protocols.

## Contributing

We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get started.

## License

[MIT License](LICENSE)
