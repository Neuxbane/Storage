#!/bin/bash
set -e

# Change to the script's directory
cd "$(dirname "$0")"

echo "[*] Building MultiStorage Client Daemon..."
go build -o multistorage-client .
echo "[*] MultiStorage Client Daemon built successfully (multistorage-client)."
