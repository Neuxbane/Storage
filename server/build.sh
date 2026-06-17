#!/bin/bash
set -e

# Change to the script's directory
cd "$(dirname "$0")"

echo "[*] Building MultiStorage Backend Server..."
go build -o multistorage-server .
echo "[*] MultiStorage Backend Server built successfully (multistorage-server)."
