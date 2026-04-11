#!/bin/bash
set -e

# Pull latest code if git repo is available
if [ -d ".git" ]; then
  echo "==> Pulling latest code..."
  git pull origin main || echo "git pull failed, continuing with existing code"
fi

# Wait for tor-proxy to be ready
echo "==> Waiting for Tor..."
until nc -z 127.0.0.1 "${SOCKS_PORT:-9050}" && nc -z 127.0.0.1 "${API_PORT:-5000}"; do
  sleep 5
done

# Run main script in a loop
mkdir -p /logs
while true; do
  pwd && ls
  sleep 5
  python3 -u thor_main.py -T >> /logs/sessions.log 2>&1
  sleep 5
done
