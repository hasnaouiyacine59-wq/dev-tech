#!/bin/bash
set -e

# Pull latest code if git repo is available
if [ -d ".git" ]; then
  echo "==> Pulling latest code..."
  git pull origin main || echo "git pull failed, continuing with existing code"
fi
echo "==> Tor...!!!!!!!!"
export TOR_HOST=${TOR_HOST:-127.0.0.1}

# Wait for tor-proxy to be ready
echo "==> Waiting for Tor...!!!!!!!!"
until nc -z ${TOR_HOST} "${SOCKS_PORT:-9050}" && nc -z ${TOR_HOST} "${API_PORT:-5000}"; do
  sleep 5
done

# Run main script in a loop
mkdir -p /logs
echo "==> W...!!!!!!!!"
while true; do
  python3 -u thor_main.py -T 2>&1 | tee -a /logs/sessions.log
  sleep 5
done
