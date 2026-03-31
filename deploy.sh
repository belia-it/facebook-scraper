#!/bin/bash

# Configuration
VPS_USER="houcem"
VPS_IP="192.168.100.45"
REMOTE_DIR="~/facebook-scraper/"

echo "------------------------------------------"
echo "🚀 Deploying updates to $VPS_USER@$VPS_IP"
echo "------------------------------------------"

# Ensure script directory is correct
cd "$(dirname "$0")"

# Check if rsync is available locally
if ! command -v rsync > /dev/null; then
    echo "⚠️ rsync not found locally. Falling back to scp..."
    scp scraper_playwright.py deploy.sh .env facebook_auth.json credentials.json "$VPS_USER@$VPS_IP:$REMOTE_DIR"
else
    # Try rsync, fallback to scp on remote failure
    if ! rsync -avz --exclude={'.venv','venv','__pycache__','.git','.vincent','chunks','wheels','*.xlsx','*.log','storage_state.json'} ./ "$VPS_USER@$VPS_IP:$REMOTE_DIR"; then
        echo "⚠️ rsync failed (maybe missing on VPS). Using scp for main files..."
        scp scraper_playwright.py deploy.sh .env facebook_auth.json credentials.json "$VPS_USER@$VPS_IP:$REMOTE_DIR"
    fi
fi

echo ""
echo "✅ Deployment complete!"
echo "------------------------------------------"
echo "To run the script manually on the VPS:"
echo "ssh $VPS_USER@$VPS_IP 'cd $REMOTE_DIR && source venv/bin/activate && python3 scraper_playwright.py'"
echo "------------------------------------------"
