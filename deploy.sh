#!/bin/bash

# Configuration
VPS_USER="houcem"
VPS_IP="192.168.100.45"
REMOTE_DIR="~/facebook-scraper/"

echo "------------------------------------------"
echo "🚀 Deploying updates to $VPS_USER@$VPS_IP"
echo "------------------------------------------"

cd "$(dirname "$0")"

if ! command -v rsync > /dev/null; then
    echo "⚠️  rsync not found locally. Falling back to scp..."
    scp scraper_playwright.py scraper_db.py deploy.sh .env facebook_auth.json credentials.json "$VPS_USER@$VPS_IP:$REMOTE_DIR"
    scp -r api "$VPS_USER@$VPS_IP:$REMOTE_DIR"
else
    rsync -avz \
        --exclude='.venv' --exclude='venv' --exclude='__pycache__' \
        --exclude='.git' --exclude='chunks' --exclude='wheels' \
        --exclude='*.xlsx' --exclude='*.log' --exclude='storage_state.json' \
        --exclude='node_modules' --exclude='.next' \
        --exclude='webapp' \
        --exclude='api/posts.db' \
        ./ "$VPS_USER@$VPS_IP:$REMOTE_DIR" \
    || {
        echo "⚠️  rsync failed. Falling back to scp..."
        scp scraper_playwright.py scraper_db.py deploy.sh .env facebook_auth.json credentials.json "$VPS_USER@$VPS_IP:$REMOTE_DIR"
        scp -r api "$VPS_USER@$VPS_IP:$REMOTE_DIR"
    }
fi

echo ""
echo "✅ Deployment complete!"
echo "------------------------------------------"
echo "GOOGLE SHEETS SCRAPER (unchanged):"
echo "  ssh $VPS_USER@$VPS_IP 'cd $REMOTE_DIR && source venv/bin/activate && python3 scraper_playwright.py'"
echo ""
echo "SQLITE SCRAPER (independent):"
echo "  ssh $VPS_USER@$VPS_IP 'cd $REMOTE_DIR && source venv/bin/activate && python3 scraper_db.py'"
echo ""
echo "FASTAPI DASHBOARD:"
echo "  ssh $VPS_USER@$VPS_IP 'cd $REMOTE_DIR && source venv/bin/activate && pip install fastapi \"uvicorn[standard]\" jinja2 && uvicorn api.main:app --host 0.0.0.0 --port 8000'"
echo "------------------------------------------"
