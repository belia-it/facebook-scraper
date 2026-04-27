#!/bin/sh
# Copy fresh app code from image backup into the volume-mounted /app/api,
# preserving only the database files so deployments always pick up new code.
if [ -d /app/api.image ]; then
  echo "[entrypoint] Syncing fresh code into /app/api ..."
  find /app/api.image -type f | while read src; do
    rel="${src#/app/api.image/}"
    # skip DB files — those live in the volume and must persist
    case "$rel" in
      *.db|*.sqlite|*.json) continue ;;
    esac
    dst="/app/api/$rel"
    mkdir -p "$(dirname "$dst")"
    cp -f "$src" "$dst"
  done
  echo "[entrypoint] Sync done."
fi
exec uvicorn api.main:app --host 0.0.0.0 --port 8000
