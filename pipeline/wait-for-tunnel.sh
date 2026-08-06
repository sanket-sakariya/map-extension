#!/bin/bash
# wait-for-tunnel.sh — runs after docker-compose up
# Watches cloudflared logs for the tunnel URL and registers it with the backend

echo "⏳ Waiting for cloudflare tunnel to start..."

MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
  TUNNEL_URL=$(docker logs pipeline-tunnel-1 2>&1 | grep -oE 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' | head -1)
  
  if [ -n "$TUNNEL_URL" ]; then
    echo "✅ Tunnel URL found: $TUNNEL_URL"
    
    # Register it with the backend
    RESP=$(curl -s -X POST http://localhost:8000/api/config/tunnel \
      -H "Content-Type: application/json" \
      -d "{\"url\": \"$TUNNEL_URL\"}")
    
    echo "   Registered: $RESP"
    echo ""
    echo "╔══════════════════════════════════════════════════════╗"
    echo "║  🗺️  Pipeline is PUBLIC at:                          ║"
    echo "║  $TUNNEL_URL"
    echo "║                                                      ║"
    echo "║  Local: http://localhost:3000                         ║"
    echo "╚══════════════════════════════════════════════════════╝"
    exit 0
  fi
  
  ATTEMPT=$((ATTEMPT + 1))
  sleep 2
done

echo "❌ Failed to detect tunnel URL after ${MAX_ATTEMPTS} attempts"
echo "   Check: docker logs pipeline-tunnel-1"
exit 1
