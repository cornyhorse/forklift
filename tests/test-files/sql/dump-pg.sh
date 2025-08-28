#!/usr/bin/env bash
set -euo pipefail

# Matches your compose env
PG_DB="${PG_DB:-testdb}"
PG_USER="${PG_USER:-testuser}"
PG_PASS="${PG_PASS:-testpass}"

# Use the compose service name directly (-T = no pseudo-TTY)
docker compose exec -T -e PGPASSWORD="$PG_PASS" postgres \
  pg_dump --no-owner --no-privileges --format=plain \
          --inserts --column-inserts \
          -U "$PG_USER" "$PG_DB"