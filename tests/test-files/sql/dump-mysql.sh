#!/usr/bin/env bash
set -euo pipefail

MY_DB="${MY_DB:-testdb}"
MY_USER="${MY_USER:-root}"        # root has full access per your compose
MY_PASS="${MY_PASS:-root}"

docker compose exec -T mysql sh -lc \
  "mysqldump --databases '$MY_DB' \
             --user='$MY_USER' --password='$MY_PASS' \
             --single-transaction --routines --events --triggers --skip-lock-tables"