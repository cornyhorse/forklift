#!/usr/bin/env bash
set -euo pipefail

SA_PASS="${SA_PASS:-YourStrong!Passw0rd}"
MSSQL_DB="${MSSQL_DB:-testdb}"
MSSQL_USER="${MSSQL_USER:-sa}"

# EITHER connect through the host’s forwarded port (Docker Desktop supports host.docker.internal)
MSSQL_HOST="${MSSQL_HOST:-host.docker.internal}"
MSSQL_PORT="${MSSQL_PORT:-1433}"

docker run --rm --platform linux/amd64 \
  -e DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 \
  python:3.11 bash -lc "
    pip -q install mssql-scripter==1.0.0a23 && \
    mssql-scripter -S ${MSSQL_HOST},${MSSQL_PORT} \
                   -d ${MSSQL_DB} \
                   -U ${MSSQL_USER} \
                   -P '${SA_PASS}' \
                   --schema-and-data
  "