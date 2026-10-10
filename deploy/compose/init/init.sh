#!/bin/sh
# One-shot setup for the Compose stack (the "init" service); every step is safe to repeat.
set -eu

echo "init: database migrations"
forklift-web migrate --no-input

echo "init: the first admin (${FORKLIFT_ADMIN_USERNAME})"
forklift-web bootstrap_admin --username "${FORKLIFT_ADMIN_USERNAME}"

echo "init: the bucket"
python /init/init_store.py

# Browsers upload and download straight to the store: one CORS rule per UI origin
echo "init: the bucket's CORS rule"
set --
for origin in $(echo "${FORKLIFT_CORS_ORIGINS}" | tr ',' ' '); do
    set -- "$@" --origin "$origin"
done
forklift-web configure_cors "$@"

# The workers' token: created once, kept in a volume only the worker mounts (read-only)
if [ ! -s /tokens/worker-token ]; then
    echo "init: a worker token"
    umask 077
    forklift-web create_worker_token --name compose > /tokens/worker-token.new
    mv /tokens/worker-token.new /tokens/worker-token
fi
chown "${FORKLIFT_WORKER_UID}:${FORKLIFT_WORKER_UID}" /tokens/worker-token
chmod 0400 /tokens/worker-token
echo "init: done"
