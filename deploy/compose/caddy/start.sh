#!/bin/sh
# Start Caddy for the "caddy" profile: check the settings, then run it with the Caddyfile next
# to this script (mounted at /etc/caddy/Caddyfile).
set -eu

fail() {
    echo "caddy: $*" >&2
    exit 2
}

[ -n "${FORKLIFT_DOMAIN:-}" ] ||
    fail "set FORKLIFT_DOMAIN in .env: the host name of the UI and API (generate_env.py --domain)"
[ "${FORKLIFT_DOMAIN}" != "${FORKLIFT_STORE_DOMAIN:-}" ] ||
    fail "FORKLIFT_DOMAIN and FORKLIFT_STORE_DOMAIN must differ (the store needs a host of its own)"
case "${FORKLIFT_TLS:-acme}" in
    acme)
        [ -n "${FORKLIFT_ACME_EMAIL:-}" ] ||
            fail "FORKLIFT_TLS=acme needs FORKLIFT_ACME_EMAIL, the email of the ACME account" \
                "(or FORKLIFT_TLS=internal for Caddy's own CA)" ;;
    internal) ;;
    *) fail "FORKLIFT_TLS is '${FORKLIFT_TLS}': use acme (Let's Encrypt or ZeroSSL) or internal" \
            "(Caddy's own CA)" ;;
esac
case "${FORKLIFT_HSTS_SECONDS:-31536000}" in
    '' | *[!0-9]*) fail "FORKLIFT_HSTS_SECONDS is '${FORKLIFT_HSTS_SECONDS}': a number of seconds" ;;
esac

# The store's site only when the store is behind Caddy: an external store (external-store.yml)
# with an https:// URL of its own needs none.
if [ -n "${FORKLIFT_STORE_DOMAIN:-}" ]; then
    export FORKLIFT_CADDY_STORE_SITE=store-site
else
    export FORKLIFT_CADDY_STORE_SITE=no-store-site
fi
exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
