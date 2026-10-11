#!/bin/bash
# Start Squid for the "egress-proxy" profile: write the allowed destinations from
# FORKLIFT_EGRESS_PROXY_ALLOW into /run/forklift-egress/allow.conf (squid.conf includes it), check
# the configuration, then run Squid in the foreground.
#
#   FORKLIFT_EGRESS_PROXY_ALLOW="https://s3.eu-west-1.amazonaws.com http://store.internal:9000"
#
# Entries are URLs (a scheme, a host and an optional port; nothing else), separated by spaces or
# commas: https://host[:port] allows CONNECT tunnels to host:port (443 by default),
# http://host[:port] plain HTTP requests to host:port (80 by default). Host names are matched
# exactly (a request for an IP address never matches one); an IP address matches that address.
#
#   start.sh           write the file, check it, run Squid
#   start.sh --check   write the file, check it, print it and exit
set -euo pipefail

ALLOW_FILE=${FORKLIFT_EGRESS_ALLOW_FILE:-/run/forklift-egress/allow.conf}
SQUID_CONF=/etc/squid/squid.conf
HOST_NAME='[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*'
URL="^(https?)://(${HOST_NAME}|\[[0-9A-Fa-f:.]+\])(:([0-9]{1,5}))?/?$"

fail() {
    echo "egress-proxy: $*" >&2
    exit 2
}

render() {
    local entries number=0 entry scheme host port acl
    read -r -a entries <<< "${FORKLIFT_EGRESS_PROXY_ALLOW//,/ }"
    [ "${#entries[@]}" -gt 0 ] || fail "FORKLIFT_EGRESS_PROXY_ALLOW is empty: set it (or" \
        "FORKLIFT_STORE_URL) to the object store's URL, for example https://s3.example.org"
    echo "# Written by start.sh from FORKLIFT_EGRESS_PROXY_ALLOW; do not edit."
    for entry in "${entries[@]}"; do
        [[ "$entry" =~ $URL ]] || fail "'$entry' in FORKLIFT_EGRESS_PROXY_ALLOW is not an" \
            "http:// or https:// URL with a host and nothing after it but an optional port" \
            "(for example https://s3.example.org or http://store.internal:9000)"
        scheme=${BASH_REMATCH[1]}
        host=${BASH_REMATCH[2]}
        port=${BASH_REMATCH[7]}
        if [ -z "$port" ]; then
            port=$([ "$scheme" = https ] && echo 443 || echo 80)
        fi
        [ "$((10#$port))" -ge 1 ] && [ "$((10#$port))" -le 65535 ] \
            || fail "'$entry' in FORKLIFT_EGRESS_PROXY_ALLOW has an invalid port"
        port=$((10#$port))
        number=$((number + 1))
        acl="forklift_store_${number}"
        echo
        if [[ "$host" =~ ^[0-9.]+$ || "$host" == \[* ]]; then
            host=${host#[}
            echo "acl ${acl} dst ${host%]}"
        else
            # -n: a URL with an IP address is not matched by its reverse DNS name, which
            # whoever controls that address's PTR record could choose
            echo "acl ${acl} dstdomain -n ${host,,}"
        fi
        echo "acl ${acl}_port port ${port}"
        if [ "$scheme" = https ]; then
            echo "http_access allow CONNECT ${acl} ${acl}_port"
        else
            echo "http_access allow !CONNECT plain_http ${acl} ${acl}_port"
        fi
    done
}

mkdir -p "$(dirname "$ALLOW_FILE")"
render > "$ALLOW_FILE.new"
mv "$ALLOW_FILE.new" "$ALLOW_FILE"
squid -k parse -f "$SQUID_CONF" > /dev/null 2>&1 || {
    squid -k parse -f "$SQUID_CONF" >&2 || true
    fail "Squid refuses the configuration (above)"
}
if [ "${1:-}" = --check ]; then
    cat "$ALLOW_FILE"
    exit 0
fi
echo "egress-proxy: allowing $(grep -c '^http_access' "$ALLOW_FILE") destination(s):" \
    "${FORKLIFT_EGRESS_PROXY_ALLOW}" >&2
exec squid -f "$SQUID_CONF" -NYC
