#!/usr/bin/env bash
# A real install of the chart on a throw-away k3s cluster in Docker, used from outside:
#
#   deploy/helm/tests/k3s_smoke.sh            # from the repository root
#
# 1. starts rancher/k3s (pinned) in Docker; its embedded network-policy controller enforces
#    NetworkPolicies, and the release's namespace enforces Pod Security "restricted";
# 2. imports the forklift-web and forklift-worker images ($FORKLIFT_WEB_IMAGE,
#    $FORKLIFT_WORKER_IMAGE; default forklift-web:local and forklift-worker:local, built from this
#    checkout) and puts the chart's no-network seccomp profile on the node;
# 3. starts a test-only PostgreSQL (k3s/postgres.yaml) and installs the chart with the bundled
#    RustFS (k3s/values.yaml): the bootstrap hooks, then every rollout;
# 4. runs k3s_e2e.py: a CSV job end to end through the public API (port-forwards on
#    127.0.0.1:18400 and 18401), a schema generated on the no-network worker group, and the
#    NetworkPolicies biting (workers reach neither PostgreSQL nor the gateway's public port; a pod
#    outside the chart cannot reach the internal port);
# 5. upgrades the release to the proxy mode (k3s/proxy-values.yaml) with changed settings, so
#    that every pod rolls (the hooks again: idempotent; the secrets kept), and runs the jobs
#    again, the workers' store requests now through Squid, which refuses anything else;
# 6. deletes the cluster (FORKLIFT_K3S_KEEP=1 keeps it for a look around).
#
# Needs Docker (privileged containers) and Python 3; helm runs from its pinned image unless
# FORKLIFT_HELM names a command. FORKLIFT_K3S_CA_FILE: a CA bundle for k3s's image pulls behind a
# TLS-inspecting proxy.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../.." && pwd)
TESTS="$REPO/deploy/helm/tests"
CLUSTER=${FORKLIFT_K3S_NAME:-forklift-helm-smoke}
K3S_IMAGE=${FORKLIFT_K3S_IMAGE:-rancher/k3s:v1.36.4-k3s1@sha256:edad48e12bf81c3a09ac1c05c0c0ffaaa22145980b989d6fae84543a76b83657}
HELM_IMAGE=alpine/helm:4.3.0@sha256:a6cf54599ccb99d90cf0712b30f03fdb3cab062e6b94e0418cc4db7e8a1464b2
API_PORT=${FORKLIFT_K3S_API_PORT:-6440}
GATEWAY_PORT=${FORKLIFT_K3S_GATEWAY_PORT:-18400}
STORE_PORT=${FORKLIFT_K3S_STORE_PORT:-18401}
WEB_IMAGE=${FORKLIFT_WEB_IMAGE:-forklift-web:local}
WORKER_IMAGE=${FORKLIFT_WORKER_IMAGE:-forklift-worker:local}
NAMESPACE=forklift
RELEASE=forklift
WORK=$(mktemp -d "${TMPDIR:-/tmp}/forklift-k3s.XXXXXX")

log() { printf '\n== %s\n' "$*"; }
kubectl() { docker exec -i "$CLUSTER" kubectl "$@"; }
helm() {
    if [ -n "${FORKLIFT_HELM:-}" ]; then
        # shellcheck disable=SC2086
        KUBECONFIG="$WORK/kubeconfig" $FORKLIFT_HELM "$@"
    else
        docker run --rm -i --network host -u "$(id -u):$(id -g)" -v "$REPO:$REPO:ro" \
            -v "$WORK:$WORK" -e KUBECONFIG="$WORK/kubeconfig" -e HOME="$WORK" "$HELM_IMAGE" "$@"
    fi
}

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        log "failed: the cluster's state"
        kubectl get pods -A -o wide || true
        kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -40 || true
        for pod in $(kubectl -n "$NAMESPACE" get pods -o name 2>/dev/null); do
            echo "--- $pod"
            kubectl -n "$NAMESPACE" logs "$pod" --all-containers --tail=40 || true
        done
    fi
    if [ -z "${FORKLIFT_K3S_KEEP:-}" ]; then
        docker rm -f -v "$CLUSTER" >/dev/null 2>&1 || true
    else
        echo "kept the cluster: docker exec -it $CLUSTER kubectl get pods -A"
    fi
    rm -rf "$WORK"
    exit "$status"
}
trap cleanup EXIT

for image in "$WEB_IMAGE" "$WORKER_IMAGE"; do
    docker image inspect "$image" >/dev/null || {
        echo "Build $image first (docker build ... from the repository root; see docs/platform/helm.md)" >&2
        exit 1
    }
done

log "k3s ($K3S_IMAGE) as $CLUSTER, API on 127.0.0.1:$API_PORT"
args=(server --disable=traefik --disable=metrics-server --disable=servicelb --tls-san=127.0.0.1
      # absolute eviction thresholds: a shared CI disk is often more than 90% full
      "--kubelet-arg=eviction-hard=nodefs.available<1Gi,imagefs.available<1Gi"
      --kubelet-arg=image-gc-high-threshold=100 --kubelet-arg=image-gc-low-threshold=99)
if [ "$(docker info --format '{{.CgroupVersion}}')" = "1" ]; then
    args+=(--kubelet-arg=fail-cgroupv1=false)   # Kubernetes 1.35+ refuses cgroup v1 otherwise
fi
mounts=(-v "$TESTS/k3s/containerd-oom.toml:/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/oom.toml:ro")
if [ -n "${FORKLIFT_K3S_CA_FILE:-}" ]; then
    mounts+=(-v "$FORKLIFT_K3S_CA_FILE:/etc/forklift-extra-ca.crt:ro" -e SSL_CERT_FILE=/etc/forklift-extra-ca.crt)
fi
docker rm -f -v "$CLUSTER" >/dev/null 2>&1 || true
docker run -d --name "$CLUSTER" --privileged --tmpfs /run --tmpfs /var/run \
    -p "127.0.0.1:$API_PORT:6443" -p "127.0.0.1:$GATEWAY_PORT:$GATEWAY_PORT" \
    -p "127.0.0.1:$STORE_PORT:$STORE_PORT" "${mounts[@]}" "$K3S_IMAGE" "${args[@]}" >/dev/null

for _ in $(seq 1 90); do
    kubectl get nodes 2>/dev/null | grep -q ' Ready' && break
    sleep 2
done
kubectl wait --for=condition=Ready node --all --timeout=120s
# The chart's no-network seccomp profile on the node (/var/lib/kubelet is a volume of the k3s
# image, so it is copied in rather than bind-mounted)
docker exec "$CLUSTER" mkdir -p /var/lib/kubelet/seccomp/forklift
docker exec -i "$CLUSTER" sh -c 'cat > /var/lib/kubelet/seccomp/forklift/worker-no-network.json' \
    < "$REPO/deploy/helm/forklift/files/seccomp/worker-no-network.json"
docker exec "$CLUSTER" cat /etc/rancher/k3s/k3s.yaml \
    | sed "s#https://127.0.0.1:6443#https://127.0.0.1:$API_PORT#" > "$WORK/kubeconfig"

log "images: $WEB_IMAGE, $WORKER_IMAGE (and the third-party ones the host has)"
images=("$WEB_IMAGE" "$WORKER_IMAGE")
for image in docker.io/library/postgres:16 docker.io/rustfs/rustfs:1.0.1 docker.io/ubuntu/squid:6.6-24.04_edge; do
    docker image inspect "$image" >/dev/null 2>&1 && images+=("$image")
done
docker save "${images[@]}" | docker exec -i "$CLUSTER" ctr -n k8s.io images import - >/dev/null

log "a test-only PostgreSQL"
DB_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')
kubectl apply -f - < "$TESTS/k3s/postgres.yaml" >/dev/null
kubectl -n forklift-test-db create secret generic postgres --from-literal=password="$DB_PASSWORD" >/dev/null
kubectl -n forklift-test-db rollout status deployment/postgres --timeout=300s

log "the release's namespace (Pod Security: restricted, enforced) and its database Secret"
kubectl create namespace "$NAMESPACE" >/dev/null
kubectl label namespace "$NAMESPACE" pod-security.kubernetes.io/enforce=restricted \
    pod-security.kubernetes.io/enforce-version=latest >/dev/null
kubectl -n "$NAMESPACE" create secret generic forklift-db --from-literal=password="$DB_PASSWORD" >/dev/null

image_args=(--set "image.web.name=${WEB_IMAGE%:*}" --set "image.web.tag=${WEB_IMAGE##*:}"
            --set "image.worker.name=${WORKER_IMAGE%:*}" --set "image.worker.tag=${WORKER_IMAGE##*:}")
install() {
    action=$1
    shift
    helm "$action" "$RELEASE" "$REPO/deploy/helm/forklift" --namespace "$NAMESPACE" \
        --values "$TESTS/k3s/values.yaml" "${image_args[@]}" "$@" --wait --timeout 10m
}

log "helm install (hooks: bootstrap before the gateway, store-setup after RustFS)"
install install
kubectl -n "$NAMESPACE" get pods -o wide
kubectl -n "$NAMESPACE" logs "job/$RELEASE-bootstrap" --all-containers

# kubectl port-forward in the k3s container, on ports Docker publishes to 127.0.0.1 (a forward
# ends with its pod, so they are started again after the upgrade)
port_forwards() {
    docker exec "$CLUSTER" sh -c 'for f in /tmp/forward-*.pid; do [ -f "$f" ] && kill "$(cat "$f")"; rm -f "$f"; done; true'
    docker exec -d "$CLUSTER" sh -c "echo \$\$ > /tmp/forward-gateway.pid; exec kubectl -n $NAMESPACE port-forward --address 0.0.0.0 svc/$RELEASE-gateway $GATEWAY_PORT:8080"
    docker exec -d "$CLUSTER" sh -c "echo \$\$ > /tmp/forward-store.pid; exec kubectl -n $NAMESPACE port-forward --address 0.0.0.0 svc/$RELEASE-rustfs $STORE_PORT:9000"
    for _ in $(seq 1 30); do
        curl -fsS "http://127.0.0.1:$GATEWAY_PORT/readyz" >/dev/null 2>&1 && return 0
        sleep 1
    done
    echo "the gateway's port-forward does not answer" >&2
    return 1
}
e2e() {
    ADMIN_PASSWORD=$(kubectl -n "$NAMESPACE" get secret "$RELEASE-admin" -o jsonpath='{.data.password}' | base64 -d)
    FORKLIFT_K3S_ADMIN_PASSWORD="$ADMIN_PASSWORD" python3 "$TESTS/k3s_e2e.py" "$@"
}
export FORKLIFT_K3S_NAME="$CLUSTER" FORKLIFT_K3S_NAMESPACE="$NAMESPACE" FORKLIFT_K3S_RELEASE="$RELEASE" \
    FORKLIFT_K3S_GATEWAY_URL="http://127.0.0.1:$GATEWAY_PORT" FORKLIFT_K3S_WORKER_IMAGE="$WORKER_IMAGE"

log "end to end through the public API, and the NetworkPolicies"
port_forwards
e2e all

log "helm upgrade to proxy mode, with other log levels (every pod rolls): the hooks again (idempotent), the secrets kept"
token_before=$(kubectl -n "$NAMESPACE" get secret "$RELEASE-worker-token" -o jsonpath='{.data.token}')
keys_before=$(kubectl -n "$NAMESPACE" get secret "$RELEASE-secrets" -o jsonpath='{.data.secrets-keys}')
install upgrade --values "$TESTS/k3s/proxy-values.yaml" --set gateway.logLevel=WARNING \
    --set workerDefaults.logLevel=debug
kubectl -n "$NAMESPACE" logs "job/$RELEASE-bootstrap" -c worker-token | tee "$WORK/token.log"
grep -q "is already registered (helm); nothing to do." "$WORK/token.log"
[ "$token_before" = "$(kubectl -n "$NAMESPACE" get secret "$RELEASE-worker-token" -o jsonpath='{.data.token}')" ]
[ "$keys_before" = "$(kubectl -n "$NAMESPACE" get secret "$RELEASE-secrets" -o jsonpath='{.data.secrets-keys}')" ]
for _ in $(seq 1 60); do   # the old pods gone (the workers drain first), so forwards pick new ones
    kubectl -n "$NAMESPACE" get pods -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"\n"}{end}' \
        | grep -q . || break
    sleep 2
done
kubectl -n "$NAMESPACE" get pods
port_forwards
e2e proxy

log "passed"
