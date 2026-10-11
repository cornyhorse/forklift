#!/usr/bin/env bash
# Lints the chart and validates what it renders, for every values file in ci/:
#
#   deploy/helm/tests/lint.sh          # from anywhere; then: python -m pytest deploy/helm/tests
#
# - helm lint --strict (with the defaults, and with each ci/*-values.yaml);
# - helm template with each of them;
# - kubeconform -strict on the result, against the Kubernetes versions the chart supports (the
#   oldest, 1.30, and the one it is tested on, 1.36), with the schemas pinned to a commit. KEDA's
#   ScaledObject and TriggerAuthentication are checked against the CRD catalog's schemas
#   (pinned too), so no kind is skipped.
#
# helm and kubeconform come from $FORKLIFT_HELM / $FORKLIFT_KUBECONFORM (command lines), else
# from PATH, else from their pinned images through Docker.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../.." && pwd)
CHART="$REPO/deploy/helm/forklift"
HELM_IMAGE=alpine/helm:4.3.0@sha256:a6cf54599ccb99d90cf0712b30f03fdb3cab062e6b94e0418cc4db7e8a1464b2
KUBECONFORM_IMAGE=ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e
KUBERNETES_VERSIONS=(1.30.0 1.36.0)
SCHEMAS='https://raw.githubusercontent.com/yannh/kubernetes-json-schema/5f1fa4f7908afc9b6641c62d282a4a22437626e1/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json'
CRD_SCHEMAS='https://raw.githubusercontent.com/datreeio/CRDs-catalog/63669a570e231d4f1f8396d229a1de512bcf0a34/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
OUT=$(mktemp -d "${TMPDIR:-/tmp}/forklift-chart.XXXXXX")
trap 'rm -rf "$OUT"' EXIT

helm() {
    if [ -n "${FORKLIFT_HELM:-}" ]; then
        # shellcheck disable=SC2086
        $FORKLIFT_HELM "$@"
    elif command -v helm >/dev/null; then
        command helm "$@"
    else
        docker run --rm -i -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$REPO:$REPO:ro" -v "$OUT:$OUT" \
            "$HELM_IMAGE" "$@"
    fi
}
kubeconform() {
    if [ -n "${FORKLIFT_KUBECONFORM:-}" ]; then
        # shellcheck disable=SC2086
        $FORKLIFT_KUBECONFORM "$@"
    elif command -v kubeconform >/dev/null; then
        command kubeconform "$@"
    else
        docker run --rm -i -v "$OUT:$OUT" "$KUBECONFORM_IMAGE" "$@"
    fi
}

echo "== helm lint --strict (defaults)"
helm lint --strict "$CHART"
for values in "$CHART"/ci/*-values.yaml; do
    name=$(basename "$values" -values.yaml)
    echo "== $name: helm lint --strict, helm template"
    helm lint --strict "$CHART" --namespace forklift --values "$values" >/dev/null
    helm template fk "$CHART" --namespace forklift --values "$values" > "$OUT/$name.yaml"
done
for version in "${KUBERNETES_VERSIONS[@]}"; do
    echo "== kubeconform -strict, Kubernetes $version"
    mkdir -p "$OUT/cache-$version"
    kubeconform -strict -summary -kubernetes-version "$version" -cache "$OUT/cache-$version" \
        -schema-location "$SCHEMAS" -schema-location "$CRD_SCHEMAS" "$OUT"/*.yaml
done
