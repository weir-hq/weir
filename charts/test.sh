#!/usr/bin/env bash
# Install the chart in a throwaway kind cluster with the server image built from this checkout,
# then run `helm test`. Uses its own kubeconfig, so it never touches your current context.
set -euo pipefail

cd "$(dirname "$0")/.."
CLUSTER=weir-chart-test
IMAGE=weir-server:chart-test
KIND=${KIND:-kind}

KUBECONFIG=$(mktemp)
export KUBECONFIG
cleanup() {
  "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG"
}
trap cleanup EXIT

docker build -q -t "$IMAGE" server
"$KIND" create cluster --name "$CLUSTER" --wait 120s
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER"

helm install weir charts/weir \
  --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE#*:}" --set image.pullPolicy=Never \
  --wait --timeout 3m
helm test weir --logs --timeout 3m
