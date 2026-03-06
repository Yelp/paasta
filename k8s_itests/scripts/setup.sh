#!/bin/bash
set -euo pipefail

echo "Configuring paasta configuration directory"

rm -rf "${PAASTA_CONFIG_DIR:?}"
mkdir -p "${PAASTA_CONFIG_DIR}"

echo "Applying kubernetes resources"
kubectl apply -Rf "${KUBE_RESOURCE_DIR}" --context "${KIND_CLUSTER}"
