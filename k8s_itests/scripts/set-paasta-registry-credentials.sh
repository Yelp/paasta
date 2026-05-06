#!/bin/bash
set -e

CLUSTER=$1
REGISTRY="docker-paasta.yelpcorp.com:443"

echo "Generating registry credentials..."
AUTH_TOKEN=$(../.tox/py310-linux/bin/python ./scripts/containerd_registry_setup.py)

if [ -z "$AUTH_TOKEN" ]; then
  echo "ERROR: Failed to generate registry credentials" >&2
  exit 1
fi

for node in $(./kind get nodes --name "${CLUSTER}"); do
  echo "Setting up registry credentials on kind node: $node ..."
  podman exec "${node}" sh -c "
    mkdir -p /etc/containerd/certs.d/${REGISTRY} && \
    printf '[host.\"https://${REGISTRY}\"]\n  capabilities = [\"pull\", \"resolve\"]\n  [host.\"https://${REGISTRY}\".header]\n    Authorization = [\"Basic ${AUTH_TOKEN}\"]\n' \
      > /etc/containerd/certs.d/${REGISTRY}/hosts.toml
  "
  podman exec "${node}" systemctl restart containerd.service
  podman exec "${node}" systemctl restart kubelet.service
done
