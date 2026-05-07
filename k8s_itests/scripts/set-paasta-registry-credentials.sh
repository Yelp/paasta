#!/bin/bash
set -e

CLUSTER=$1
REGISTRY="docker-paasta.yelpcorp.com:443"

echo "Generating registry credentials..."
CREDS=$(echo "$REGISTRY" | docker-credential-yelp-okta get)
USERNAME=$(echo "$CREDS" | jq -r '.Username')
SECRET=$(echo "$CREDS" | jq -r '.Secret')

if [ -z "$SECRET" ]; then
  echo "ERROR: Failed to generate registry credentials" >&2
  exit 1
fi

AUTH_TOKEN=$(echo -n "${USERNAME}:${SECRET}" | base64 -w 0)

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
