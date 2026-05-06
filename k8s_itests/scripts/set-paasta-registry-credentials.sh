#!/bin/bash
set -e

CLUSTER=$1

echo "Generating registry credentials..."
AUTH_TOKEN=$(../.tox/py310-linux/bin/python -c "
import sys; sys.path.insert(0, 'scripts')
from containerd_registry_setup import get_registry_auth
print(get_registry_auth())
")

if [ -z "$AUTH_TOKEN" ]; then
  echo "ERROR: Failed to generate registry credentials" >&2
  exit 1
fi

for node in $(./kind get nodes --name "${CLUSTER}"); do
  echo "Setting up registry credentials on kind node: $node ..."
  podman cp "${node}:/etc/containerd/config.toml" "./.tmp/${node}-containerd.toml"
  ../.tox/py310-linux/bin/python ./scripts/containerd_registry_setup.py "./.tmp/${node}-containerd.toml" "$AUTH_TOKEN"
  podman cp "./.tmp/${node}-containerd.toml" "${node}:/etc/containerd/config.toml"
  rm ./.tmp/${node}-containerd.toml
  podman exec "${node}" systemctl restart containerd.service
  podman exec "${node}" systemctl restart kubelet.service
done
