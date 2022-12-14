#!/bin/bash
set -e

CLUSTER=$1

for node in $(./kind get nodes --name "${CLUSTER}"); do
  echo "Moving credentials to kind node: $node ..."
  docker cp "${node}:/etc/containerd/config.toml" "./.tmp/${node}-containerd.toml"
  ../.tox/py37-linux/bin/python ./scripts/containerd_registry_setup.py "./.tmp/${node}-containerd.toml"
  docker cp "./.tmp/${node}-containerd.toml" "${node}:/etc/containerd/config.toml"
  rm ./.tmp/${node}-containerd.toml
#   restart kubelet and containerd to pick up the updated config
  docker exec "${node}" systemctl restart containerd.service
  docker exec "${node}" systemctl restart kubelet.service
done
