#!/bin/sh
set -o errexit -x

REG_NAME=$1
REG_PORT=$2
CLUSTER_NAME=$3

# create registry container unless it already exists
running="$(docker inspect -f '{{.State.Running}}' "${REG_NAME}" 2>/dev/null || true)"
if [ "${running}" != 'true' ]; then
    docker run -d --restart=always -e REGISTRY_HTTP_ADDR=0.0.0.0:${REG_PORT} -p "${REG_PORT}:${REG_PORT}" --name "${REG_NAME}" registry:2
fi

# connect the registry to the cluster network
docker network connect "kind" "${REG_NAME}"

# tell https://tilt.dev to use the registry
# https://docs.tilt.dev/choosing_clusters.html#discovering-the-registry
for node in $(kind get nodes --name ${CLUSTER_NAME}); do
    kubectl annotate node "${node}" "kind.x-k8s.io/registry=localhost:${REG_PORT}";
done
