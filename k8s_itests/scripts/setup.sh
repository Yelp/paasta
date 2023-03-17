#!/bin/bash
echo "Configuring paasta configuration directory"

rm -rf $PAASTA_CONFIG_DIR
mkdir $PAASTA_CONFIG_DIR
./scripts/render_template.py -s deployments/paasta/ -d $PAASTA_CONFIG_DIR

echo "Applying kubernetes resources"
kubectl apply -Rf $KUBE_RESOURCE_DIR --context $KIND_CLUSTER



ZOOKEEPER_PORT=$(shuf -i 2000-65000 -n 1)

docker run -p "${ZOOKEEPER_PORT}":2181 -e "ZOO_SERVERS=0.0.0.0:2888:3888" -e "ALLOW_ANONYMOUS_LOGIN=yes" --name "${USER}"-paasta-zookeeper zookeeper:3.5
