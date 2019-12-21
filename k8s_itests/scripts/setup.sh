#!/bin/bash
echo "Configuring paasta configuration directory"
rm -rf $PAASTA_CONFIG_DIR
mkdir $PAASTA_CONFIG_DIR
./scripts/render_template.py -s deployments/paasta/ -d $PAASTA_CONFIG_DIR

echo "Applying kubernetes resources"
kubectl apply -Rf $KUBE_RESOURCE_DIR
