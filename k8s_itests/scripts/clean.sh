#!/bin/bash
PAASTA_CONFIG_DIR=$1
KIND_CLUSTER=$2

rm -rf .kube
rm  .create_cluster
rm -rf $PAASTA_CONFIG_DIR
./kind delete cluster --name=$KIND_CLUSTER
unset KUBECONFIG
rm ./kind
