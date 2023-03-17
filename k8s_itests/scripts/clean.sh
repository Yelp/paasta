#!/bin/bash
PAASTA_CONFIG_DIR=$1
KIND_CLUSTER=$2

rm -rf .kube
rm  .create_cluster
rm .fake_cluster
rm .create_cluster
rm -rf $PAASTA_CONFIG_DIR
rm kubeconfig
./kind delete cluster --name=$KIND_CLUSTER
unset KUBECONFIG
docker kill $USER-paasta-zookeeper
docker rm $USER-paasta-zookeeper
rm ./kind
rm -rf ./.tmp
