# You need kind installed to run this!

CLUSTER  := paasta-k8s-test-cluster
export KIND_CLUSTER=$(CLUSTER)

all: clean init itest

init:
	kind create cluster --name=$(KIND_CLUSTER) --config=./deployments/cluster.yaml

itest:
	export KUBECONFIG=`kind get kubeconfig-path --name="paasta-k8s-test-cluster"`; ../.paasta/bin/tox -e k8s_itests

clean:
	rm -rf .kube
	kind delete cluster --name=$(KIND_CLUSTER)
