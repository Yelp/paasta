# You need kind installed to run this!
CLUSTER  := ${USER}-k8s-test
CWD := $(shell pwd)
export PAASTA_CONFIG_DIR=$(CWD)/deployments/.tmp
export KUBE_RESOURCE_DIR=$(CWD)/deployments/kind/resources
export KIND_CLUSTER=$(CLUSTER)
export SOA_DIR=$(PAASTA_CONFIG_DIR)/fake_soa_config
export PAASTA_SYSTEM_CONFIG_DIR=$(PAASTA_CONFIG_DIR)/fake_etc_paasta
export PAASTA_API_PORT=$(shell ephemeral-port-reserve)
export KUBECONFIG=$(shell ./kind get kubeconfig-path --name=$(KIND_CLUSTER))

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	export KIND_CONFIG=deployments/kind/cluster-devbox.yaml
else
	export KIND_CONFIG=deployments/kind/cluster.yaml
endif

all: clean itest

kind:
	./scripts/install-kind.sh

.create_cluster: kind
	./kind create cluster --name=$(KIND_CLUSTER) --config=$(KIND_CONFIG)
	touch .create_cluster

itest: .create_cluster
	../.paasta/bin/tox -e k8s_itests

clean:
	./scripts/clean.sh $(PAASTA_CONFIG_DIR) $(KIND_CLUSTER) 2>/dev/null || true
