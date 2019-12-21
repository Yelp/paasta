# You need kind installed to run this!
CLUSTER  := ${USER}-k8s-test
CWD := $(shell pwd)
export PAASTA_CONFIG_DIR=$(CWD)/deployments/.tmp
export KUBE_RESOURCE_DIR=$(CWD)/deployments/kind/resources
export KIND_CLUSTER=$(CLUSTER)
export SOA_DIR=$(PAASTA_CONFIG_DIR)/fake_soa_config
export PAASTA_SYSTEM_CONFIG_DIR=$(PAASTA_CONFIG_DIR)/fake_etc_paasta

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	export KIND_CONFIG=deployments/kind/cluster-devbox.yaml
	export KUBECONFIG=$(shell kind get kubeconfig-path --name=$(KIND_CLUSTER))
else
	export KIND_CONFIG=deployments/kind/cluster.yaml
	pip install ephemeral-port-reserve
endif

export PAASTA_API_PORT=$(shell ephemeral-port-reserve)

all: clean itest

.create_cluster:
	kind create cluster --name=$(KIND_CLUSTER) --config=$(KIND_CONFIG)
	touch .create_cluster

itest: .create_cluster
	../.paasta/bin/tox -e k8s_itests

clean:
	./scripts/clean.sh $(PAASTA_CONFIG_DIR) $(KIND_CLUSTER)
