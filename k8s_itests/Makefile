# You need kind installed to run this!
CLUSTER  := ${USER}-k8s-test
CWD := $(shell pwd)
export PAASTA_CONFIG_DIR=$(CWD)/deployments/.tmp
export KUBE_RESOURCE_DIR=$(CWD)/deployments/kind/resources
export KIND_CLUSTER=kind-$(CLUSTER)
export SOA_DIR=$(PAASTA_CONFIG_DIR)/fake_soa_config
export PAASTA_SYSTEM_CONFIG_DIR=$(PAASTA_CONFIG_DIR)/fake_etc_paasta
export PAASTA_API_PORT=$(shell ephemeral-port-reserve)
export KUBECONFIG=$(CWD)/kubeconfig

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	export KIND_CONFIG=deployments/kind/cluster-devbox.yaml
	PAASTA_K8S_ENV ?= YELP
else
	export KIND_CONFIG=deployments/kind/cluster.yaml
	PAASTA_K8S_ENV ?= $(shell hostname --fqdn)
endif

.PHONY: all itest fake_cluster kind create_cluster clean

all: clean itest

fake_cluster: create_cluster
ifeq (,$(wildcard ./.fake_cluster))
	if [ "$(PAASTA_K8S_ENV)" = "YELP" ]; then \
		./scripts/set-paasta-registry-credentials.sh $(CLUSTER); \
	fi
	touch .fake_cluster
endif

kind:
ifeq (,$(wildcard ./kind))
	./scripts/install-kind.sh
endif

create_cluster: kind
ifeq (,$(wildcard ./.create_cluster))
	./kind create cluster --name=$(CLUSTER) --config=$(KIND_CONFIG) --kubeconfig $(KUBECONFIG)
	touch .create_cluster
endif

itest: create_cluster
	../.paasta/bin/tox -e k8s_itests

clean:
	./scripts/clean.sh $(PAASTA_CONFIG_DIR) $(CLUSTER) 2>/dev/null || true
