# You need kind installed to run this!
CLUSTER  := ${USER}-k8s-test
PAASTA_CONFIG_DIR := deployments/.tmp
export KIND_CLUSTER=$(CLUSTER)
export SOA_DIR=$(PAASTA_CONFIG_DIR)/fake_soa_config
export PAASTA_SYSTEM_CONFIG_DIR=$(PAASTA_CONFIG_DIR)/fake_etc_paasta
export KUBECONFIG=$(shell kind get kubeconfig-path --name=$(KIND_CLUSTER))

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	export KIND_CONFIG=deployments/kind/cluster-devbox.yaml
else
	export KIND_CONFIG=deployments/kind/cluster.yaml
endif


all: clean init itest

init:
	kind create cluster --name=${KIND_CLUSTER} --config=$(KIND_CONFIG)

itest:
	rm -rf $(PAASTA_CONFIG_DIR)
	mkdir $(PAASTA_CONFIG_DIR)
	python3.6 tools/render_template.py -s deployments/paasta/ -d $(PAASTA_CONFIG_DIR)
	../.paasta/bin/tox -e k8s_itests

clean:
	rm -rf .kube
	rm -rf $(PAASTA_CONFIG_DIR)
	kind delete cluster --name=$(KIND_CLUSTER)
