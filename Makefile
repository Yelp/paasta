# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set ENV to 'YELP' if FQDN ends in '.yelpcorp.com'
# Otherwise, set ENV to the FQDN

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	PAASTA_ENV ?= YELP
else
	PAASTA_ENV ?= $(shell hostname --fqdn)
endif

ifeq ($(PAASTA_ENV),YELP)
	export DOCKER_REGISTRY ?= docker-dev.yelpcorp.com/
else
	export DOCKER_REGISTRY ?= docker.io/
	export INDEX_URL_BUILD_ARG ?= PIP_INDEX_URL
endif

.PHONY: all docs test itest k8s_itests quick-test

dev: .paasta/bin/activate
	.paasta/bin/tox

docs: .paasta/bin/activate
	.paasta/bin/tox -e docs

test: .paasta/bin/activate
	if [ "$(PAASTA_ENV)" != "YELP" ]; then \
		.paasta/bin/tox -e tests; \
	else \
		.paasta/bin/tox -e tests-yelpy; \
	fi

test-yelpy: .paasta/bin/activate
	.paasta/bin/tox -e tests-yelpy

test-not-yelpy: .paasta/bin/activate
	.paasta/bin/tox -e tests

quick-test: .tox/py38-linux
	TZ=UTC .tox/py38-linux/bin/py.test --failed-first -x --disable-warnings -- tests

.tox/py38-linux: .paasta/bin/activate
	.paasta/bin/tox

dev-api: .tox/py38-linux
	.paasta/bin/tox -e dev-api

.paasta/bin/activate: requirements.txt requirements-dev.txt
	test -d .paasta/bin/activate || virtualenv -p python3.8 .paasta
	.paasta/bin/pip install -U \
		pip==18.1 \
		virtualenv==16.2.0 \
		tox==3.7.0 \
		tox-pip-extensions==1.4.2
	touch .paasta/bin/activate

itest: test .paasta/bin/activate
	.paasta/bin/tox -e general_itests

itest_%:
	# See the makefile in yelp_package/Makefile for packaging stuff
	make -C yelp_package PAASTA_ENV=$(PAASTA_ENV) $@

# Steps to release
# 1. Bump version in yelp_package/Makefile
# 2. `make release`
release:
	make -C yelp_package release

clean:
	-rm -rf ./dist
	-make -C yelp_package clean
	-rm -rf docs/build
	-find . -name '*.pyc' -delete
	-find . -name '__pycache__' -delete
	-rm -rf .tox
	-rm -rf .paasta
	-make -C k8s_itests clean

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: install-hooks
install-hooks:
	tox -e install-hooks

k8s_itests: .paasta/bin/activate
	make -C k8s_itests all

.PHONY: k8s_fake_cluster
k8s_fake_cluster: .tox/py38-linux
	make -C k8s_itests .fake_cluster

.PHONY: k8s_clean
k8s_clean: .paasta/bin/activate
	make -C k8s_itests clean

# image source: openapitools/openapi-generator-cli:latest
# java command:
#   in oapi repo: mvn clean && mvn install
#   in paasta repo: java -jar ~/openapi-generator/modules/openapi-generator-cli/target/openapi-generator-cli.jar
openapi-codegen:
	rm -rf paasta_tools/paastaapi
	docker run --rm -i -v `pwd`:/src -w /src \
		${DOCKER_REGISTRY}yelp/openapi-generator-cli:20201026 \
		generate \
		-i paasta_tools/api/api_docs/oapi.yaml \
		-g python-experimental \
		--package-name paasta_tools.paastaapi \
		-o temp-openapi-client \
		-p pythonAttrNoneIfUnset=true
	mv temp-openapi-client/paasta_tools/paastaapi paasta_tools/paastaapi
	rm -rf temp-openapi-client

swagger-validate:
	docker run --rm -i -v `pwd`:/src -w /src \
		${DOCKER_REGISTRY}yelp/openapi-generator-cli:20201026 \
		validate \
		-i paasta_tools/api/api_docs/swagger.json

.PHONY: vscode_settings
vscode_settings: .paasta/bin/activate .tox/py38-linux
	.paasta/bin/python paasta_tools/contrib/ide_helper.py

etc_paasta_playground soa_config_playground: .paasta/bin/activate .tox/py38-linux
	.tox/py38-linux/bin/python paasta_tools/contrib/create_paasta_playground.py

.PHONY: generate_deployments_for_service
generate_deployments_for_service: | soa_config_playground .tox/py38-linux
	export KUBECONFIG=./k8s_itests/kubeconfig;\
	export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/;\
	export PAASTA_TEST_CLUSTER=kind-${USER}-k8s-test;\
	.tox/py38-linux/bin/python -m paasta_tools.cli.cli list -a -y ./soa_config_playground | shuf | xargs -n 1 --no-run-if-empty \
	.tox/py38-linux/bin/python -m paasta_tools.generate_deployments_for_service -d ./soa_config_playground -v -s

.PHONY: playground-api
playground-api: .tox/py38-linux | soa_config_playground
	.paasta/bin/tox -e playground-api

.PHONY: setup-kubernetes-job
setup-kubernetes-job: k8s_fake_cluster generate_deployments_for_service
	export KUBECONFIG=./k8s_itests/kubeconfig;\
	export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/;\
	export PAASTA_TEST_CLUSTER=kind-${USER}-k8s-test;\
	.tox/py38-linux/bin/python -m paasta_tools.list_kubernetes_service_instances -d ./soa_config_playground --shuffle --group-lines 1 | xargs --no-run-if-empty .tox/py38-linux/bin/python -m paasta_tools.setup_kubernetes_job -d ./soa_config_playground -c kind-${USER}-k8s-test

.PHONY: cleanup-kubernetes-jobs
cleanup-kubernetes-jobs:
	export KUBECONFIG=./k8s_itests/kubeconfig;\
	export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/;\
	export PAASTA_TEST_CLUSTER=kind-${USER}-k8s-test;\
	.tox/py38-linux/bin/python -m paasta_tools.cleanup_kubernetes_jobs -d ./soa_config_playground -c kind-${USER}-k8s-test --force

.PHONY: paasta-secrets-sync
paasta-secrets-sync: setup-kubernetes-job .vault-token
	export KUBECONFIG=./k8s_itests/kubeconfig;\
	export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/;\
	export PAASTA_TEST_CLUSTER=kind-${USER}-k8s-test;\
	{ .tox/py38-linux/bin/python -m paasta_tools.list_kubernetes_service_instances -d ./soa_config_playground ; echo -n \ _shared; } | cut -f1 -d"." | uniq | shuf | xargs .tox/py38-linux/bin/python -m paasta_tools.kubernetes.bin.paasta_secrets_sync -v -d ./soa_config_playground -t ./.vault-token

define ANNOUNCE_CRONS_BODY
The following PaaSTA cron jobs will run on an infinite loop using the PaaSTA Playground k8s cluster:
- setup-kubernetes-job
- cleanup-kubernetes-job
- paasta-secrets-sync
- generate_deployments_for_service
endef
export ANNOUNCE_CRONS_BODY
.PHONY: paasta-crons
make paasta-cronjobs:
	@echo "$$ANNOUNCE_CRONS_BODY"
	while true; do make paasta-secrets-sync && make cleanup-kubernetes-jobs; sleep 5; done

.vault-token:
	export VAULT_ADDR=https://vault-devc.yelpcorp.com:8200 ;\
	export VAULT_SKIP_VERIFY=true ;\
	vault login -token-only -method=ldap > .vault-token

.PHONY: clean-playground
clean-playground:
		rm -rf ./etc_paasta_playground
		rm -rf ./soa_config_playground
		rm -f .vault-token
