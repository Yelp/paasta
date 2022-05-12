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
	export PIP_INDEX_URL ?= https://pypi.yelpcorp.com/simple
	export DOCKER_REGISTRY ?= docker-dev.yelpcorp.com/
	PAASTA_ENV ?= YELP
	DOCKER_AWS_ENV := -e AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN
else
	export PIP_INDEX_URL ?= https://pypi.python.org/simple
	export DOCKER_REGISTRY ?= ""
	PAASTA_ENV ?= $(shell hostname -f)
endif

.PHONY: all docs test itest k8s_itests

dev: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL)

docs: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e docs

test: .paasta/bin/activate
	if [ "$(PAASTA_ENV)" != "YELP" ]; then \
		.paasta/bin/tox -i $(PIP_INDEX_URL) -e tests; \
	else \
		.paasta/bin/tox -i $(PIP_INDEX_URL) -e tests-yelpy; \
	fi

test-yelpy: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e tests-yelpy

test-not-yelpy: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e tests

.tox/py37-linux: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL)

dev-api: .tox/py37-linux
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e dev-api

.paasta/bin/activate: requirements.txt requirements-dev.txt
	test -d .paasta/bin/activate || virtualenv -p python3.7 .paasta
	.paasta/bin/pip install -U \
		pip==18.1 \
		virtualenv==16.2.0 \
		tox==3.7.0 \
		tox-pip-extensions==1.4.2
	touch .paasta/bin/activate

itest: test .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e general_itests
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e paasta_itests

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

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: install-hooks
install-hooks:
	tox -e install-hooks

k8s_itests: .paasta/bin/activate
	make -C k8s_itests all

# image source: openapitools/openapi-generator-cli:latest
# java command:
#   in oapi repo: mvn clean && mvn install
#   in paasta repo: java -jar ~/openapi-generator/modules/openapi-generator-cli/target/openapi-generator-cli.jar
openapi-codegen:
	rm -rf paasta_tools/paastaapi
	docker run --rm -i --user `id -u`:`id -g` -v `pwd`:/src -w /src \
		yelp/openapi-generator-cli:20201026 \
		generate \
		-i paasta_tools/api/api_docs/oapi.yaml \
		-g python-experimental \
		--package-name paasta_tools.paastaapi \
		-o temp-openapi-client \
		-p pythonAttrNoneIfUnset=true
	mv temp-openapi-client/paasta_tools/paastaapi paasta_tools/paastaapi
	rm -rf temp-openapi-client

build-image:
	make -C yelp_package PAASTA_ENV=$(PAASTA_ENV) package_bionic
	docker build -t paasta-tools:latest .

push-image:
	docker tag paasta-tools:latest docker-dev.yelpcorp.com/hack-2776-paasta-tools
	docker push docker-dev.yelpcorp.com/hack-2776-paasta-tools

run-kube-job:
	kubectl --context infra-uswest2-devc apply -f paasta-tools-job.yaml

run-local-soa:
	docker run --rm --init -d \
		--name soaconfigs \
		paasta-tools:soaconfigs true

# get some tokens with something like `eval $(aws-okta -a dev -r production-engineering)` assuming your role has permissions on the target cluster (see .kubeconfig)
run-local: run-local-soa
	docker run -ti --init --rm \
		$(DOCKER_AWS_ENV) \
		-v $$HOME/git/yelpsoa-configs/:/nail/etc/services:ro \
		-v $(PWD)/.kubeconfig:/root/.kube/config:ro \
		-e KUBECONFIG=/root/.kube/config \
		--volumes-from soaconfigs \
		paasta-tools:latest setup_kubernetes_job --cluster fake_baking_cluster compute-infra-test-service.one_instance
