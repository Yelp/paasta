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
	PAASTA_ENV ?= YELP
else
	export PIP_INDEX_URL ?= https://pypi.python.org/simple
	PAASTA_ENV ?= $(shell hostname -f)
endif

.PHONY: all docs test itest

docs: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL) -e docs

test: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL)

.tox/py36-linux: .paasta/bin/activate
	.paasta/bin/tox -i $(PIP_INDEX_URL)

dev-api: .tox/py36-linux
	.tox/py36-linux/bin/python -m paasta_tools.run-paasta-api-in-dev-mode

.paasta/bin/activate: requirements.txt requirements-dev.txt
	test -d .paasta/bin/activate || virtualenv -p python3.6 .paasta
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
	rm -rf ./dist
	make -C yelp_package clean
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox
	rm -rf .paasta

yelpy: ## Installs the yelp-internal packages into the default tox environment
	.tox/py36-linux/bin/pip-custom-platform install -i https://pypi.yelpcorp.com/simple -r yelp_package/extra_requirements_yelp.txt -r ./extra-linux-requirements.txt


.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: install-hooks
install-hooks:
	tox -e install-hooks
