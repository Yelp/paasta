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

DOCKER_TAG ?= fake_simple_service-$(USER)-dev

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	DOCKER_REGISTRY ?= docker-dev.yelpcorp.com/
	PIP_INDEX_URL ?= https://pypi.yelpcorp.com/simple
	export XENIAL_IMAGE_NAME ?= xenial_pkgbuild
	export BIONIC_IMAGE_NAME ?= bionic_pkgbuild
	PAASTA_ENV ?= YELP
else
	DOCKER_REGISTRY ?= ""
	PIP_INDEX_URL ?= https://pypi.python.org/simple
	export XENIAL_IMAGE_NAME ?= ubuntu:xenial
	export BIONIC_IMAGE_NAME ?= ubuntu:bionic
	PAASTA_ENV ?= $(shell hostname -f)
endif

.PHONY: cook-image

cook-image:
	docker build --build-arg PIP_INDEX_URL=$(PIP_INDEX_URL) --build-arg DOCKER_REGISTRY=$(DOCKER_REGISTRY) --build-arg IMAGE_NAME=${BIONIC_IMAGE_NAME} -t $(DOCKER_TAG) .
