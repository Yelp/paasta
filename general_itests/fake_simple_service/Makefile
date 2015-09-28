DOCKER_TAG ?= fake_simple_service-$(USER)-dev
export DOCKER_TAG

.PHONY: cook-image

cook-image:
	docker build -t "$$DOCKER_TAG" .
