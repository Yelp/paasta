DOCKER_TAG_VERSION:=$(shell git rev-parse HEAD)
DOCKER_HOST:=docker-paasta.yelpcorp.com:443
DOCKER_IMG_NAME:=hacheck-k8s-sidecar


.PHONY: build
build:
	docker build -t $(DOCKER_HOST)/$(DOCKER_IMG_NAME):$(DOCKER_TAG_VERSION) .

.PHONY: push
push: build
	sudo -i bash -c 'docker push $(DOCKER_HOST)/$(DOCKER_IMG_NAME):$(DOCKER_TAG_VERSION)'
