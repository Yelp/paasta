UWSGI_EXPORTER_TAG ?= v1.0.0

DOCKER_IMAGE ?= docker-paasta.yelpcorp.com:443/uwsgi_exporter-k8s-sidecar:$(UWSGI_EXPORTER_TAG)

all: docker_image push

uwsgi_exporter:
	git clone --branch $(UWSGI_EXPORTER_TAG) https://github.com/timonwong/uwsgi_exporter

uwsgi_exporter/uwsgi_exporter: uwsgi_exporter
	make -C uwsgi_exporter

docker_image: uwsgi_exporter/uwsgi_exporter
	docker build -t $(DOCKER_IMAGE) .

push:
	sudo -H docker push $(DOCKER_IMAGE)
