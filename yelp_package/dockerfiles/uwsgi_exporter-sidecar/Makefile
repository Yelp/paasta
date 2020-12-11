DOCKER_IMAGE ?= docker-paasta.yelpcorp.com:443/uwsgi_exporter-k8s-sidecar

all: docker_image push

uwsgi_exporter:
	git clone --branch v1.0.0 https://github.com/timonwong/uwsgi_exporter

uwsgi_exporter/uwsgi_exporter: uwsgi_exporter
	make -C uwsgi_exporter

docker_image: uwsgi_exporter/uwsgi_exporter
	docker build -t $(DOCKER_IMAGE) .

push:
	sudo -i bash -c 'docker push $(DOCKER_IMAGE)'
