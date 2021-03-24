UWSGI_EXPORTER_REPO ?= https://github.com/timonwong/uwsgi_exporter
UWSGI_EXPORTER_TAG ?= v1.0.0
YELP_SUFFIX ?= yelp2

DOCKER_IMAGE ?= docker-paasta.yelpcorp.com:443/uwsgi_exporter-k8s-sidecar:$(UWSGI_EXPORTER_TAG)-$(YELP_SUFFIX)

all: docker_image push

uwsgi_exporter:
	git clone --branch $(UWSGI_EXPORTER_TAG) $(UWSGI_EXPORTER_REPO)

checkout: uwsgi_exporter
	git -C uwsgi_exporter fetch $(UWSGI_EXPORTER_REPO)
	git -C uwsgi_exporter checkout --force $(UWSGI_EXPORTER_TAG)

uwsgi_exporter/uwsgi_exporter: checkout
	make -C uwsgi_exporter

docker_image: uwsgi_exporter/uwsgi_exporter
	docker build -t $(DOCKER_IMAGE) .

push: docker_image
	sudo -H docker push $(DOCKER_IMAGE)
