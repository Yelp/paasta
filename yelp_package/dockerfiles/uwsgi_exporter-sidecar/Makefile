UWSGI_EXPORTER_REPO ?= https://github.com/timonwong/uwsgi_exporter
UWSGI_EXPORTER_TAG ?= v1.3.0
YELP_SUFFIX ?= yelp0

DOCKER_IMAGE ?= docker-paasta.yelpcorp.com:443/uwsgi_exporter-k8s-sidecar:$(UWSGI_EXPORTER_TAG)-$(YELP_SUFFIX)

all: docker_image check_if_push_needed push

uwsgi_exporter:
	git clone --branch $(UWSGI_EXPORTER_TAG) $(UWSGI_EXPORTER_REPO)

checkout: uwsgi_exporter
	git -C uwsgi_exporter fetch $(UWSGI_EXPORTER_REPO)
	git -C uwsgi_exporter checkout --force $(UWSGI_EXPORTER_TAG)

uwsgi_exporter/uwsgi_exporter: checkout
	make -C uwsgi_exporter test build

docker_image: uwsgi_exporter/uwsgi_exporter
	docker build -t $(DOCKER_IMAGE) .

push: docker_image
	sudo -H docker push $(DOCKER_IMAGE)

# NOTE: we can get rid of this target if we're ok with overwriting the currently
# tagged image on every run of the CI pipeline that will use this Makefile
.PHONY: check_if_push_needed
check_if_push_needed:
	# if run on a non-Jammy box, this requires `DOCKER_CLI_EXPERIMENTAL=enabled`
	# to be set as an env var
	# this will return 1 if the image does not exist, 0 otherwise - so we need to invert
	# these
	if sudo -H docker manifest inspect ${DOCKER_IMAGE} > /dev/null 2>&1; \
	then \
		echo 'Image already exists - cowardly refusing to continue' && false; \
	else \
		echo 'Image does not exist' && true; \
	fi
