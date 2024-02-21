EXPORTER_REPO ?= https://github.com/prometheus/statsd_exporter
EXPORTER_TAG ?= v0.24.0
YELP_SUFFIX ?= yelp0

DOCKER_IMAGE ?= docker-paasta.yelpcorp.com:443/gunicorn_exporter-k8s-sidecar:$(EXPORTER_TAG)-$(YELP_SUFFIX)

all: docker_image check_if_push_needed push

statsd_exporter:
	git clone --branch $(EXPORTER_TAG) $(EXPORTER_REPO)

checkout: statsd_exporter
	git -C statsd_exporter fetch --tags $(EXPORTER_REPO)
	git -C statsd_exporter checkout --force $(EXPORTER_TAG)

statsd_exporter/statsd_exporter: checkout
	make -C statsd_exporter test build

docker_image: statsd_exporter/statsd_exporter
	docker build -t $(DOCKER_IMAGE) .

push: docker_image
ifeq ($(CI), true)
	docker push $(DOCKER_IMAGE)
else
	sudo -H docker push $(DOCKER_IMAGE)
endif

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
