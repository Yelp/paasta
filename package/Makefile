.PHONY: itest_% itest_%-external package_% package_%-external build_%_docker

SYSTEM_PKG_NAME ?= clusterman
PYTHON_PKG_NAME ?= $(shell cd $(CURDIR)/.. && python setup.py --name)
PACKAGE_VERSION ?= $(shell cd $(CURDIR)/.. && python setup.py --version)
ACCEPTANCE_DIR:=$(CURDIR)/../acceptance

UID:=`id -u`
GID:=`id -g`
DOCKER_BUILD_RUN:=docker run -t -v $(CURDIR)/..:/src:ro -v $(CURDIR)/dist:/dist:rw
VERSIONED_FILES:=$(shell cd $(CURDIR)/.. && git ls-files -z --cached --modified | xargs -0 -I@ echo -n "'@' ")
DOCKER_WORKDIR:=mkdir -p /work && cd /src && cp -vP --parents $(VERSIONED_FILES) /work/ && cp -r completions /work/ && cd /work

itest_%: export EXTRA_VOLUME_MOUNTS=/nail/etc/services/services.yaml:/nail/etc/services/services.yaml:ro
itest_%: package_% dist/%/Packages.gz
	./debian-itest-runner $* $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION)

itest_%-external: export EXTRA_VOLUME_MOUNTS=$(ACCEPTANCE_DIR)/srv-configs/clusterman-external.yaml:/nail/srv/configs/clusterman.yaml:ro
itest_%-external: package_%_external dist/%/Packages.gz
	./debian-itest-runner $* $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION)

dist/%/Packages.gz:
	$(DOCKER_BUILD_RUN) $(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
	  cd /dist/$* && dpkg-scanpackages . /dev/null | gzip -9c > Packages.gz \
	  && chown $(UID):$(GID) Packages.gz"

package_%: build_%_docker
	mkdir -p $(CURDIR)/dist/$*
	$(DOCKER_BUILD_RUN) \
		$(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
		$(DOCKER_WORKDIR) && cat extra-requirements-yelp.txt >> requirements.txt \
		&& dpkg-buildpackage -d \
		&& mv ../*.deb /dist/$*/$(SYSTEM_PKG_NAME)_$(PACKAGE_VERSION)_amd64.deb \
		&& chown $(UID):$(GID) /dist/$*/*.deb"

package_%_external: build_%_docker
	mkdir -p $(CURDIR)/dist/$*
	$(DOCKER_BUILD_RUN) \
		$(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
		$(DOCKER_WORKDIR) && echo \"file:///work/examples/clusterman_metrics\" >> requirements.txt \
		&& dpkg-buildpackage -d -Rdebian/rules.external \
		&& mv ../*.deb /dist/$*/$(SYSTEM_PKG_NAME)_$(PACKAGE_VERSION)_amd64.deb \
		&& chown $(UID):$(GID) /dist/$*/*.deb"

build_%_docker:
	mkdir -p dist
	cd dockerfiles/$*/ && docker build -t "$(SYSTEM_PKG_NAME)_$*_container" .
