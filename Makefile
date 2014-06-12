
UID:=`id -u`
GID:=`id -g`
DOCKER_RUN:=docker run -t -v  $(CURDIR):/work:rw soatools_lucid_container

all:

itest_lucid: package_lucid
	$(DOCKER_RUN) /work/itest/ubuntu.sh

package_lucid: test_lucid
	$(DOCKER_RUN) /bin/bash -c "cd src && dpkg-buildpackage -d && mv ../*.deb ../dist/"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

test_lucid: build_lucid_docker
	find . -name "*.pyc" -exec rm -rf {} \;
	cd src && tox
#	$(DOCKER_RUN) bash -c "cd src && tox"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

build_lucid_docker:
	[ -d dist ] || mkdir dist
	cd dockerfiles/lucid/ && docker build -t "soatools_lucid_container" .

clean:
	rm -rf dist/
	rm -rf .tox

