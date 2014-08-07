
UID:=`id -u`
GID:=`id -g`
DOCKER_RUN:=docker run -t -v  $(CURDIR):/work:rw soatools_lucid_container
DOCKER_RUN_CHRONOS:=docker run -t -i --link=chronos_itest_chronos:chronos -v  $(CURDIR):/work:rw chronos_itest/itest

.PHONY: all docs

all:

docs:
	cd src && tox -e docs

itest_lucid: package_lucid
	$(DOCKER_RUN) /work/itest/ubuntu.sh

package_lucid: test_lucid
	$(DOCKER_RUN) /bin/bash -c "cd src && dpkg-buildpackage -d && mv ../*.deb ../dist/"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

test_lucid: build_lucid_docker
	find . -name "*.pyc" -exec rm -rf {} \;
	cd src && tox -r
#	$(DOCKER_RUN) bash -c "cd src && tox"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

build_lucid_docker:
	[ -d dist ] || mkdir dist
	cd dockerfiles/lucid/ && docker build -t "soatools_lucid_container" .

clean:
	rm -rf dist/
	rm -rf .tox
	rm -rf src/service_deployment_tools.egg-info
	rm -rf src/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

test_chronos: package_lucid setup_chronos_itest
	$(DOCKER_RUN_CHRONOS) /work/itest/chronos.sh
	make cleanup_chronos_itest

setup_chronos_itest: build_chronos_itest
	docker run -d --name=chronos_itest_zk chronos_itest/zookeeper
	docker run -d --name=chronos_itest_mesos --link chronos_itest_zk:zookeeper chronos_itest/mesos
	docker run -d --name=chronos_itest_chronos --link=chronos_itest_mesos:mesos --link=chronos_itest_zk:zookeeper chronos_itest/chronos

cleanup_chronos_itest:
	docker kill chronos_itest_zk
	docker kill chronos_itest_mesos
	docker kill chronos_itest_chronos
	docker rm chronos_itest_zk
	docker rm chronos_itest_mesos
	docker rm chronos_itest_chronos



build_chronos_itest: build_chronos_itest_zookeeper_docker build_chronos_itest_mesos_docker build_chronos_itest_chronos_docker build_chronos_itest_itest_docker

build_chronos_itest_zookeeper_docker:
	cd dockerfiles/chronos_itest/zookeeper/ && docker build -t "chronos_itest/zookeeper" .

build_chronos_itest_mesos_docker:
	cd dockerfiles/chronos_itest/mesos/ && docker build -t "chronos_itest/mesos" .

build_chronos_itest_chronos_docker:
	cd dockerfiles/chronos_itest/chronos/ && docker build -t "chronos_itest/chronos" .

build_chronos_itest_itest_docker:
	cd dockerfiles/chronos_itest/itest/ && docker build -t "chronos_itest/itest" .
