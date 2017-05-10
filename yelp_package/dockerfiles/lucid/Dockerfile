# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM docker-dev.yelpcorp.com/lucid_pkgbuild
MAINTAINER Kyle Anderson <kwa@yelp.com>

ENV DEBIAN_FRONTEND=noninteractive
# Make sure we get a package suitable for building this package correctly.
# Per dnephin we need https://github.com/spotify/dh-virtualenv/pull/20
# Which at this time is in this package
RUN apt-get update > /dev/null && apt-get -y install dpkg-dev python-pip python-setuptools python-dev libssl-dev libffi-dev debhelper dh-virtualenv python-yaml libyaml-dev python-pytest pyflakes python2.7 python2.7-dev help2man zsh git > /dev/null

# mesos.native is not available on pypi, so we can either build mesos from
# source or do this, and building from source takes a long time.
# Conveniently, the .so's included in this wheel seem to be self-contained and don't link against the system libmesos.
# So that we know if this changes, we remove the mesos system package after creating the .whl, so itests run without
# a system mesos package.
RUN apt-get install -y mesos=1.0.1-yelp2 zip > /dev/null && \
    cd /usr/local/lib/python2.7/site-packages/ && \
	zip -r /root/mesos.native-1.0.1-cp27-none-linux_x86_64.whl mesos/native mesos.native-1.0.1.dist-info && \
	zip -r /root/mesos.executor-1.0.1-cp27-none-linux_x86_64.whl mesos/executor mesos.executor-1.0.1.dist-info && \
	zip -r /root/mesos.scheduler-1.0.1-cp27-none-linux_x86_64.whl mesos/scheduler mesos.scheduler-1.0.1.dist-info && \
	apt-get remove -y mesos


RUN pip install -U virtualenv==15.1.0 tox==2.6.0

WORKDIR /work
