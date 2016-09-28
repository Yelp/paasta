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

FROM ubuntu:trusty

RUN echo "deb http://repos.mesosphere.com/ubuntu trusty main" > /etc/apt/sources.list.d/mesosphere.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF

RUN apt-get update && apt-get -y install dpkg-dev python-tox python-setuptools \
  python-dev libssl-dev libffi-dev debhelper python-yaml libyaml-dev python-pytest pyflakes \
  git help2man zsh wget zip

RUN cd `mktemp -d` && wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_0.11-1_all.deb && dpkg -i dh-virtualenv_0.11-1_all.deb && apt-get -f install

# mesos.native is not available on pypi, so we can either build mesos from
# source or do this, and building from source takes a long time.
# Conveniently, the .so's included in this wheel seem to be self-contained and don't link against the system libmesos.
# So that we know if this changes, we remove the mesos system package after creating the .whl, so itests run without
# a system mesos package.
RUN apt-get install -yq mesos=1.0.1-2.0.93.ubuntu1404 && \
    cd /usr/lib/python2.7/site-packages && \
	zip -r /root/mesos.native-1.0.1-py27-none-any.whl mesos/native mesos.native-1.0.1.dist-info && \
	zip -r /root/mesos.executor-1.0.1-py27-none-any.whl mesos/executor mesos.executor-1.0.1.dist-info && \
	zip -r /root/mesos.scheduler-1.0.1-py27-none-any.whl mesos/scheduler mesos.scheduler-1.0.1.dist-info && \
	apt-get remove -yq mesos

ADD mesos-slave-secret /etc/mesos-slave-secret

ENV HOME /work
ENV PWD /work
WORKDIR /work
