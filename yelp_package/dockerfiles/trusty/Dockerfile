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
  python-dev debhelper python-yaml libyaml-dev python-pytest pyflakes \
  git help2man zsh wget mesos=0.28.2-2.0.27.ubuntu1404

RUN cd `mktemp -d` && wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_0.11-1_all.deb && dpkg -i dh-virtualenv_0.11-1_all.deb && apt-get -f install

RUN apt-get install -yq zip
RUN cd /usr/lib/python2.7/site-packages && zip -r /root/mesos.native-0.28.2-py27-none-any.whl mesos/native mesos.native-0.28.2.dist-info

ENV HOME /work
ENV PWD /work
WORKDIR /work
