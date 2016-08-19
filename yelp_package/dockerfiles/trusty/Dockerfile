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

RUN apt-get update && apt-get -y install dpkg-dev python-tox python-setuptools \
  python-dev debhelper python-yaml libyaml-dev python-pytest pyflakes \
  git help2man zsh wget

RUN apt-get install -yq ruby1.9.1 ruby1.9.1-dev rubygems1.9.1 zlib1g-dev
RUN gem install --no-ri --no-rdoc synapse -v 0.12.1

ADD https://github.com/haproxy/haproxy/archive/v1.6.0.tar.gz /haproxy.tar.gz
RUN tar -axvf /haproxy.tar.gz
WORKDIR /haproxy-1.6.0
RUN make TARGET=linux26 && mv haproxy /usr/bin/haproxy-synapse

RUN cd `mktemp -d` && wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_0.11-1_all.deb && dpkg -i dh-virtualenv_0.11-1_all.deb && apt-get -f install

ENV HOME /work
ENV PWD /work
WORKDIR /work
