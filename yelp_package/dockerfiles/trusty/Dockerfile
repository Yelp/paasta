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
  python-dev libffi-dev libssl-dev debhelper python-yaml libyaml-dev python-pytest pyflakes \
  git help2man zsh wget

RUN cd `mktemp -d` && wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_0.11-1_all.deb && dpkg -i dh-virtualenv_0.11-1_all.deb && apt-get -f install

ENV HOME /work
ENV PWD /work
WORKDIR /work
