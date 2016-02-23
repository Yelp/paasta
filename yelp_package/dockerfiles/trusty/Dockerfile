# Copyright 2015 Yelp Inc.
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

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 2F6464D3394993A19970736C88ADA4A042F8DD13 && \
    echo 'deb http://ppa.launchpad.net/dh-virtualenv/daily/ubuntu trusty main\ndeb-src http://ppa.launchpad.net/dh-virtualenv/daily/ubuntu trusty main' >> /etc/apt/sources.list

RUN apt-get update && apt-get -y install dpkg-dev python-tox python-setuptools \
  python-dev debhelper python-yaml python-pytest pyflakes \
  git help2man zsh dh-virtualenv=0.11-0~85~ubuntu14.04.1

ENV HOME /work
ENV PWD /work
WORKDIR /work
