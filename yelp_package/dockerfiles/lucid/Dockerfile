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
RUN apt-get update > /dev/null && apt-get -y install \
        dpkg-dev \
        python-pip \
        python-setuptools \
        python-dev \
        libssl-dev \
        libffi-dev \
        debhelper \
        dh-virtualenv \
        python-yaml \
        libyaml-dev \
        python-pytest \
        pyflakes \
        python3.6 \
        python3.6-dev \
        help2man \
        zsh \
        git > /dev/null \
    && apt-get clean > /dev/null


RUN pip install -U virtualenv==15.1.0 tox==2.6.0

WORKDIR /work
