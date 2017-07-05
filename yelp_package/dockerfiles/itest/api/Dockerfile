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

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update > /dev/null && apt-get install -y --no-install-recommends \
        debhelper \
        dpkg-dev \
        gcc \
        gdebi-core \
        git \
        help2man \
        libffi-dev \
        libssl-dev \
        libyaml-dev \
        pyflakes \
        python-dev \
        python-pip \
        python-pytest \
        python-setuptools \
        python-tox \
        python-yaml \
        wget \
        zip \
        zsh > /dev/null \
    && apt-get clean > /dev/null

WORKDIR /work

RUN cd /work
ADD requirements.txt /work
RUN pip install -r requirements.txt
ADD . /work/
RUN mkdir -p /nail/etc/
RUN mkdir -p /etc/paasta/
RUN cp /work/yelp_package/dockerfiles/itest/api/mesos-cli.json /nail/etc/
RUN cp /work/yelp_package/dockerfiles/trusty/mesos-slave-secret /etc/
RUN cp /work/yelp_package/dockerfiles/trusty/mesos-slave-secret /nail/etc/
RUN cp /work/yelp_package/dockerfiles/itst/api/*.json /etc/paasta/
RUN pip install -e /work/
