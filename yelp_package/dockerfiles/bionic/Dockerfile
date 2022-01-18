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

ARG DOCKER_REGISTRY=docker-dev.yelpcorp.com/
ARG IMAGE_NAME=bionic_pkgbuild
FROM ${DOCKER_REGISTRY}${IMAGE_NAME}

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

ARG PAASTA_ENV
ENV PAASTA_ENV=$PAASTA_ENV

RUN rm /etc/dpkg/dpkg.cfg.d/excludes
RUN apt-get update && apt-get install -yq gnupg2

# Need Python 3.7
RUN apt-get update > /dev/null && \
    apt-get install -y --no-install-recommends software-properties-common && \
    if [ "$PAASTA_ENV" != "YELP" ]; then add-apt-repository ppa:deadsnakes/ppa; else true; fi # only use deadsnakes externally

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        debhelper \
        gdebi-core \
        git \
        libffi-dev \
        libgpgme11 \
        libgpgme-dev \
        libssl-dev \
        libyaml-dev \
        python3.7-dev \
        python-pip \
        python-tox \
        wget \
        zsh > /dev/null \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip==20.0.2
RUN pip install virtualenv==16.0.0
RUN if [ "$PAASTA_ENV" != "YELP" ]; then cd /tmp && \
        wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
        gdebi -n dh-virtualenv*.deb && \
        rm dh-virtualenv_*.deb; \
    else \
        apt-get install dh-virtualenv=1.0-1; fi

ADD mesos-slave-secret /etc/mesos-slave-secret

WORKDIR /work
