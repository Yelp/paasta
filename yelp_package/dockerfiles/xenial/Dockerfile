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

ARG IMAGE_NAME=xenial_pkgbuild
FROM ${DOCKER_REGISTRY}${IMAGE_NAME}

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

ARG PAASTA_ENV
ENV PAASTA_ENV=$PAASTA_ENV

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common \
        build-essential \
        debhelper \
        gdebi-core \
        git \
        libffi-dev \
        libgpgme11 \
        libssl-dev \
        libyaml-dev \
        python3.7-dev \
        python-pip \
        wget \
        zsh > /dev/null

# our base images already have python-tox
RUN echo $PAASTA_ENV && if [ "$PAASTA_ENV" != "YELP" ]; then DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python-tox; else true; fi
RUN rm -rf /var/lib/apt/lists/*

RUN if [ "$PAASTA_ENV" != "YELP" ]; then cd /tmp && \
        wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
        gdebi -n dh-virtualenv*.deb && \
        rm dh-virtualenv_*.deb; \
    else \
        apt-get install dh-virtualenv=1.0-1; fi

ADD mesos-slave-secret /etc/mesos-slave-secret

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip==20.0.2
RUN pip install virtualenv==16.7.7
RUN virtualenv --python=python3.7 venv && venv/bin/pip install -r requirements.txt


WORKDIR /work
