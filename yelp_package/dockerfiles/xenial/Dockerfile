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
FROM ${DOCKER_REGISTRY}xenial_pkgbuild

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

# RUN sed -i 's/archive.ubuntu.com/us-east1.gce.archive.ubuntu.com/g' /etc/apt/sources.list

RUN echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        debhelper \
        dh-virtualenv \
        gdebi-core \
        git \
        libffi-dev \
        libgpgme11 \
        libssl-dev \
        libyaml-dev \
        python-pip \
        python3.7-dev \
        software-properties-common \
        wget \
        zsh > /dev/null \
    && rm -rf /var/lib/apt/lists/*

ADD mesos-slave-secret /etc/mesos-slave-secret

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip==20.0.2
RUN pip install virtualenv==16.7.7
RUN virtualenv --python=python3.7 venv && venv/bin/pip install -r requirements.txt


WORKDIR /work
