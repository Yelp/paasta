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

FROM ubuntu:xenial

# Install packages to allow apt to use a repository over HTTPS
# https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#xenial-1604
RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common && \
    echo "deb https://dl.bintray.com/yelp/paasta xenial main" > /etc/apt/sources.list.d/paasta.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 8756C4F765C9AC3CB6B85D62379CE192D401AB61 && \
    echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF  && \
    rm -rf /var/lib/apt/lists/*

# Need Python 3.6
RUN add-apt-repository ppa:deadsnakes/ppa

# Build Deps
RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        debhelper \
        gdebi-core \
        git \
        libffi-dev \
        libgpgme11 \
        libssl-dev \
        libyaml-dev \
        python3.6-dev \
        python-pip \
        python-tox \
        wget \
        zsh > /dev/null \
    && rm -rf /var/lib/apt/lists/*
RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

# Itest Deps
RUN add-apt-repository ppa:ubuntu-toolchain-r/test && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 9DC858229FC7DD38854AE2D88D81803C0EBFCD88 && \
    apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        libsasl2-modules \
        build-essential \
        sudo \
        lsb-release \
        docker-ce=17.03.2~ce-0~ubuntu-xenial \
        mesos=1.7.2-2.0.1 \
        chronos=2.5.0-yelp32-1.ubuntu1604 \
        marathon=1.4.11-1.0.676.ubuntu1604 \
        rsyslog \
        python2.7-dev \
        virtualenv \
        zookeeper
