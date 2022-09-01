# Copyright 2015-2017 Yelp Inc.
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
FROM ${DOCKER_REGISTRY}ubuntu:bionic

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

# RUN sed -i 's/archive.ubuntu.com/us-east1.gce.archive.ubuntu.com/g' /etc/apt/sources.list

# Need Python 3.7
RUN apt-get update > /dev/null && \
    apt-get install -y --no-install-recommends curl software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        gcc \
        git \
        curl \
        python3.7-dev \
        libffi-dev \
        libssl-dev \
        libyaml-dev \
        virtualenv > /dev/null \
    && apt-get clean > /dev/null

# Install kubectl
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/`curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt`/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl

WORKDIR /work

ADD requirements.txt /work/
RUN virtualenv /venv -ppython3.7 --no-download
ENV PATH=/venv/bin:$PATH
RUN pip install -r requirements.txt
ADD yelp_package/dockerfiles/itest/k8s/wait_paasta_api.sh /venv/bin

ADD . /work/
RUN pip install -e /work/
RUN pip install pytest
