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

RUN sed -i 's/archive.ubuntu.com/us-east1.gce.archive.ubuntu.com/g' /etc/apt/sources.list

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common \
        gcc \
        git \
        curl \
        python3.7-dev \
        libffi-dev \
        libssl-dev \
        libyaml-dev \
        virtualenv > /dev/null \
    && apt-get clean > /dev/null

WORKDIR /work

ADD requirements.txt /work/
RUN virtualenv /venv -ppython3.7 --no-download
ENV PATH=/venv/bin:$PATH
RUN pip install -r requirements.txt

COPY yelp_package/dockerfiles/xenial/mesos-slave-secret /etc/
COPY yelp_package/dockerfiles/itest/api/mesos-cli.json yelp_package/dockerfiles/xenial/mesos-slave-secret /nail/etc/
COPY yelp_package/dockerfiles/itest/api/*.json /etc/paasta/

ADD . /work/
RUN pip install -e /work/
