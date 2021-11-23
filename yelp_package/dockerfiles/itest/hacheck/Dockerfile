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
FROM ${DOCKER_REGISTRY}ubuntu:bionic

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

RUN sed -i 's/archive.ubuntu.com/us-east1.gce.archive.ubuntu.com/g' /etc/apt/sources.list

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
        git \
        python3.7-dev \
        libyaml-dev \
        virtualenv > /dev/null && \
    apt-get clean

RUN git clone git://github.com/Yelp/hacheck
WORKDIR /hacheck

RUN virtualenv --python=python3.7 venv && \
    venv/bin/pip install setuptools==53.0.0 && \
    venv/bin/pip install -r requirements.txt && \
    venv/bin/pip install -e .

RUN echo 'allow_remote_spool_changes: yes' > /etc/hacheck.yaml

CMD ["venv/bin/hacheck", "-p", "6666", "--config-file", "/etc/hacheck.yaml"]
EXPOSE 6666
