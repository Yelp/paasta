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

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        apt-transport-https \
        software-properties-common && \
    echo "deb https://dl.bintray.com/yelp/paasta xenial main" > /etc/apt/sources.list.d/paasta.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 8756C4F765C9AC3CB6B85D62379CE192D401AB61 && \
    echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
        libsasl2-modules mesos=1.7.2-2.0.1 && \
    apt-get clean

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        lsb-release \
        marathon=1.4.11-1.0.676.ubuntu1604 \
        && \
    apt-get clean

RUN echo -n "secret2" > /etc/marathon_framework_secret

EXPOSE 8080
