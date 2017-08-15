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

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        apt-transport-https \
        software-properties-common > /dev/null && \
    echo "deb https://dl.bintray.com/yelp/paasta xenial main" > /etc/apt/sources.list.d/paasta.list && \
    echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF && \
    apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
        libsasl2-modules mesos=1.3.0-2.0.3 > /dev/null && \
    apt-get clean

RUN add-apt-repository ppa:webupd8team/java && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 7B2C3B0889BF5709A105D03AC2518248EEA14886 && \
    echo "debconf shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections && \
    echo "debconf shared/accepted-oracle-license-v1-1 seen true" | debconf-set-selections && \
    apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        lsb-release \
        marathon=1.4.6-1.0.656.ubuntu1604 \
        oracle-java8-installer > /dev/null && \
    apt-get clean

RUN echo -n "secret2" > /etc/marathon_framework_secret

EXPOSE 8080
