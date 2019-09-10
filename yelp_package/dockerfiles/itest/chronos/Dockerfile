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
RUN sed -i 's/archive.ubuntu.com/us-east1.gce.archive.ubuntu.com/g' /etc/apt/sources.list

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        apt-transport-https \
        software-properties-common > /dev/null && \
    echo "deb https://dl.bintray.com/yelp/paasta xenial main" > /etc/apt/sources.list.d/paasta.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 8756C4F765C9AC3CB6B85D62379CE192D401AB61 && \
    echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF && \
    apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
        libsasl2-modules mesos=1.7.2-2.0.1 > /dev/null && \
    apt-get clean

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install \
        -y --no-install-recommends --allow-unauthenticated \
        lsb-release \
        chronos=2.5.0-yelp32-1.ubuntu1604 \
        rsyslog \
        && \
    apt-get clean

# Chronos will look in here for zk config, so we blow away the bogus defaults
RUN rm -rf /etc/mesos/

RUN echo 8081 > /etc/chronos/conf/http_port
RUN echo 'zk://zookeeper:2181/mesos-testcluster' > /etc/chronos/conf/master
RUN echo 'zookeeper:2181' > /etc/chronos/conf/zk_hosts
RUN echo '/chronos' > /etc/chronos/conf/zk_path
RUN echo -n 'chronos' > /etc/chronos/conf/mesos_authentication_principal
RUN echo -n 'secret3' > /etc/chronos_framework_secret
RUN echo -n '/etc/chronos_framework_secret' > /etc/chronos/conf/mesos_authentication_secret_file

CMD rsyslogd ; sleep 1; (/usr/bin/chronos &) ; tail -f /var/log/syslog

EXPOSE 8081
