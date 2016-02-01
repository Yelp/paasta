# Copyright 2015 Yelp Inc.
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

FROM ubuntu:trusty
RUN echo "deb http://repos.mesosphere.com/ubuntu trusty main" > /etc/apt/sources.list.d/mesosphere.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF
RUN apt-get update && apt-get -y install mesos=0.23.1-0.2.61.ubuntu1404

RUN apt-get -y install chronos=2.4.0-0.1.20151007110204.ubuntu1404
# Chronos will look in here for zk config, so we blow away the bogus defaults
RUN rm -rf /etc/mesos/

RUN echo 8081 > /etc/chronos/conf/http_port
RUN echo 'zk://zookeeper:2181/mesos-testcluster' > /etc/chronos/conf/master
RUN echo 'zookeeper:2181' > /etc/chronos/conf/zk_hosts
RUN echo '/chronos' > /etc/chronos/conf/zk_path

CMD rsyslogd ; (/usr/bin/chronos &) ; tail -f /var/log/syslog

EXPOSE 8081
