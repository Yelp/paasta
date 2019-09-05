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

FROM yelp/paastatools_xenial_container

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
