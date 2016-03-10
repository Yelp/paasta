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
RUN apt-get update && apt-get -y install libsasl2-modules mesos=0.24.1-0.2.35.ubuntu1404
RUN echo -n '{\n\
  "credentials": [\n\
    {\n\
      "principal": "slave",\n\
      "secret": "secret1"\n\
    },\n\
    {\n\
      "principal": "marathon",\n\
      "secret": "secret2"\n\
    },\n\
    {\n\
      "principal": "chronos",\n\
      "secret": "secret3"\n\
    }\n\
  ]\n\
}' > /etc/mesos-secrets
RUN echo -n '{\n\
  "principal": "slave",\n\
  "secret": "secret1"\n\
}' > /etc/mesos-slave-secret
RUN chmod 600 /etc/mesos-secrets
