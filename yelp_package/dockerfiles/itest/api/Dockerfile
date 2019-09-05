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

FROM yelp/paastatools_xenial_container

WORKDIR /work

ADD requirements.txt /work/
RUN virtualenv /venv -ppython3.6
ENV PATH=/venv/bin:$PATH
RUN pip install -r requirements.txt

COPY yelp_package/dockerfiles/xenial/mesos-slave-secret /etc/
COPY yelp_package/dockerfiles/itest/api/mesos-cli.json yelp_package/dockerfiles/xenial/mesos-slave-secret /nail/etc/
COPY yelp_package/dockerfiles/itest/api/*.json /etc/paasta/

ADD . /work/
RUN pip install -e /work/
