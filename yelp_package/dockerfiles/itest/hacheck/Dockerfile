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
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
        git \
        python2.7-dev \
        libyaml-dev \
        virtualenv && \
    apt-get clean

RUN git clone git://github.com/Yelp/hacheck
WORKDIR /hacheck

RUN virtualenv --python=python2.7 venv && venv/bin/pip install -r requirements.txt && venv/bin/pip install -e .

RUN echo 'allow_remote_spool_changes: yes' > /etc/hacheck.yaml

CMD ["venv/bin/hacheck", "-p", "6666", "--config-file", "/etc/hacheck.yaml"]
EXPOSE 6666
