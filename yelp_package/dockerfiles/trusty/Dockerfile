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
RUN apt-get update && apt-get -y install dpkg-dev python-tox python-setuptools \
  python-dev debhelper python-yaml python-pytest pyflakes \
  help2man zsh

# Older versions of dh-virtualenv are buggy and don't.. work
RUN curl http://ppa.launchpad.net/dh-virtualenv/daily/ubuntu/pool/main/d/dh-virtualenv/dh-virtualenv_0.10-0~80~ubuntu14.04.1_all.deb --output dh-virtualenv_0.10-0~80~ubuntu14.04.1_all.deb && \
  dpkg -i dh-virtualenv_0.10-0~80~ubuntu14.04.1_all.deb && rm dh-virtualenv_0.10-0~80~ubuntu14.04.1_all.deb

WORKDIR /work
