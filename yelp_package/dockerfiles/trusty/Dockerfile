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

FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Kyle Anderson <kwa@yelp.com>

RUN apt-get update && apt-get update

RUN apt-get -y install dpkg-dev python-tox python-setuptools
RUN apt-get -y install python-dev debhelper

# Make sure we get a package suitable for building this package correctly.
# Per dnephin we need https://github.com/spotify/dh-virtualenv/pull/20
# Which at this time is in this package
RUN apt-get -y install dh-virtualenv=0.6-yelp2
RUN apt-get -y install python-yaml
RUN apt-get -y install python-pytest
RUN apt-get -y install pyflakes
RUN apt-get -y install help2man

ENV HOME /work
ENV PWD /work
WORKDIR /work
