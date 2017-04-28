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

FROM ubuntu:trusty

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get update > /dev/null && apt-get -y install git python2.7 python2.7-dev libyaml-dev python-virtualenv python-pkg-resources > /dev/null

RUN git clone --branch=yelp git://github.com/Yelp/hacheck
WORKDIR /hacheck
# Until my remote_maint branch gets merged in to our fork, we'll need to merge it here.
# git complains when making merge commits unless you have user.email/user.name defined
RUN git config --global user.email "docker@docker.docker"; git config --global user.name "Docker Docker"
RUN git pull git://github.com/EvanKrall/hacheck remote_maint

RUN virtualenv --python=python2.7 --no-site-packages venv && . venv/bin/activate && pip install -e .

RUN echo 'allow_remote_spool_changes: yes' > /etc/hacheck.yaml

CMD venv/bin/hacheck -p 6666 --config-file /etc/hacheck.yaml
EXPOSE 6666
