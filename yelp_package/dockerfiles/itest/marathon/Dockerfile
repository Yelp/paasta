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


RUN apt-get update > /dev/null && apt-get -y install apt-transport-https > /dev/null

RUN echo "deb https://dl.bintray.com/yelp/paasta trusty main" > /etc/apt/sources.list.d/paasta.list
RUN echo "deb http://repos.mesosphere.com/ubuntu trusty main" > /etc/apt/sources.list.d/mesosphere.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF
RUN apt-get update > /dev/null && apt-get -y install libsasl2-modules mesos=1.2.0-2.0.2 > /dev/null

# Install Java 8 PPA
RUN apt-get install -y software-properties-common > /dev/null
RUN add-apt-repository ppa:webupd8team/java
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv 7B2C3B0889BF5709A105D03AC2518248EEA14886
RUN echo "debconf shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections
RUN echo "debconf shared/accepted-oracle-license-v1-1 seen true" | debconf-set-selections
RUN apt-get update > /dev/null && apt-get -y install lsb-release oracle-java8-installer > /dev/null

RUN apt-get update > /dev/null && apt-get -y install marathon=1.4.3-1.0.649.ubuntu1404 > /dev/null

RUN echo -n "secret2" > /etc/marathon_framework_secret

EXPOSE 8080
