FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Kyle Anderson <kwa@yelp.com>

RUN apt-get update && apt-get update

RUN apt-get -y install dpkg-dev python-tox python-setuptools
RUN apt-get -y install python-dev debhelper

# Make sure we get a package suitable for building this package correctly.
# Per dnephin we need https://github.com/spotify/dh-virtualenv/pull/20
# Which at this time is in this package
RUN apt-get -y install dh-virtualenv=0.6-yelp2
RUN apt-get -y install python-yaml
RUN apt-get -y install python-pycurl
RUN apt-get -y install python-pytest
RUN apt-get -y install pyflakes

ENV HOME /work
ENV PWD /work
WORKDIR /work
