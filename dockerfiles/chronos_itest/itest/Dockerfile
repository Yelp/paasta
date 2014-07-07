FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Sophie Haskins <shaskins@yelp.com>

RUN apt-get update
RUN apt-get -y install chronos
RUN apt-get -y install rbenv-ruby-1.9.3-p448

RUN apt-get -y install dpkg-dev python-tox python-setuptools
RUN apt-get -y install python-dev debhelper

# Make sure we get a package suitable for building this package correctly.
# Per dnephin we need https://github.com/spotify/dh-virtualenv/pull/20
# Which at this time is in this package
RUN apt-get -y install dh-virtualenv=0.6-yelp2

# Grrrr
RUN ln -s /usr/bin/python /usr/local/bin/python

RUN apt-get -y install python-yaml
RUN apt-get -y install python-pycurl
RUN apt-get -y install python-pytest
RUN apt-get -y install pyflakes
