FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Kyle Anderson <kwa@yelp.com>

# Make sure we get a package suitable for building this package correctly.
# Per dnephin we need https://github.com/spotify/dh-virtualenv/pull/20
# Which at this time is in this package
RUN apt-get update && apt-get -y install dpkg-dev python-tox python-setuptools python-dev debhelper dh-virtualenv=0.6-yelp2 python-yaml python-pytest pyflakes python2.7 python2.7-dev help2man

ENV HOME /work
ENV PWD /work
WORKDIR /work
