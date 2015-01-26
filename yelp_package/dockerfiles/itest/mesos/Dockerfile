FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Sophie Haskins <shaskins@yelp.com>

RUN apt-get update
RUN apt-get -y install mesos

EXPOSE 5050
CMD LD_LIBRARY_PATH=/usr/lib/jvm/java-7-oracle-1.7.0.15/jre/lib/amd64:/usr/lib/jvm/java-7-oracle-1.7.0.15/jre/lib/amd64/server /usr/local/sbin/mesos-master
