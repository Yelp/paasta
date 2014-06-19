FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Sophie Haskins <shaskins@yelp.com>

RUN apt-get update
RUN apt-get -y install chronos
RUN apt-get -y install mesos

ADD etc-default-chronos /etc/default/chronos
ADD chronos.sh /usr/bin/chronos
RUN chmod +x /usr/bin/chronos

EXPOSE 8080

CMD JAVA_LIBPATH="/usr/local/lib" /usr/bin/chronos --master zk://zookeeper:2181/mesos --zk_hosts zookeeper:2181
