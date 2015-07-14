FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Sophie Haskins <shaskins@yelp.com>

RUN apt-get update
RUN apt-get -y install chronos
RUN apt-get -y install mesos=0.20.1-yelp4
# Chronos will look in here for zk config, so we blow away the bogus defaults
RUN rm -rf /etc/mesos/

RUN echo 8081 > /etc/chronos/conf/http_port
RUN echo 'zk://zookeeper:2181/mesos-testcluster' > /etc/chronos/conf/master
RUN echo 'zookeeper:2181' > /etc/chronos/conf/zk_hosts
RUN echo '/chronos' > /etc/chronos/conf/zk_path

ENV PATH="/usr/lib/jvm/java-7-oracle-1.7.0.60/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
CMD rsyslogd ; (/usr/bin/chronos &) ; tail -f /var/log/messages

EXPOSE 8081
