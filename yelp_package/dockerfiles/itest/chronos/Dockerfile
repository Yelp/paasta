FROM docker-dev.yelpcorp.com/lucid_yelp

RUN apt-get update
RUN apt-get -y install oracle-java7-jre

RUN locale-gen en_US.UTF-8
RUN apt-get -y install mesos=0.21.1-yelp1

RUN apt-get -y install chronos
# Chronos will look in here for zk config, so we blow away the bogus defaults
RUN rm -rf /etc/mesos/

RUN echo 8081 > /etc/chronos/conf/http_port
RUN echo 'zk://zookeeper:2181/mesos-testcluster' > /etc/chronos/conf/master
RUN echo 'zookeeper:2181' > /etc/chronos/conf/zk_hosts
RUN echo '/chronos' > /etc/chronos/conf/zk_path

ENV PATH="/usr/lib/jvm/java-7-oracle-1.7.0.60/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
CMD rsyslogd ; (/usr/bin/chronos &) ; tail -f /var/log/messages

EXPOSE 8081
