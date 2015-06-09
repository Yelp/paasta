FROM docker-dev.yelpcorp.com/lucid_yelp

RUN apt-get update
RUN apt-get -y install marathon=0.8.2-yelp1.ubuntu1204

RUN apt-get -y install oracle-java7-jre oracle-java7-jdk
RUN locale-gen en_US.UTF-8
RUN apt-get -y install mesos

RUN mkdir -p /etc/marathon/conf/
RUN echo 'local' > /etc/marathon/conf/master
RUN echo 'zk://zookeeper:2181/marathon' > /etc/marathon/conf/zk

EXPOSE 8080
