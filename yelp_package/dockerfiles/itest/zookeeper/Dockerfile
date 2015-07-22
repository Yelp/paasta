FROM docker-dev.yelpcorp.com/lucid_yelp

RUN apt-get update
RUN apt-get -y install oracle-java7-jre
RUN apt-get -y install zookeeper

EXPOSE 2181
CMD /usr/share/zookeeper/bin/zkServer.sh start-foreground
