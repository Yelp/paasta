FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Sophie Haskins <shaskins@yelp.com>

RUN apt-get update
RUN apt-get -y install zookeeper

EXPOSE 2181
CMD /usr/share/zookeeper/bin/zkServer.sh start-foreground
