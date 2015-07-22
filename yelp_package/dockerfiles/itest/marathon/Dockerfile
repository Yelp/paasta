FROM docker-dev.yelpcorp.com/lucid_yelp

RUN apt-get update
RUN apt-get -y install oracle-java7-jre

RUN locale-gen en_US.UTF-8
RUN apt-get -y install mesos=0.20.1-yelp4

RUN apt-get -y install marathon=0.8.2-yelp1.ubuntu1204

EXPOSE 8080
