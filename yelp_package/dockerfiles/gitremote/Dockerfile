FROM ubuntu:trusty
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update > /dev/null && apt-get install -y openssh-server git > /dev/null
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd
RUN cd /root/ && git clone --bare https://github.com/mattmb/dockercloud-hello-world
