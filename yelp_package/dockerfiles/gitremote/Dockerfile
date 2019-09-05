FROM ubuntu:xenial
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        git \
        openssh-server && \
    apt-get clean
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd
RUN cd /root/ && git clone --bare https://github.com/mattmb/dockercloud-hello-world
