ARG DOCKER_REGISTRY=docker-dev.yelpcorp.com/
ARG IMAGE_NAME=bionic_pkgbuild
FROM ${DOCKER_REGISTRY}${IMAGE_NAME}

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        git \
        openssh-server > /dev/null && \
    apt-get clean
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd
RUN cd /root/ && git clone --bare https://github.com/mattmb/dockercloud-hello-world
