FROM docker-dev.yelpcorp.com/bionic_yelp
ARG HACHECK_VERSION=0.18.2-yelp1

RUN apt-get update && \
    apt-get install -y hacheck=${HACHECK_VERSION} paasta-tools python3-distutils && \
    mkdir -p /etc/paasta

ADD ./check_smartstack_up.sh /check_smartstack_up.sh
ADD ./check_proxy_up.sh /check_proxy_up.sh
ADD ./hacheck.conf.yaml /etc/hacheck.conf.yaml

ENTRYPOINT ["/usr/bin/hacheck"]
CMD ["-p", "6666", "-c", "/etc/hacheck.conf.yaml", "--spool-root", "/var/spool/hacheck"]
