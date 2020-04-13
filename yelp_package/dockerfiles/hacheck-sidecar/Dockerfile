FROM docker-dev.yelpcorp.com/xenial_yelp
RUN apt-get update && apt-get install -y hacheck python paasta-tools=0.81.14-yelp1
RUN mkdir -p /etc/paasta
ADD ./check_smartstack_up.sh /check_smartstack_up.sh
ADD ./check_proxy_up.sh /check_proxy_up.sh
ADD ./hacheck.conf.yaml /etc/hacheck.conf.yaml
ENTRYPOINT ["/usr/bin/hacheck"]
CMD ["-p", "6666", "-c", "/etc/hacheck.conf.yaml", "--spool-root", "/var/spool/hacheck"]
