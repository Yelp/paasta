FROM docker-dev.yelpcorp.com/bionic_yelp

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

RUN apt-get update && apt-get install -y python paasta-tools=0.97.72-yelp1
RUN mkdir -p /etc/paasta
ENTRYPOINT ["/usr/bin/setup_kubernetes_job"]
CMD []
