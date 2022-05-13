FROM docker-dev.yelpcorp.com/bionic_yelp

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL


RUN apt-get update && apt-get install -y python3.7 python3-distutils paasta-tools=0.127.3-yelp1

RUN mkdir -p /etc/paasta
ENTRYPOINT ["/usr/bin/paasta-api"]
CMD ["5054"]
