FROM docker-dev.yelpcorp.com/bionic_yelp

RUN apt-get update && \
	apt-get install -y --allow-unauthenticated \
	gdebi-core \
	wget \
	python3-distutils # needed by paasta-tools->docker

COPY dist/paasta-tools_0.127.3-yelp1_amd64.deb /tmp/

RUN gdebi -n /tmp/paasta-tools_*.deb && \
	rm /tmp/paasta-tools_*.deb

ENTRYPOINT ["setup_kubernetes_job"]
