FROM docker-dev.yelpcorp.com/lucid_yelp
LABEL break-cache="1"

RUN apt-get update
RUN apt-get -y install git python2.7 python-virtualenv python-pkg-resources

RUN git clone --branch=remote_maint git://github.com/EvanKrall/hacheck
WORKDIR /hacheck
RUN git config --global user.email "docker@docker.docker"; git config --global user.name "Docker Docker"
RUN git pull git://github.com/Yelp/hacheck yelp

RUN virtualenv --python=python2.7 --no-site-packages venv && . venv/bin/activate && pip install -e .

RUN echo 'allow_remote_spool_changes: yes' > /etc/hacheck.yaml

CMD venv/bin/hacheck -p 6666 --config-file /etc/hacheck.yaml
EXPOSE 6666
