FROM docker-dev.yelpcorp.com/lucid_yelp
LABEL break-cache="1"

RUN apt-get update
RUN apt-get -y install git python2.7 python-virtualenv python-pkg-resources

RUN git clone --branch=yelp git://github.com/Yelp/hacheck
WORKDIR /hacheck
# Until my remote_maint branch gets merged in to our fork, we'll need to merge it here.
# git complains when making merge commits unless you have user.email/user.name defined
RUN git config --global user.email "docker@docker.docker"; git config --global user.name "Docker Docker"
RUN git pull git://github.com/EvanKrall/hacheck remote_maint

RUN virtualenv --python=python2.7 --no-site-packages venv && . venv/bin/activate && pip install -e .

RUN echo 'allow_remote_spool_changes: yes' > /etc/hacheck.yaml

CMD venv/bin/hacheck -p 6666 --config-file /etc/hacheck.yaml
EXPOSE 6666
