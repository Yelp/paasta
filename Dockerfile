# This is an example Dockerfile to run your service in PaaSTA!
# It satisfies the PaaSTA contract.

FROM    docker-dev.yelpcorp.com/xenial_yelp:latest

# python and uwsgi deps
RUN     apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            awscli \
            git \
            libatlas-base-dev \
            libpython3.7 \
            libxml2 \
            libyaml-0-2 \
            lsb-release \
            make \
            openssh-client \
            python3.7 \
            python-pip \
            python-setuptools \
            stdin2scribe \
            tox \
            virtualenv \
            zk-flock \
        && apt-get clean

ENV     PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
RUN     /usr/bin/pip install supervisor
COPY    tox.ini requirements.txt requirements-bootstrap.txt extra-requirements-yelp.txt /code/
RUN     cd code && tox -e virtualenv_run
RUN     cd code && virtualenv_run/bin/pip-custom-platform install -rextra-requirements-yelp.txt

RUN     mkdir /home/nobody
ENV     HOME /home/nobody

# Code is COPY'ed here after the pip install above, so that code changes do not
# break the preceding cache layer.
COPY    . /code
RUN     chown nobody /code

# This is needed so that we can pass PaaSTA itests on Jenkins; for some reason (probably aufs-related?)
# root can't modify the contents of /code on Jenkins, even though it works locally.  Root needs to
# modify these contents so that it can configure the Dockerized Mesos cluster that we run our itests on.
# This shouldn't be a security risk because we drop privileges below and on overlay2, root can already
# modify the contents of this directory.
RUN     chmod -R 775 /code/acceptance
RUN     ln -s /code/clusterman/supervisord/fetch_clusterman_signal /usr/bin/fetch_clusterman_signal
RUN     ln -s /code/clusterman/supervisord/run_clusterman_signal /usr/bin/run_clusterman_signal

RUN     install -d --owner=nobody /code/logs

# Create /nail/run to store the batch PID file
RUN     mkdir -p /nail/run && chown -R nobody /nail/run

# For sake of security, don't run your service as a privileged user
USER    nobody
WORKDIR /code
ENV     BASEPATH=/code PATH=/code/virtualenv_run/bin:$PATH
