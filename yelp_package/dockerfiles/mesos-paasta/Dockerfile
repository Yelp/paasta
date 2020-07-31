FROM example_cluster_mesosbase

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        libffi-dev \
        libssl-dev \
        libyaml-dev \
        python-pip \
        python3.6-dev \
        openssh-server > /dev/null && \
    apt-get clean

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir -p /var/log/paasta_logs /var/run/sshd
RUN mkdir -p /nail/etc
RUN ln -s /etc/mesos-slave-secret /nail/etc/mesos-slave-secret
RUN ln -s /etc/paasta/mesos-cli.json /nail/etc/mesos-cli.json

ADD requirements.txt requirements-dev.txt requirements-bootstrap.txt /paasta/
RUN pip install virtualenv==15.1.0
RUN virtualenv /venv -ppython3.6
ENV PATH=/venv/bin:$PATH
RUN pip install -r /paasta/requirements-bootstrap.txt
RUN pip install -r /paasta/requirements.txt

ADD ./yelp_package/dockerfiles/mesos-paasta/cron.d /etc/cron.d
RUN chmod -R 600 /etc/cron.d
ADD ./yelp_package/dockerfiles/mesos-paasta/start.sh /start.sh
ADD ./yelp_package/dockerfiles/mesos-paasta/setup-ssh.sh /setup-ssh.sh
ADD ./yelp_package/dockerfiles/mesos-paasta/start-slave.sh /start-slave.sh
