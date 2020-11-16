FROM example_cluster_itest_xenial

ARG PIP_INDEX_URL=https://pypi.yelpcorp.com/simple
ENV PIP_INDEX_URL=$PIP_INDEX_URL

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl \
        docker.io \
        jq \
        openssh-server \
        vim > /dev/null && \
    apt-get clean

RUN mkdir -p /var/log/paasta_logs /var/run/sshd /nail/etc
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN ln -s /etc/paasta/mesos-cli.json /nail/etc/mesos-cli.json

ADD requirements.txt requirements-dev.txt requirements-bootstrap.txt /paasta/
RUN virtualenv /venv -ppython3.6
ENV PATH=/venv/bin:$PATH
RUN pip install -r /paasta/requirements-bootstrap.txt
RUN pip install -r /paasta/requirements.txt

ADD ./yelp_package/dockerfiles/playground/start.sh /start.sh
ADD ./yelp_package/dockerfiles/playground/setup-ssh.sh /setup-ssh.sh
