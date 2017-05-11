FROM ubuntu:trusty

RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        debhelper dpkg-dev gcc gdebi-core git help2man libffi-dev \
        libssl-dev libsasl2-modules libyaml-dev pyflakes python-dev python-pip python-pytest \
        python-setuptools python-tox python-yaml wget zip zsh \
        openssh-server docker.io curl vim jq libsvn-dev \
    && apt-get clean

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd

RUN pip install --upgrade pip==9.0.1
RUN pip install --upgrade virtualenv==15.1.0

COPY ./requirements.txt /tmp/
COPY ./requirements-dev.txt /tmp/
COPY ./task_processing/examples/cluster/mesos-0.27.0-py2-none-any.whl /tmp/
COPY ./task_processing/examples/cluster/mesos.cli-0.27.0-py2-none-any.whl /tmp/
COPY ./task_processing/examples/cluster/mesos.interface-0.27.0-py2-none-any.whl /tmp/
COPY ./task_processing/examples/cluster/mesos.native-0.27.0-cp27-none-linux_x86_64.whl /tmp/

RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/requirements-dev.txt
RUN pip install -i \
      /tmp/mesos-0.27.0-py2-none-any.whl /tmp/mesos.cli-0.27.0-py2-none-any.whl \
      /tmp/mesos.interface-0.27.0-py2-none-any.whl /tmp/mesos.native-0.27.0-cp27-none-linux_x86_64.whl

ENV PYTHONPATH=/src
WORKDIR /src

CMD /bin/bash
