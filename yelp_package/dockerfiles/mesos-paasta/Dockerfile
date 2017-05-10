FROM examplecluster_mesosbase
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update > /dev/null && apt-get -y install python-tox python-setuptools \
  python-dev libyaml-dev openssh-server libffi-dev libssl-dev > /dev/null
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir -p /var/log/paasta_logs 
RUN mkdir /var/run/sshd
ADD ./yelp_package/dockerfiles/mesos-paasta/cron.d /etc/cron.d
RUN chmod -R 600 /etc/cron.d
RUN mkdir -p /nail/etc
RUN ln -s /etc/mesos-slave-secret /nail/etc/mesos-slave-secret
RUN ln -s /etc/paasta/mesos-cli.json /nail/etc/mesos-cli.json
ADD ./requirements-dev.txt /paasta/requirements-dev.txt
ADD ./requirements.txt /paasta/requirements.txt
RUN pip install --upgrade pip==9.0.1
RUN pip install --upgrade virtualenv==15.1.0
RUN pip install -r /paasta/requirements-dev.txt
ADD ./yelp_package/dockerfiles/mesos-paasta/start.sh /start.sh
ADD ./yelp_package/dockerfiles/mesos-paasta/start-slave.sh /start-slave.sh
