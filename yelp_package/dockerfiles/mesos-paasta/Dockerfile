FROM examplecluster_mesosbase
RUN apt-get update && apt-get -y install python-tox python-setuptools \
  python-dev libyaml-dev openssh-server
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir -p /var/log/paasta_logs 
RUN mkdir /var/run/sshd
RUN mkdir -p /root/.ssh
RUN chmod 700 /root/.ssh
ADD ./mesos_rsa.pub /root/.ssh/authorized_keys
ADD ./mesos_rsa /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
ADD ./cron.d /etc/cron.d
RUN mkdir -p /nail/etc
RUN ln -s /etc/mesos-slave-secret /nail/etc/mesos-slave-secret
ADD ./start.sh /start.sh
