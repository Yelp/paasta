FROM examplecluster_trusty
RUN apt-get update && apt-get install -y openssh-server docker.io curl vim
RUN mkdir -p /var/log/paasta_logs 
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd
RUN mkdir -p /root/.ssh
RUN chmod 700 /root/.ssh
ADD ./mesos_rsa.pub /root/.ssh/authorized_keys
ADD ./mesos_rsa /root/.ssh/id_rsa
ADD ./ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/id_rsa
ADD ./start.sh /start.sh
