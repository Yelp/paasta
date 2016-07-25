FROM examplecluster_trusty
RUN apt-get update && apt-get install -y openssh-server
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd
RUN mkdir -p /root/.ssh
RUN chmod 700 /root/.ssh
ADD ./mesos_rsa.pub /root/.ssh/authorized_keys
ADD ./mesos_rsa /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN cd /root/ && git clone --bare https://github.com/mattmb/dockercloud-hello-world
