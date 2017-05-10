#!/bin/bash
rm -rf /root/.ssh/*
cp /work/example_cluster/ssh_config /root/.ssh/config
ssh-keygen -f /root/.ssh/id_rsa -N ''
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/*
/usr/sbin/sshd

pip install -e /work/extra-linux-requirements.txt
pip install -e /work
# This is a hack because we're not creating a real package which would create symlinks for the .py scripts
while read link; do echo $link|sed -e 's/opt\/venvs\/paasta-tools\//\/usr\/local\//'| sed -e 's/\ usr/\ \/usr/'| xargs ln -s; done < /work/debian/paasta-tools.links
/usr/sbin/rsyslogd
cron
mesos-master --zk=zk://zookeeper:2181/mesos-testcluster --registry=in_memory --quorum=1 --authenticate --authenticate_slaves --credentials=/etc/mesos-secrets --hostname=$(hostname) &
while true; do
    pserve /work/paasta_tools/api/development.ini --reload
    echo "RESTARTING API IN 5 SECONDS"
    sleep 5
done
