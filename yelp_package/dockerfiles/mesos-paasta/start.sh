#!/bin/bash
/setup-ssh.sh
rm -rf /root/.ssh/*
cp /work/example_cluster/ssh_config /root/.ssh/config
ssh-keygen -f /root/.ssh/id_rsa -N ''
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/*
/usr/sbin/sshd

# This is a hack because we're not creating a real package which would create symlinks for the .py scripts
while read link; do echo $link|sed -e 's|opt/venvs/paasta-tools/|/venv/|'| sed -e 's/\ usr/\ \/usr/'| xargs ln -s; done < /work/debian/paasta-tools.links
/usr/sbin/rsyslogd
cron
mesos-master --zk=zk://zookeeper:2181/mesos-testcluster --registry=in_memory --quorum=1 --authenticate --authenticate_slaves --credentials=/etc/mesos-secrets --hostname=$(hostname) &
paasta-deployd &> /var/log/paasta-deployd.log
while true; do
    paasta-deployd &> /var/log/paasta-deployd.log
    echo "paasta-deployd exited, restarting in 5s..."
    sleep 5
done
