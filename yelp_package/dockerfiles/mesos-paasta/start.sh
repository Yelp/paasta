#!/bin/bash
rm -rf /root/.ssh/*
cp /work/example_cluster/ssh_config /root/.ssh/config
ssh-keygen -f /root/.ssh/id_rsa -N ''
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/*

/usr/sbin/sshd
if [ ! -f /var/tmp/pip_cache/built_wheels ]; then
    pip wheel /work --wheel-dir=/var/tmp/pip_cache
    touch /var/tmp/pip_cache/built_wheels
fi
pip install --no-index --find-links=/var/tmp/pip_cache -e /work
# This is a hack because we're not creating a real package which would create symlinks for the .py scripts
while read link; do echo $link|sed -e 's/usr\/share\/python\/paasta-tools\//\/usr\/local\//'| sed -e 's/\ usr/\ \/usr/'| xargs ln -s; done < /work/debian/paasta-tools.links
/usr/sbin/rsyslogd
cron
mesos-master --zk=zk://zookeeper:2181/mesos-testcluster --registry=in_memory --quorum=1 --authenticate --authenticate_slaves --credentials=/etc/mesos-secrets --hostname=$(hostname)
