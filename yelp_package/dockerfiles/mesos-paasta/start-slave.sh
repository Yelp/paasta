#!/bin/bash
/usr/sbin/sshd
mesos-slave --master=zk://zookeeper:2181/mesos-testcluster --resources="cpus(*):10; mem(*):512; disk(*):100" --credential=/etc/mesos-slave-secret --containerizers=docker --docker=/usr/bin/docker --work_dir=/tmp/mesos --attributes="region:fakeregion;pool:default" --no-docker_kill_orphans
