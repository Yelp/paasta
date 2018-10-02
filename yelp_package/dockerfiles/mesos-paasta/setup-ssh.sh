#!/bin/bash
rm -rf /root/.ssh/*
cp /work/example_cluster/ssh_config /root/.ssh/config
ssh-keygen -f /root/.ssh/id_rsa -N ''
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/*
