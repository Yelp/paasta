#!/bin/bash
set -eu
PAASTA_CLUSTER=$1
ZK_CLUSTER_TYPE=${2:-infrastructure}
ALERT_ON_LESS_THAN=${3:-1}

NUM_HOSTS_COMPETING=$(zkcmd --cluster-type ${ZK_CLUSTER_TYPE} --cluster-location ${PAASTA_CLUSTER} -g /mesos-${PAASTA_CLUSTER}/paasta-deployd-leader|tail -1| sed 's/.*numChildren\=//' |sed 's/,.*//')
if [ $NUM_HOSTS_COMPETING -lt $ALERT_ON_LESS_THAN ]; then
	echo "Less than $ALERT_ON_LESS_THAN hosts competing for deployd leadership"
	exit 2
else
	echo "$NUM_HOSTS_COMPETING hosts competing for leadership"
	echo "OK"
fi
