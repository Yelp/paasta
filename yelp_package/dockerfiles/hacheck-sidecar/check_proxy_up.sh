#!/bin/bash
set -e
if [ -f /var/ready ]; then
	exit 0
else
	/opt/venvs/paasta-tools/bin/is_pod_healthy_in_proxy.py $@ && touch /var/ready
	exit $?
fi
