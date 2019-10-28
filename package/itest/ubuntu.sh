#!/bin/bash

set -euxo pipefail

cd /

highlight() {
  echo -n "$(tput setaf 3)"
  echo -n "$@"
  echo "$(tput op)"
}

highlight_exec() {
  highlight "$@"
  command "$@"
  return $?
}

PACKAGE_NAME="$1"
PACKAGE_VERSION="$2"

# This will get DISTRIB_CODENAME
source /etc/lsb-release
export DISTRIB_CODENAME
echo $PACKAGE_NAME $PACKAGE_VERSION $DISTRIB_CODENAME

# Set up the timezone so clusterman_metrics gets the right data
export TZ=US/Pacific
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

apt-get update && apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa && apt-get update
apt-get install -y --force-yes python3.7 python3-pip python3-yaml awscli
dpkg -i /dist/${DISTRIB_CODENAME}/clusterman_${PACKAGE_VERSION}_amd64.deb || true
apt-get install -y --force-yes --fix-broken

export ACCEPTANCE_ROOT=/itest
pip3 install boto3 simplejson
python3 /itest/run_instance.py

# Run the critical clusterman CLI commands
highlight_exec /usr/bin/clusterman --version
highlight_exec /usr/bin/clusterman status --cluster docker -v
highlight_exec /usr/bin/clusterman manage --cluster docker --target-capacity 10 --dry-run
highlight_exec /usr/bin/clusterman disable --cluster docker --until tomorrow
highlight_exec /usr/bin/clusterman enable --cluster docker
highlight_exec /usr/bin/clusterman simulate --cluster docker --start-time 2017-12-01T08:00:00Z --end-time 2017-12-01T09:00:00Z --metrics-data-files /itest/metrics.json.gz
highlight_exec /usr/bin/clusterman --log-level debug simulate --cluster docker --scheduler mesos --autoscaler-config /itest/autoscaler_config.yaml --start-time 2017-12-01T08:00:00Z --end-time 2017-12-01T09:00:00Z --metrics-data-files /itest/metrics.json.gz

highlight "$0:" 'success!'
