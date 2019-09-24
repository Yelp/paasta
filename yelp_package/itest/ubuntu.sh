#!/bin/bash
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

SCRIPTS="am_i_mesos_leader
autoscale_all_services
check_marathon_services_replication
check_kubernetes_api
check_kubernetes_services_replication
check_flink_services_replication
check_cassandracluster_services_replication
cleanup_chronos_jobs
check_chronos_jobs
cleanup_marathon_jobs
paasta_cleanup_tron_namespaces
deploy_chronos_jobs
deploy_marathon_services
paasta_deploy_tron_jobs
generate_deployments_for_service
generate_services_file
generate_services_yaml
list_chronos_jobs
list_marathon_service_instances
paasta_list_tron_namespaces
paasta_autoscale_cluster
paasta_execute_docker_command
paasta_metastatus
paasta_serviceinit
setup_chronos_job
chronos_rerun
setup_marathon_job
paasta_setup_tron_namespace
synapse_srv_namespaces_fact"

MARATHON_SERVICES="fake_service_uno.main
fake_service_dos.niam"

SERVICE_NAMESPACES="fake_service_uno.main
fake_service_uno.canary
fake_service_dos.all_fake"

PAASTA_COMMANDS="list
list-clusters
check
fsm
info
itest
local-run
mark-for-deployment
metastatus
performance-check
push-to-registry
security-check
status
validate"

mkdir -p /nail/etc
[ -L /nail/etc/services ] || ln -s /work/yelp_package/itest/fake_services /nail/etc/services

mkdir -p /etc/paasta
[ -L /etc/paasta/volumes.json ] || ln -s /work/yelp_package/itest/volumes.json /etc/paasta/volumes.json
[ -L /etc/paasta/cluster.json ] || ln -s /work/yelp_package/itest/cluster.json /etc/paasta/cluster.json

# If left there, they are included in the python path and can pollute the tests.
rm -rf debian/paasta-tools/

if dpkg -i /work/dist/"$1"; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi

if ! /opt/venvs/paasta-tools/bin/python -c 'import yaml; assert yaml.__with_libyaml__' >/dev/null; then
  echo "Python doesn't have the C-based yaml loader and will be really slow!"
fi

for scr in $SCRIPTS
do
  which $scr >/dev/null || (echo "$scr failed to install!"; exit 1)
done
setup_marathon_job --help >/dev/null

for srv in $MARATHON_SERVICES
do
  if ! list_marathon_service_instances | grep -q $srv; then
    echo "Service instance $srv ISN'T showing up in list_marathon_service_instances!"
    exit 1
  else
    echo "Service $srv showed up in list_marathon_service_instances"
  fi
done

for ns in $SERVICE_NAMESPACES
do
  if ! synapse_srv_namespaces_fact | grep -q $ns; then
    echo "Service namespace $ns ISN'T showing up in synapse_srv_namespaces_fact!"
    exit 1
  else
    echo "Namespace $ns showed up in synapse_srv_namespaces_fact"
  fi
done

mkdir -p fake_service_uno
remove_fake_service_uno() {
  rm -rf fake_service_uno
}

cd fake_service_uno
for command in $PAASTA_COMMANDS
do
  echo "Running 'paasta $command -h' to make sure it works"
  paasta $command -h >/dev/null || (echo "paasta $command failed to execute!"; exit 1)
done
echo "Running 'paasta --version', it should return non-zero"
paasta --version || (echo "paasta --version failed to execute!"; exit 1)

if check_synapse_replication --help >/dev/null; then
  echo "Looks like we can check_synapse_replication with --help"
else
  echo "Could not invoke check_synapse_replication with --help"
  exit 1
fi

# Test generate_services_yaml

services_yaml=$(mktemp)
remove_services_yaml() {
  rm -f "$services_yaml"
}

trap_handler() {
    remove_services_yaml;
    remove_fake_service_uno;
}
trap trap_handler EXIT

generate_services_yaml "$services_yaml"

for ns in $SERVICE_NAMESPACES
do
    if grep -q $ns "$services_yaml"; then
        echo "Namespace $ns showed up in services.yaml"
    else
        echo "Service namespace $ns ISN'T showing up in services.yaml"
        exit 1
    fi
done

# Tab completion tests
"$(dirname "$0")/tab_complete.sh"


echo "Everything worked!"
