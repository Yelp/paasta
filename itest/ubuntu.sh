#!/bin/bash
set -e

SCRIPTS="setup_marathon_job
am_i_mesos_leader
synapse_srv_namespaces_fact
setup_chronos_jobs
cleanup_marathon_jobs
check_marathon_services_frontends
list_marathon_service_instances
deploy_marathon_services
generate_deployments_json
check_marathon_services_replication"

MARATHON_SERVICES="fake_service_uno.main
fake_service_dos.niam"

SERVICE_NAMESPACES="fake_service_uno.main
fake_service_uno.canary
fake_service_dos.all_fake"

mkdir -p /nail/etc
[ -L /nail/etc/services ] || ln -s /work/itest/fake_services /nail/etc/services

mkdir -p /etc/service_deployment_tools
[ -L /etc/service_deployment_tools/marathon_config.json ] || ln -s /work/itest/marathon_config.json /etc/service_deployment_tools/marathon_config.json

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi

for scr in $SCRIPTS
do
  which $scr >/dev/null || (echo "$scr failed to install!"; exit 1)
done

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

if check_synapse_replication --help >/dev/null; then
  echo "Looks like we can check_synapse_replication with --help"
else
  echo "Could not invoke check_synapse_repkication with --help"
  exit 1
fi

if check_classic_service_replication --help >/dev/null; then
  echo "Looks like we can check_classic_service_replication with --help"
else
  echo "Could not invoke check_classic_service_replication with --help"
  exit 1
fi

echo "Everything worked!"
