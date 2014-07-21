#!/bin/bash
set -e

SCRIPTS="setup_marathon_job
am_i_mesos_leader
synapse_srv_namespaces_fact
setup_chronos_jobs
cleanup_marathon_jobs
check_marathon_services_http_frontends
list_marathon_service_instances"

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
  fi
done

for ns in $SERVICE_NAMESPACES
do
  if ! synapse_srv_namespaces_fact | grep -q $ns; then
    echo "Service namespace $ns ISN'T showing up in synapse_srv_namespaces_fact!"
    exit 1
  fi
done

echo "Everything worked!"