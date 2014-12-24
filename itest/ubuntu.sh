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
check_marathon_services_replication
generate_services_yaml
cleanup_marathon_orphaned_containers"

MARATHON_SERVICES="fake_service_uno.main
fake_service_dos.niam"

SERVICE_NAMESPACES="fake_service_uno.main
fake_service_uno.canary
fake_service_dos.all_fake"

PAASTA_COMMANDS="list
check
generate-pipeline
help
status
version"

mkdir -p /nail/etc
[ -L /nail/etc/services ] || ln -s /work/itest/fake_services /nail/etc/services

mkdir -p /etc/paasta_tools
[ -L /etc/paasta_tools/marathon_config.json ] || ln -s /work/itest/marathon_config.json /etc/paasta_tools/marathon_config.json

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

mkdir fake_service_uno
remove_fake_service_uno() {
  rm -rf fake_service_uno
}

(
  cd fake_service_uno
  for command in $PAASTA_COMMANDS
  do
    paasta $command -h >/dev/null || (echo "paasta $command failed to execute!"; exit 1)
  done
)

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

if cleanup_marathon_orphaned_containers --help >/dev/null; then
  echo "Looks like we can cleanup_marathon_orphaned_containers with --help"
else
  echo "Could not invoke cleanup_marathon_orphaned_containers with --help"
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


echo "Everything worked!"
