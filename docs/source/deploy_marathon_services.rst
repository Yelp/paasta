deploy_marathon_services (bash script)
======================================

This is a bash script that runs list_marathon_service_instances
and then xargs each service instance to setup_marathon_job,
but only if am_i_mesos_leader returns 0 (the host is the
current leader). 
