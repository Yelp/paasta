deploy_marathon_services (bash script)
======================================

This is a bash script that runs list_marathon_service_instances
and then xargs a list of service.instances to `setup_marathon_job <setup_marathon_job.html>`_,
but only if am_i_mesos_leader returns 0 (the host is the
current leader).

The maximum number of service.instances that ``setup_marathon_job`` takes,
and the maximum number of parallel deployment processes are configurable
in puppet. By default, ``setup_marathon_job`` takes one service.instance
each time and there are at most five parallel ``setup_marathon_job`` processes.

With below config in ``modules/profile_paasta/data/clusters/mesosstage.yaml``,
each ``setup_marathon_job`` in the cluster mesosstage can take 64 service.instances and xargs can fork four
``setup_marathon_job`` process to deploy services in parallel::

  profile_paasta::marathon::max_serviceinstances_per_deployment: 64
  profile_paasta::marathon::max_parallel_deployments: 4

Changes can be made to other clusters accordingly.
