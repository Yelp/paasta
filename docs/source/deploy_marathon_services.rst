deploy_marathon_services (bash script)
======================================

This is a bash script that runs list_marathon_service_instances
and then xargs a list of service.instances to ``setup_marathon_job``,
but only if am_i_mesos_leader returns 0 (the host is the
current leader).

How does setup_marathon_job work
--------------------------------

``setup_marathon_job`` iterates over a list of service.instances to avoid
fetching the mesos master state on each service.instance. Each service.instance
is called an app in ``setup_marathon_job``. Since apps are independent to
each other, it won't be a problem to cache the mesos master state which may
become stale when apps are created or destroyed while ``setup_marathon_job``
makes progress.

``setup_marathon_job`` grabs a zookeeper lock for each app it tries to
deploy. Locking is not necessary if `deploy_marathon_services <deploy_marathon_services.html>`_
is the only source of app deployment. It provides safety against another
``setup_marathon_job`` being launched, e.g. manually by operations. When
an app being created or destroyed at marathon, another zookeeper lock is
acquired to provide protection against other app addition/removal activities,
e.g. the ``cleanup_marathon_jobs`` tool.

It takes multiple ``setup_marathon_job`` runs to advance an app from its
current state to the desired state. For example, below steps occur in a
crossover bounce in separate ``setup_marathon_job`` runs:

* The new app is created with marathon and marathon is working to launch
  the required number of tasks.
* As soon as tasks of the new app are up and running, tasks of the previous
  app can begin draining. In an upthendown bounce, all tasks of the new app
  need to be up before tasks of the previous app can start to drain.
* Tasks of the previous app can now be killed if they have been draining
  for ``delay`` seconds so that the load balancer have stopped traffic to
  these tasks. The old app is removed from marathon when all of its tasks
  have been killed.

``setup_marathon_job`` skips above steps when an app have reached its
desired state.
