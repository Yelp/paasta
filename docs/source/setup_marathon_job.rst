setup_marathon_job
======================================

``setup_marathon_job`` iterates over a list of service.instances to take
advantage of the installed requests_cache to avoid fetching the mesos
master state on each service.instance. Each service.instance is called
an app in ``setup_marathon_job``. Since apps are independent to each other,
it won't be a problem to cache the mesos master state which may become
stale when apps are created or destroyed while ``setup_marathon_job`` is
making progress.

``setup_marathon_job`` grabs a zookeeper lock for each app while it is
being deployed. Locking is not necessary if `deploy_marathon_services <deploy_marathon_services.html>`_
is the only source of service deployment. It provides safety against another
``setup_marathon_job`` being launched, e.g. manually by a system admin.

It takes multiple ``setup_marathon_job`` runs to advance an app from its
current state to the desired state. For example, below steps occur in
sequential ``setup_marathon_job`` runs in a crossover bounce:

* The new app is created with marathon and marathon is working to launch
  the required number of tasks.
* As soon as tasks of the new app are up and running, tasks of the previous
  app can begin to drain. In a upthendown bounce, all tasks of the new app
  need to be up before tasks of the previous app can start to drain.
* Tasks of the previous app can now be killed if they have been draining
  for ``delay`` seconds so that the load balancer have stopped traffic to
  these tasks. The old app is removed from marathon when all of its tasks
  have been killed.

``setup_marathon_job`` skips above steps when an app is at its desired state.
