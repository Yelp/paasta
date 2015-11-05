Cooking: How it all comes together
==================================

Build and deploy workflow
-------------------------
`These slides
<https://docs.google.com/a/yelp.com/presentation/d/1mtWoJUVevBrI7I2iCvZRiqKcLZudYLtrLV8kTkdP0jI/edit#>`_
provide a high level overview of the ingredients involved.

Cluster configuration
---------------------
Puppet does the server configuration work: installing packages, configuring
Mesos, scheduling crons to run the deployment scripts, etc. See the
`profile_mesos module
<https://opengrok.yelpcorp.com/xref/sysgit/puppet/modules/profile_mesos/>`_.

Service configuration
---------------------
``soa-configs`` are used to centrally configure services. See the documentation for
`soa-configs <soa_configs.html>`_ for more information on what these are.

Contract with services
----------------------
The `Paasta Contract <http://y/paasta-contract>`_ describes the
responsibilities of services that wish to work with PaaSTA.

paasta_tools contains the implementation of several of these rules.
For example, `generate_deployments_for_service <generate_deployments_for_service.html>`_ is
the piece that checks each service's git repo for the specially-named branch
that tells PaaSTA which versions of the service should go to which clusters.

Deployment
----------
A yelpsoa-configs master runs `generate_deployments_for_service <generated/paasta_tools.generate_deployments_for_service.html>`_
frequently. The generated ``deployments.json`` appears in ``/nail/etc/services/service_name`` throughout the cluster.

Marathon masters run `deploy_marathon_services
<deploy_marathon_services.html>`_, a thin wrapper around `setup_marathon_job
<setup_marathon_job.html>`_. These scripts parse ``deployments.json`` and the
current cluster state, then issue comands to Marathon to put the cluster into
the right state -- cluster X should be running version Y of service Z.

How PaaSTA Runs Docker Containers
---------------------------------
Marathon launches the Docker containers that comprise a PaaSTA service.

Docker images are run by Mesos's native Docker executor. PaaSTA composes the
configuration for the running image:

* ``--attach``: stdout and stderr from running images are sent to logs that end
  up in the Mesos sandbox (currently unavailable).

* ``--cpu-shares``: This is the value set in ``marathon.yaml`` as "cpus".

* ``--memory``: This is the value set in ``marathon.yaml`` as "mem".

* ``--net``: PaaSTA uses bridge mode to enable random port allocation.

* ``--env``: Any environment variables specified in the ``env`` section will be here.

  * **WARNING**: A PORT variable is provided to the docker image, but it represents the EXTERNAL port, not the internal one. The internal service MUST listen on 8888, so this PORT variable confuses some service stacks that are listening for this variable. Such services MUST overwrite this environment variable to function. (``PORT=8888 ./uwisgi.py```)

* ``--publish``: Mesos picks a random port on the host that maps to and exposes
  port 8888 inside the container. This random port is announced to Smartstack
  so that it can be used for load balancing.

* ``--privileged``: Containers run by PaaSTA are not privileged.

* ``--restart``: No restart policy is set on PaaSTA containers. Restarting
  tasks is left as a job for the Framework (Marathon).

* ``--rm``: Mesos containers are rm'd after they finish.

* ``--tty``: Mesos containers are *not* given a tty.

* ``--volume``: Volume mapping is controlled via the paasta_tools
  configuration. This is not user-controlled for security reasons.
  The default mappings include:

  * ``/nail/srv/`` (including `/nail/srv/configs <https://trac.yelpcorp.com/wiki/HowToService/Configuration>`_)

  * ``/nail/etc/services`` (`yelpsoa-configs <https://docs.google.com/a/yelp.com/document/d/1ZBg5ykniRU30UXj4YcsKfmmnuegQbtR2VuqCAIGi-50/edit#bookmark=id.nn2fb0z24rjh>`_)

  * ``/nail/etc/[habitat,datacenter,ecosystem,superregion,etc]`` (Common `location discovery files <https://trac.yelpcorp.com/wiki/Habitat_Datacenter_Ecosystem_Runtimeenv_Region_Superregion>`_)

  * ``/etc/boto_cfg`` (`rotated aws credentials <https://trac.yelpcorp.com/wiki/ztmPage/AutoReloadAWSCreds>`_)

  * ``/var/run/synapse/services`` (`raw service discovery files <https://jira.yelpcorp.com/browse/PAASTA-618>`_)

  * ``/nail/etc/zookeeper_discovery`` (`Zookeeper discovery files <https://docs.google.com/document/d/1Iongm7TSlnd0Zahsa2BoyyR6o2dxNh5AvOetVtJcXho/edit>`_)

  * ``/nail/etc/kafka_discovery`` (`Kafka discovery files <http://servicedocs.yelpcorp.com/docs/yelp_kafka/index.html>`_)


* ``--workdir``: Mesos containers are launched in a temporary "workspace"
  directory on disk. Use the workdir sparingly and try not to output files.

Mesos is the actual system that runs the docker images. In Mesos land these are
called "TASKS". PaaSTA-configured tasks use exponential backoff to prevent
unhealthy tasks from continuously filling up disks and logs -- the more times
that your service has failed to start, the longer Mesos will wait before
trying to start it again.

Mesos *will* healthcheck the task based on the same healthcheck that Smartstack
uses, in order to prune unhealthy tasks. This pruning is less agressive than
smartstack's checking, so a dead task will go DOWN in smartstack before it is
reaped by Mesos.

Time Zones In Docker Containers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Docker containers launched by PaaSTA have whatever time zone is set by the
Dockerfile. If it is not set, the default is the Linux default, **UTC**.

Some code makes assumptions about the underlying time zone a server is in.
In such a situation the time zone should be explicitly set in the Dockerfile.
For example, this line can be placed in a Dockerfile to set the container
to run in US Pacific time::

  RUN ln -fs /usr/share/zoneinfo/US/Pacific /etc/localtime

**Warning**: Forcing a time zone like this is not advised, as Docker containers
could potentially be launched in many geographic locations. Ideally code
should *not* make assumptions about the local time zone setting of a server.

Bouncing
--------
PaaSTA supports pluggable bounce_methods to give service authors a choice
on how to handle the transition between new and old versions of as service.

There are four bounce methods available:

* `brutal <bounce_lib.html#bounce_lib.brutal_bounce>`_ - Stops old versions and
  starts the new version, without regard to safety. Not recommended for most
  use cases; it's mostly for debugging, but this is probably the fastest bounce
  method.
* `upthendown <bounce_lib.html#bounce_lib.upthendown_bounce>`_ - Brings up the
  new version of the service and waits until all instances are healthy before
  stopping the old versions. May be useful for services that need a quorum of
  the new version. During a bounce, your service will have up to twice as many
  instances running, so it will up to twice as many cluster resources as usual.
* `downthenup <bounce_lib.html#bounce_lib.downthenup_bounce>`_ - Stops any old
  versions and waits for them to die before starting the new version. May be
  useful for services without strict uptime requirements (log tailers, queue
  workers) that do not want more than one version running at a time.
* `crossover <bounce_lib.html#bounce_lib.crossover_bounce>`_ - Starts the new
  version, and gradually kills instances of the old versions as new instances
  become healthy. The code behind this is more complex than the other methods,
  but this is recommended for most use cases. It provides good safety (will not
  take your old instances down if your new version doesn't pass healthchecks)
  but does not consume as many resources as ``upthendown``.

A service author can select a bounce method by setting ``bounce_method`` in
the marathon configuration file. (e.g. ``marathon-SHARED.yaml``) This setting
is set per-instance. If not set, it will default to the ``crossover`` method.
See the docs on the `marathon config <yelpsoa_configs.html#marathon-clustername-yaml>_` file.

Additionally, a service author can configure how the bounce code determines
which instances are healthy by setting ``bounce_health_params``. This
dictionary is passed in as keyword arguments to `get_happy_tasks <bounce_lib.html#bounce_lib.get_happy_tasks>`_.
Valid options are:

* ``min_task_uptime``: Minimum number of seconds that a task must be running
  before we consider it healthy. Useful if tasks take a while to start up.
* ``check_haproxy``: Whether to check the local haproxy to make sure this task
  has been registered and discovered.

Monitoring
----------

PaaSTA gives you a few `Sensu <https://sensuapp.org/docs/latest/>`_-powered
monitoring checks for free:

* `setup_marathon_job <generated/paasta_tools.setup_marathon_job.html#module-paasta_tools.setup_marathon_job>`_:
  Alerts when a Marathon service cannot be deployed or bounced for some reason.
  It will resolve when a service has been successfully deployed/bounced.

* `check_marathon_services_replication <generated/paasta_tools.check_marathon_services_replication.html>`_:
  runs periodically and sends an alert if fewer than 50% of the requested
  instances are deployed on a cluster. If the service is registered in Smartstack
  it will look in Smartstack to count the available instances. Otherwise it
  counts the number of healthy tasks in Mesos.


Cleanup
-------
`cleanup_marathon_jobs <cleanup_marathon_jobs.html>`_ gets rid of Marathon jobs
that don't need to be running anymore. This should be rare, like if you change
a service's name or manually delete a ``paasta-[clustername]`` git branch, but
is a useful safety net in case a task escapes.
