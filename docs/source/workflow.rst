Cooking: How it all comes together
==================================

Ways That PaaSTA Can Run Services
---------------------------------

Long Running Services
^^^^^^^^^^^^^^^^^^^^^

Long running services are are processes that are expected to run continuously
and usually have the same process id throughout. PaaSTA uses
`Marathon <yelpsoa_configs.html#marathon-clustername-yaml>`_ to configure how these
services should run.

These services often serve network traffic, usually HTTP. PaaSTA integrates with
SmartStack to make it easy for others to discover these long running services, or
operators can use alternative service discovery methods to interact with them.

Alternative names: workers, daemons, batch daemons, servers

Scheduled Jobs
^^^^^^^^^^^^^^

Scheduled tasks are those tasks that are periodically run, and are not expected
to run continuously. Due to their ephemeral nature, they often do not expose a TCP port.

You can schedule jobs on PaaSTA using `Tron <yelpsoa_configs.html#tron-tron-clustername-yaml>`_.

Adhoc Tasks
^^^^^^^^^^^

Adhoc tasks are often required to support one-time tasks, like a database
migration.  Sometimes they are also useful for exploratory purposes, or even
sometimes for end-to-end tests. PaaSTA supports this use case through ``paasta
local-run``, which supports building images locally, as well as using the same
image as a "live" deployment.

See the `adhoc instances <adhoc_instances.html>`_ doc for more information on
running adhoc instances using ``paasta local-run``.

Alternative names: Adhoc batches, interactive batches, one-off dynos, one-off
tasks

Service configuration
---------------------
``soa-configs`` are used to centrally configure services. See the documentation for
`soa-configs <soa_configs.html>`_ for more information on what these are.

Contract with services
----------------------
The `Paasta Contract <about/contract.html>`_ describes the
responsibilities of services that wish to work with PaaSTA.

paasta_tools contains the implementation of several of these rules.
For example, `generate_deployments_for_service <generate_deployments_for_service.html>`_ is
the piece that checks each service's git repo for the specially-named branch
that tells PaaSTA which versions of the service should go to which clusters.

Deployment
----------
A yelpsoa-configs master runs `generate_deployments_for_service <generated/paasta_tools.generate_deployments_for_service.html>`_
frequently. The generated ``deployments.json`` appears in ``/nail/etc/services/service_name`` throughout the cluster.

Marathon masters run `deploy_marathon_services <deploy_marathon_services.html>`_,
a thin wrapper around ``setup_marathon_job``.
These scripts parse ``deployments.json`` and the current cluster state,
then issue commands to Marathon to put the cluster into the right state
-- cluster X should be running version Y of service Z.

How PaaSTA Runs Docker Containers
---------------------------------
Marathon launches the Docker containers that comprise a PaaSTA service.

Docker images are run by Mesos's native Docker executor. PaaSTA composes the
configuration for the running image:

* ``--attach``: stdout and stderr from running images are sent to logs that end
  up in the Mesos sandbox (currently unavailable).

* ``--cpu-shares``: This is the value set in ``marathon.yaml`` as "cpus".

* ``--memory``: This is the value set in ``marathon.yaml`` as "mem".

* ``--memory-swap``: Total memory limit (memory + swap). We set this to the same value
  as "mem", rounded up to the nearest MB, to prevent containers being able to swap.

* ``--net``: PaaSTA uses bridge mode to enable random port allocation.

* ``--env``: Any environment variables specified in the ``env`` section will be here. Additional
  ``PAASTA_``, ``MARATHON_``, and ``MESOS_`` environment variables will also be injected, see the
  `related docs <yelpsoa_configs.html#env>`_ for more information.

* ``--publish``: Mesos picks a random port on the host that maps to and exposes
  port 8888 inside the container. This random port is announced to Smartstack
  so that it can be used for load balancing.

* ``--privileged``: Containers run by PaaSTA are not privileged.

* ``--restart``: No restart policy is set on PaaSTA containers. Restarting
  tasks is left as a job for the Framework (Marathon).

* ``--rm``: Mesos containers are rm'd after they finish.

* ``--tty``: Mesos containers are *not* given a tty.

* ``--volume``: Volume mapping is controlled via the paasta_tools
  configuration. PaaSTA uses the volumes declared in ``/etc/paasta/volumes.json``
  as well as per-service volumes declared in ``extra_volumes`` declared
  in the `soa-configs <yelpsoa_configs.html#marathon-clustername-yaml>`_.

* ``--workdir``: Mesos containers are launched in a temporary "workspace"
  directory on disk. Use the workdir sparingly and try not to output files.

Mesos is the actual system that runs the docker images. In Mesos land these are
called "TASKS". PaaSTA-configured tasks use exponential backoff to prevent
unhealthy tasks from continuously filling up disks and logs -- the more times
that your service has failed to start, the longer Mesos will wait before
trying to start it again.

Mesos *will* healthcheck the task based on the same healthcheck that SmartStack
uses, in order to prune unhealthy tasks. This pruning is less aggressive than
SmartStack's checking, so a dead task will go DOWN in SmartStack before it is
reaped by Marathon. By default the healthchecks occur every 10 seconds, and a service
must fail 30 times before that task is pruned and a new one is launched in its place.
This means a task had 5 minutes by default to properly respond to its healthchecks.

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
Bouncing in this context refers to how PaaSTA deploys new version of a
service or handles change in its configuration. (soa-config changes: memory,
cpu, environment variables, etc).

See the dedicated `bounce docs <bouncing.html>`_ on how PaaSTA does this
in a safe (by default) manner.

Draining
--------
Draining is the process to stop instances of an old service from receiving
traffic. PaaSTA supports pluggable drain methods for service authors to mark
services up and down in their environments.

Current master has three draining methods:

* `noop <generated/paasta_tools.drain_lib.html#drain_lib.NoopDrainMethod>`_ - This draining method skips
  draining completely. Service instances are killed as needed.

* `test <generated/paasta_tools.drain_lib.html#drain_lib.TestDrainMethod>`_ - This draining method uses
  class variables to keep track of instances that are marked down to drain and
  instances that have stopped receiving traffic.

* `hacheck <generated/paasta_tools.drain_lib.html#drain_lib.HacheckDrainMethod>`_ - `hacheck <https://github.com/Yelp/hacheck>`_ is
  used at Yelp to provide APIs to query and change state of a service instance.
  The hacheck draining method requests hacheck to mark down an instance. HAProxy
  checks with hacheck periodically to keep its view of instance state up-to-date.
  The hacheck draining method will wait for a configurable ``delay`` to make sure
  HAProxy has the update before considering safe to kill an instance. Note that
  the hacheck draining method sets an expiration when marking an instance down on
  hacheck. hacheck will drop the down state if it receives a status query after
  expiration.

Monitoring
----------

PaaSTA gives you a few `Sensu <https://docs.sensu.io/sensu-go/latest/>`_-powered
monitoring checks for free:

* `setup_marathon_job <generated/paasta_tools.setup_marathon_job.html#module-paasta_tools.setup_marathon_job>`_:
  Alerts when a Marathon service cannot be deployed or bounced for some reason.
  It will resolve when a service has been successfully deployed/bounced.

* `check_marathon_services_replication <generated/paasta_tools.check_marathon_services_replication.html>`_:
  runs periodically and sends an alert if fewer than 50% of the requested
  instances are deployed on a cluster. If the service is registered in Smartstack
  it will look in Smartstack to count the available instances. Otherwise it
  counts the number of healthy tasks in Mesos.


The PaaSTA command line
------------------------

The PaaSTA command line interface, ``paasta``, gives users of PaaSTA the
ability to inspect the state of services, as well as stop and start existing
services. See the man pages for a description and detail of options for any
individual paasta command.  Some of the most frequently used commands are
listed below:

* ``paasta start`` - sets the desired state of the service instance to
  'started'. In the case of long-running services, this will mean ensuring that
  the number of instances of your application matches that set in your
  soa-configs.

* ``paasta stop`` - sets the desired state of the service instance to 'stop'.
  The result of this for long running tasks is that your tasks are shutdown
  using whichever drain method you have specified, and tells PaaSTA that the
  number of instances of your task deployed should be 0.
  In the case of scheduled tasks, this tells PaaSTA to ensure that your task is
  no longer scheduled.
  **NB**: ``paasta stop`` is a temporary measure; that is, it's effect only lasts until
  you deploy a new version of your service. That means that if you run ``paasta
  stop`` and push a version of the docker image serving your service, then
  paasta will reset the effect of ``paasta stop``.
