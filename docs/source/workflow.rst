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

PaaSTA uses `Chronos <yelpsoa_configs.html#chronos-clustername-yaml>`_ to define
the command these scheduled jobs should execute, as well as their RAM, CPU, environment
variables, etc.

PaaSTA supports jobs being triggered by both a schedule, and by the completion
of other jobs.  The jobs that are required to complete before another job can
start are defined in a job's definition with the ``parents`` field. The ``parents`` field and
``schedule`` field are mutually exclusive; that is to say a job cannot specify both a
schedule and parents.

Notes on running scheduled jobs with PaaSTA:
 * PaaSTA (more specifically, Chronos) only supports running one 'instance' of
   a job at one time. That is to say if I have a job scheduled to run every
   24H, but one 'run' of that job takes longer than 24 hours, then when the job
   is next scheduled to be run, it will not do so.
 * A job cannot be run at a schedule more frequent than 1 minute.
 * A job *can* specify cross-service dependencies. That is, a ``parent`` job can belong to a different
   service to that it is triggering.


Alternative names: scheduled tasks, scheduled batches, cron jobs (Note: Chronos does not support cron-syntax)

Re Running Failed Jobs
""""""""""""""""""""""

If a Scheduled Job is required to be 'rerun', then this can be achieved using
the ``paasta rerun`` command.  This allows you to run a job in the context of
another date.

An example might be:
 * Assume I have a job, ``my-job``, which belongs to the service ``myservice``.
 * In the service's ``chronos-testcluster.yaml`` file, the ``schedule`` field is set to ``R/2014-09-25T00:00:00Z/PT24H``.
 * The run for my job yesterday failed due to a third party outage.
 * The command for my job includes a tron style datestring. ``cmd: ./run-job --date %(shortdate-14)s``.

If the date today is ``2016-04-26``, I can rerun the job for yesterday with the
command:

``$ paasta rerun --service myservice --instance my-job -c test-cluster -d
2016-04-25T00:00:00``

The cmd string in the job will be interpolated as it would have been at the time
provided by the ``-d`` parameter, and the job will be run once.

Notes on rerunning jobs:

  * Rerunning a job has no impact on the regular schedule of a job.
  * You can view information about a rerun job with ``paasta status``.
  * The result of the job is kept for 24 Hours after it's completion.
  * If the job being rerun is listed in any other job's ``parents`` field,
    that is, there are other dependent jobs defined in the cluster that only
    start once the job being rerun has completed, they will *not* be triggered once the rerun
    job has completed. Each downstream job must be rerun individually.

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
  soa-configs. In the case of scheduled-tasks, this will ensure that your task
  is enabled, and will be scheduled as normal. **Note** unless you have run
  `paasta stop` or `paasta emergency-stop` against your instance, this will be
  noop. Your service is started by default, and this command does not have to
  be run for a service to run.

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


* ``paasta emergency-start`` - In the case of long running services,
  ``emergency-start`` will ensure that the number of running instances of a
  service matches the desired instances; if this is already the case, then this
  is a noop. In the case of a chronos job, then emergency start will trigger a
  run of the job now, irrespective of whether one is scheduled to be run.  This
  will not impact the schedule, and jobs will continue to run according to the
  schedule thereafter. If the scheduled task has ``disabled: True`` in the
  service's soa-configs, then this is no op.


* ``paasta emergency-stop`` - In the case of long running services, any
  instances of your service will be immediately killed, with no regard for
  draining or a safe shutdown. PaaSTA will leave the number of desired
  instances at 0 until you next deploy your service. In the case of scheduled
  tasks, any in-flight tasks will be killed, and the job disabled until a new
  version of the service is deployed.
