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
`CEP 319 <http://y/cep319>`_ discusses how yelpsoa-configs are distributed to
``/nail/etc/services`` on machines in the cluster.

Contract with services
----------------------
The `Paasta Contract <http://y/paasta-contract>`_ describes the
responsibilities of services that wish to work with PaaSTA.

paasta_tools contains the implementation of several of these rules.
For example, `generate_deployments_json <generate_deployments_json.html>`_ is
the piece that checks each service's git repo for the specially-named branch
that tells PaaSTA which versions of the service should go to which clusters.

Deployment
----------
A yelpsoa-configs master runs `generate_deployments_json
<generate_deployments_json.html>`_ frequently. The generated
``deployments.json`` appears in ``/nail/etc/services`` throughout the cluster.

Marathon masters run `deploy_marathon_services
<deploy_marathon_services.html>`_, a thin wrapper around `setup_marathon_job
<setup_marathon_job.html>`_. These scripts parse ``deployments.json`` and the
current cluster state, then issue comands to Marathon to put the cluster into
the right state -- cluster X should be running version Y of service Z.

How PaaSTA runs Docker images
-----------------------------
Marathon launches the Docker containers that comprise a PaaSTA service. The
default configuration is managed by puppet in the `paasta_tools
module
<https://opengrok.yelpcorp.com/xref/sysgit/puppet/modules/paasta_tools/manifests/init.pp>`_.

Docker images are run by Mesos's native Docker executor. PaaSTA composes the
configuration for the running image:

* ``--attach``: stdout and stderr from running images are sent to logs that end
  up in the Mesos sandbox (currently unavailable).

* ``--cpu-shares``: This is the value set in ``marathon.yaml`` as "cpus".

* ``--memory``: This is the value set in ``marathon.yaml`` as "mem".

* ``--net``: PaaSTA uses bridge mode to enable random port allocation.

* ``--env``: Any environment variables specified in the ``env`` section will be here.

  * **WARNING**: A PORT variable is provided to the docker image, but it represents the EXTERNAL port, not the internal one. The internal service MUST listen on 8888, so this PORT variable confuses some service stacks that are listening for this variable. Such services MUST overwrite this environment variable to function. (``PORT=8888 ./uwisgi.py```) We tried to work around this, see `PAASTA-267 <https://jira.yelpcorp.com/browse/PAASTA-267>`_.

* ``--publish``: Mesos picks a random port on the host that maps to and exposes
  port 8888 inside the container. This random port is announced to Smartstack
  so that it can be used for load balancing.

* ``--privileged``: Containers run by PaaSTA are not privileged.

* ``--restart``: No restart policy is set on PaaSTA containers. Restarting
  tasks is left as a job for the Framework (Marathon).

* ``--rm``: Mesos containers are rm'd after they finish.

* ``--tty``: Mesos containers are *not* given a tty.

* ``--volume``: Volume mapping is controlled via the paasta_tools
  configuration. This is not user-controlled for security reasons. The default
  mappings include common configuration folders (like `srv-configs
  <https://trac.yelpcorp.com/wiki/HowToService/Configuration>`_), `yelpsoa-configs
  <https://docs.google.com/a/yelp.com/document/d/1ZBg5ykniRU30UXj4YcsKfmmnuegQbtR2VuqCAIGi-50/edit#bookmark=id.nn2fb0z24rjh>`_,
  and key files in ``/nail/etc`` (``habitat``, ``ecosystem``, etc). The aws keys
  in ``/etc/boto_cfg`` are also included by default.

* ``--workdir``: Mesos containers are launched in a temporary "workspace"
  directory on disk. Use the workdir sparingly and try not to output files.

Mesos is the actual system that runs the docker images. In Mesos land these are
called "TASKS". PaaSTA-configured tasks use exponential backoff to prevent
unhealthy tasks from continuously filling up disks and logs -- the more times
that your service has failed to start, the longer Mesos will wait before
trying to start it again.

Mesos *will* healthcheck the task based on the same healthcheck that Smarstack
uses, in order to prune unhealthy tasks. This pruning is less agressive than
smartstack's checking, so a dead task will go DOWN in smartstack before it is
reaped by Mesos.

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
is set per-instance. See the docs on the `marathon config <yelpsoa_configs.html#marathon-clustername-yaml>_`
file.

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
`check_marathon_services_replication <check_marathon_services_replication.html>`_
runs periodically and sends an alert if the actual state of the cluster does
not match the desired state.

Cleanup
-------
`cleanup_marathon_jobs <cleanup_marathon_jobs.html>`_ gets rid of Marathon jobs
that don't need to be running anymore. This should be rare, like if you change
a service's name or manually delete a ``paasta-[clustername]`` git branch, but
is a useful safety net in case a task escapes.
