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

There are two bounce methods currently available:

* `upthendown <bounce_lib.html#bounce_lib.upthendown_bounce>`_ - Brings up a
  service, waits two minutes for it to be fully available in the load
  balancer, then removes the old service. This is the default method.
* `brutal <bounce_lib.html#bounce_lib.brutal_bounce>`_ - Destroys
  the old app first, then brings up the new one.

A service author can select a bounce method by setting ``bounce_method`` in
the marathon configuration file. (e.g. ``marathon-SHARED.yaml``) This setting
is set per-instance. See the docs on the `marathon config <yelpsoa_configs.html#marathon-clustername-yaml>_`
file.

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

`cleanup_marathon_orphaned_containers
<cleanup_marathon_orphaned_containers.html>`_ reaps containers that get lost in
the shuffle when we restart Mesos slaves too hard.
