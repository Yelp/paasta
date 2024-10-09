Cooking: How it all comes together
==================================

Ways That PaaSTA Can Run Services
---------------------------------

Long Running Services
^^^^^^^^^^^^^^^^^^^^^

Long running services are processes that are expected to run continuously
and usually have the same process id throughout. PaaSTA uses
`Kubernetes <yelpsoa_configs.html#kubernetes-clustername-yaml>`_ to configure how these
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

How PaaSTA Runs Docker Containers
---------------------------------
Kubernetes launches the Docker containers that comprise a PaaSTA service. Once a Pod is scheduled to start, the kubelet on the node running the Pod interacts with the container runtime
through the Container Runtime Interface (CRI) to start the container defined in the Pod specification.

Note: Kubernetes supports containerd as the Container Runtime.

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

* **check_kubernetes_services_replication**:
  runs periodically and sends an alert if fewer than 50% of the requested
  instances are deployed on a cluster. If the service is registered in Smartstack
  it will look in Smartstack to count the available instances. Otherwise it
  counts the number of healthy Pods in Kubernetes.


The PaaSTA command line
------------------------

The PaaSTA command line interface, ``paasta``, gives users of PaaSTA the
ability to inspect the state of services, as well as stop and start existing
services. See the man pages for a description and detail of options for any
individual PaaSTA command.  Some of the most frequently used commands are
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
  PaaSTA will reset the effect of ``paasta stop``.
