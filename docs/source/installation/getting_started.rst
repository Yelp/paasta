Getting Started
===============

**Warning**: PaaSTA is an opinionated way to integrate a collection of open
source components in a holistic way to build a PaaS. It is not optimized to be
simple to deploy for operators. It is optimized to not reinvent the wheel and
utilizes existing solutions to problems where possible.

PaaSTA has many dependencies. This document provides documentation on
installing some of these dependencies, but some of them are left as an
exercise to the reader.


soa-configs
-----------

soa-configs are the shared configuration storage that PaaSTA uses to hold the
description and configuration of what services exist and how they should be
deployed and monitored.

This directory needs to be deployed globally in the same location to every
server that runs any PaaSTA component. See the
`dedicated documentation <../soa_configs.html>`_ on how to build your own ``soa-configs``.

Docker and a Docker Registry
----------------------------

PaaSTA uses `Docker <https://www.docker.com/>`_ to build and distribute code for each service. PaaSTA
assumes that a single registry is available and that the associated components
(Docker commands, unix users, mesos slaves, etc) have the correct credentials
to use it.

The docker registry needs to be defined in a config file in ``/etc/paasta/``.
PaaSTA merges all json files in ``/etc/paasta/`` together, so the actual
filename is irrelevant, but here would be an example
``/etc/paasta/docker.json``::

  {
    "docker_registry": "private-docker-registry.example.com:443"
  }

There are many registries available to use, or you can
`host your own <https://docs.docker.com/registry/>`_.

Mesos
-----

PaaSTA uses Mesos to do the heavy lifting of running the actual services on
pools of machines.  See the `official documentation <http://mesos.apache.org/gettingstarted/>`_
on how to get started with Mesos.

Marathon
--------

PaaSTA uses `Marathon <https://mesosphere.github.io/marathon/>`_ for supervising long-running
services running in Mesos. See the `official documentation <https://mesosphere.github.io/marathon/docs/>`_
for how to get started with Marathon.
, and then see the `PaaSTA documentation <yelpsoa_configs.html#marathon-clustername-yaml>`_
for how to define Chronos jobs.

Chronos
-------

`Chronos <http://mesos.github.io/chronos/>`_ is a Marathon framework for
running scheduled tasks. See the `official documentation <http://mesos.github.io/chronos/docs/getting-started.html>`_
for how to get started with Chronos, and then see the `PaaSTA documentation <yelpsoa_configs.html#marathon-clustername-yaml>`_
for how to define Chronos jobs.

SmartStack
----------

`SmartStack <http://nerds.airbnb.com/smartstack-service-discovery-cloud/>`_ is
a dynamic service discovery system that allows clients to find and route to
healthy mesos tasks for a particular service.

The glue that actually configures SmartStack is `currently not open source <https://github.com/Yelp/paasta/issues/13>`_.

Other service-discovery mechanisms can be used with PaaSTA, but SmartStack
is currently the only supported method.

Sensu
-----

`Sensu <https://sensuapp.org/>`_ is a flexible and scalable monitoring system
that allows clients to send alerts for arbitary events. PaaSTA uses Sensu to allow
individual teams to get alerts for their services.

The `official documentation <https://sensuapp.org/docs/latest/overview>` has
instructions on how to set it up.

Out of the box Sensu doesn't understand team-centric routing, and must be combined
with handlers that are "team aware" it it is installed in a multi-tenant environment.
We to do that, we have written some `custom Sensu handlers <https://github.com/Yelp/sensu_handlers>`_
to do that.

Sensu is an optional but highly recommended component.

Jenkins / Build Orchestration
-----------------------------

Jenkins is the suggested method for orchestrating build pipelines for services,
but it is not a hard requirement. The actual method that Yelp uses to integrate
Jenkins with PaaSTA is not open source.

In practice, each organization will have to decide how they want to actually
run the ``paasta`` cli tool to kick off the building and deploying of images.
This may be something as simple as a bash script::

  #!/bin/bash
  service=my_service
  sha=$(git rev-parse HEAD)
  paasta itest --service $service --commit $sha
  paasta push-to-registry --service $service --commit $sha
  paasta mark-for-deployment --git-url $(git config --get remote.origin.url) --commit $sha --clusterinstance prod.main --service $service

PaaSTA can integrate with any existing orchestration tool that can execute
commands like this.
