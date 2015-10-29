Getting Started
===============

**Warning**: PaaSTA is an opinionated way to integrate a collection of open source components in a holistic way to build a PaaS. It is not optimized to be simple to deploy for operators. It is optimized to not reinvent the wheel and utilized existing solutions to problems where possible.

PaaSTA has many dependencies. This document provides documentation on installing some of these dependencies, but some of them are left up as an exercise to the reader.


soa-configs
-----------

soa-configs are the shared configuration storage that PaaSTA uses to hold the description and configuration of what services exist and how they should be deployed and monitored.

This directory needs to be deployed globally in the same location to every server that runs any PaaSTA component. We recommend using a git-based distribution mechanism for access control and auditing.

PaaSTA reads particular config files for each service in the soa-configs directory. There is one folder per service. Here is an example tree::

  soa-configs
  ├── web
  │   ├── deploy.yaml
  │   ├── marathon-dev.yaml
  │   ├── marathon-prod.yaml
  │   ├── monitoring.yaml
  │   ├── service.yaml
  │   └── smartstack.yaml
  ├── api
  │   ├── deploy.yaml
  │   ├── marathon-dev.yaml
  │   ├── marathon-prod.yaml
  │   ├── monitoring.yaml
  │   ├── service.yaml
  │   └── smartstack.yaml
  ...

For the `soa-configs documentation <yelpsoa-configs>`_ for more information about the structure and contents of these files.

For more information about why we chose this method of config distribution, see watch `this talk on Yelp's soa-config and how they are used <https://vimeo.com/141231345>`_.

Docker and a Docker Registry
----------------------------

PaaSTA uses Docker to build and distribute code for each service. PaaSTA assumes that a single registry is available and that the associated components (Docker commands, unix users, mesos slaves, etc) have the correct credentials to use it.

The docker registry needs to be defined in a config file in ``/etc/paasta/``. PaaSTA merges all json files in ``/etc/paasta/`` together, so the actual filename is irrelevant, but here would be an example ``/etc/paasta/docker.json``::

  {
    "docker_registry": "private-docker-registry.example.com:443"
  }

There are many registries available to use, or you can `host your own <https://docs.docker.com/registry/>`_.

Mesos
-----

TBD

Marathon
--------

TBD

Chronos
-------

TBD

SmartStack
----------

TBD

Sensu
-----

TBD

Jenkins / Build Orchestration
-----------------------------

Jenkins is the suggested method for orchestrating build pipelines for services, but it is not a hard requirement. The actual method that Yelp uses to integrate Jenkins with PaaSTA is not open source.

In practice, each organization will have to decide how they want to actually run the ``paasta`` cli tool to kick off the building and deploying of images. This may be something as simple as a bash script::

  #!/bin/bash
  service=my_service
  sha=$(git rev-parse HEAD)
  paasta itest --service $service --commit $sha
  paasta push-to-registry --service $service --commit $sha
  paasta mark-for-deployment --git-url $(git config --get remote.origin.url) --commit $sha --clusterinstance prod.main --service $service

We designed PaaSTA to use normal command line tools so it could be integrated with opinionated existing workflows.
