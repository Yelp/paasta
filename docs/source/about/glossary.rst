Glossary
========

**App**
~~~~~~~~

Marathon app. A unit of configuration in Marathon. During normal
operation, one service "instance" maps to one Marathon app, but during
deploys there may be more than one app. Apps contain Tasks.

**Docker**
~~~~~~~~~~

Container `technology <https://www.docker.com/whatisdocker/>`_ that
PaaSTA uses.

**clustername**
~~~~~~~~~~~~~~~

A shortname used to describe a PaaSTA cluster. Use \`paasta
list-clusters\` to see them all.

**instancename**
~~~~~~~~~~~~~~~~

Logical collection of Mesos tasks that comprise a Marathon app. service
name + instancename = Marathon app name. Examples: main, canary.

**namespace**
~~~~~~~~~~~~~

An haproxy/SmartStack concept grouping backends that listen on a
particular port. A namespace may route to many healthy Marathon
instances. By default, the namespace in which a Marathon job appears is
its instancename.

**Nerve**
~~~~~~~~~

A service announcement `daemon <https://github.com/airbnb/nerve>`_
that registers services in zookeeper to be discovered.

**Marathon**
~~~~~~~~~~~~

A `Mesos Framework <https://mesosphere.github.io/marathon/>`_
designed to deploy stateless services.

**Mesos**
~~~~~~~~~

A `Cluster/Scheduler <http://mesos.apache.org/>`_ that interacts
with other `Framework <https://docs.mesosphere.com/frameworks/>`_
software to run things on nodes.

**Mesos Master**
~~~~~~~~~~~~~~~~

A machine running a Mesos Master process, responsible for coordination
but not responsible for actually running Marathon or Tron jobs. There
are several Masters, coordinating as a quorum via Zookeeper.

**Mesos Slave**
~~~~~~~~~~~~~~~

A machine running a Mesos Slave process, responsible for running
Marathon or Tron jobs as assigned by the Mesos Master.

**PaaSTA**
~~~~~~~~~~

The name of the Platform as a Service that powers a
`Yellow pages knockoff <http://yelp.com/>`_.

**service\_configuration\_lib**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A python library for interacting with soa-configs.

**SmartStack**
~~~~~~~~~~~~~~

The brand name for Airbnb’s Nerve + Synapse service discovery solution.

**Synapse**
~~~~~~~~~~~

A local haproxy daemon that runs on yocalhost

**Task**
~~~~~~~~

Marathon task. A process (usually inside a Docker container) running on
a machine (a Mesos Slave). One or more Tasks constitutes an App.

**soa-configs**
~~~~~~~~~~~~~~~

A git repo for centralized service configs.

**yocalhost**
~~~~~~~~~~~~~

An ip that both on-metal and containers can use to connect to other
services.

**Zookeeper**
~~~~~~~~~~~~~

A distributed key/value store used by Mesos for coordination and
persistence.
