Upgrading Mesos
===============

In general, follow the
`Mesos Upgrading Documentation <http://mesos.apache.org/documentation/latest/upgrades/>`_
for the procedure to follow.

Paasta-tools Specific Requirements
----------------------------------

This project contains integration tests that run against a particular version of
Mesos. Here is an example timeline of how to actually execute an upgrade:

1. Bump the version of Mesos that is installed in the integration tests; ensure they pass.
2. Upgrade Mesos in the actual environment. (see `Orchestrating Actual Mesos Upgrades`_)
3. Update any client-specific Mesos bindings in ``paasta-tools`` (e.g. ``mesos.interface`` in ``requirements.txt``)


Orchestrating Actual Mesos Upgrades
-----------------------------------

At Yelp, Puppet controls the version of Mesos that is deployed to particular clusters.
To upgrade Mesos in this situation, update the version of Mesos to be deployed in hiera,
then use the ``./orchestration_tools/upgrade_paasta_cluster.sh`` script on that cluster.

See the script for more details. The exact procedure for upgrading a Mesos cluster
may change from version to version, so be sure to investigate the script to verify
that it does actions conform to the upstream documentation's
`recommendations <http://mesos.apache.org/documentation/latest/upgrades/>`_.
