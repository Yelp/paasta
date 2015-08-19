Upgrading Mesos
==================

In general, follow the
`Mesos Upgrading Documentation <http://mesos.apache.org/documentation/latest/upgrades/>`_
for the procedure to follow.


Orchestrating Mesos Upgrades
-------------------------------

At Yelp, Puppet controls the version of Mesos that is deployed to particular clusters.
To upgrade Mesos in this situation, update the version of Mesos to be deployed in hiera,
then use the `./orchestration_tools/upgrade_paasta_cluster.sh` script on that cluster.

See the script for more details. The exact procedure for upgrading a Mesos cluster
may change from version to version, so be sure to investigate the script to verify
that it does actions that conform to the documentation's recommendations.
