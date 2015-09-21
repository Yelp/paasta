How PaaSTA Interacts with SmartStack
====================================

PaaSTA uses SmartStack configuration to influence the **deployment** and
**monitoring** of services. This document assumes some prior knowledge
regarding what SmartStack is, see http://y/smartstack for further reading.

How SmartStack Settings Influence Deployment
********************************************

In SmartStack, a service can be configured to be *discovered* at a particular
latency zone.

At Yelp, we give names to these latency zones like this:

.. image:: discovery_settings.svg
   :width: 700px

The ``discover:`` key in smartstack sets the scope at which individual
tasks can be discovered and load balanced to. For example, a setting
of ``discover: superregion`` would discover one large pool of tasks for
use, regardless of which region or habitat they are in. (great for availability)

As another example, a setting of ``discover: habitat`` would make it so only
the tasks in a particular habitat are discover-able by the clients in
that habitat. (great for latency)

What Would Happen if PaaSTA Were Not Aware of SmartStack
-------------------------------------------------------

PaaSTA uses `Marathon <https://mesosphere.github.io/marathon/>`_ to deploy
long-running services. At Yelp, PaaSTA clusters are deployed at the
``superregion`` level. If PaaSTA were unaware of the Smartstack Discovery
settings, Marathon would naively deploy tasks in a potentially "unbalanced"
manner:

.. image:: unbalanced_distribution.svg
   :width: 700px

With the naive approach, there is a total of 6 tasks for the superregion, but
four landed in ``region 1``, and two in ``region 2``. If this service's
``discover`` setting were ``region``, it would be unbalanced. Even worse, if
the ``discover`` setting were set to ``habitat``, there would be habitats
**without** tasks available to serve anything, likely causing an outage.

In a world with configurable SmartStack discovery settings, the deployment
system (Marathon) must be aware of these and deploy accordingly.

What A SmartStack-Aware Deployment Looks Like
----------------------------------------------

By taking advantage of
`Marathon Constraint Language <https://mesosphere.github.io/marathon/docs/constraints.html>`_
, specifically the
`GROUP_BY <https://mesosphere.github.io/marathon/docs/constraints.html#group_by-operator>`_
operator, Marathon can deploy tasks in such a way as to ensure a balanced number
of tasks in each latency zone. For example, if the SmartStack setting
were ``discover: habitat`` [1]_, we could calculate the Marathon Constraint
``["habitat", "GROUP_BY"]``, which will ask Marathon to distribute tasks
evenly between the habitats:

.. image:: balanced_distribution.svg
   :width: 700px

In this way, each habitat will be served as equally as possible. [2]_.

Similarly, if the ``discover`` setting were set to ``region``, the equivalent
Marathon constraint would ensure an equal number of tasks distributed to each region.

.. [1] Technically PaaSTA should be using the smallest value of the ``advertise``
   setting, tracked in `PAASTA-1253 <https://jira.yelpcorp.com/browse/PAASTA-1253>`_.
.. [2] Currently the ``instances:`` count represents the total number of
   instances in the cluster, eventually with `PAASTA-1254  <https://jira.yelpcorp.com/browse/PAASTA-1254>`_
   the instance count will be a per-discovery-location setting, meaning there
   will always be an equal number of instances per location.


How SmartStack Settings Influence Monitoring
********************************************

TBD
