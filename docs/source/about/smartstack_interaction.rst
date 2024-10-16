SmartStack Service Discovery and PaaSTA Integration
===================================================

This document assumes some prior knowledge about SmartStack; see http://nerds.airbnb.com/smartstack-service-discovery-cloud/ for more information.

.. contents:: Table of Contents
   :depth: 2

SmartStack Service Discovery and Latency Zones
----------------------------------------------

In SmartStack, a service can be configured to be *discovered* at a particular
latency zone.

At Yelp, we give names to these latency zones like this:

.. image:: discovery_settings.svg
   :width: 700px

The ``discover:`` key in smartstack sets the scope at which individual
tasks can be discovered and load balanced to. For example, a setting
of ``discover: superregion`` would discover one large pool of tasks for
use, regardless of which region or habitat they are in. (great for availability)
e.g. A service in habitat A will make requests of a service in any of habitats
A-F. This is great for availability -- six habitats to try -- but may introduce
latency if communications are slow between region 1 and region 2.

As another example, a setting of ``discover: habitat`` would make it so only
the tasks in a particular habitat are discover-able by the clients in that
habitat. (great for latency)
e.g. A service in habitat A will make requests of a service only in habitats
A-C. This is great for latency -- only talk to habitats that are
topographically "nearby" -- but reduces availability since only three habitats
can be reached.

PaaSTA's SmartStack Unawareness and Pod Spreading Strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PaaSTA is not natively aware of SmartStack, to make it aware or more specifically Kubernetes scheduler aware, we can use Pod Topology Spread Contraints.
To balance pods across Availability Zones (AZs) in Kubernetes, we use `topology spread contraints <https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/>`_. By using the key
"topology_spread_constraints" in soa-configs to assign it for each instance of a service.

The Relationship Between Nerve "namespaces" and PaaSTA "instances"
------------------------------------------------------------------

Example: One-to-one Mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^

SmartStack's Nerve component can be configured to advertise different
components of the same service on different ports. In PaaSTA we call these
"Nerve namespaces". Here is an example of a service with two namespaces::

    #smartstack.yaml
    main:
        proxy_port: 20001
    api:
        proxy_port: 20002

The corresponding Kubernetes configuration in PaaSTA might look like this::

    #kubernetes.yaml
    main:
       instances: 10
       cmd: myserver.py
    api:
       instances: 3
       cmd: apiserver.py

In this way, a service can provide two different "modes", while using the same
codebases. Here there is a one-to-one mapping between the "Nerve namespaces" and
the "PaaSTA instances". By default the PaaSTA will advertise a service under the
Nerve namespace with the *same name* as the instance.

Example: Canary
^^^^^^^^^^^^^^^

However, there are situations where you would like to pool instances together under
the same Nerve namespace. Consider this example::

    #smartstack.yaml
    main:
        proxy_port: 20001

    #kubernetes.yaml
    main:
        instances: 10
        cmd: myserver.py
    canary:
        instances: 1
        registrations: ['service.main']
        cmd: myserver.py --experiment

With this example, the ``canary`` instance gets advertised *under* the ``main`` Nerve
namespace, which gives an effective pool of *11* instances listening behind port 20001.

This allows the canary instance, which may be configured differently than the main instance,
to participate in the normal "main" pool.

Example: Sharding
^^^^^^^^^^^^^^^^^

Sharding is another use case for using alternative namespaces::

    #smartstack.yaml
    main:
        proxy_port: 20001

    #kubernetes.yaml
    shard1:
        instances: 10
        registrations: ['service.main']
    shard2:
        instances: 10
        registrations: ['service.main']
    shard3:
        instances: 10
        registrations: ['service.main']

These shards all end up being load-balanced in the same "main" pool. More
complex YAML definitions can take advantage of YAML's
`anchors and references <https://gist.github.com/bowsersenior/979804>`_
to reduce duplication.

Addendum: Non-Smartstack Monitoring
***********************************

If a service is not in SmartStack, then our monitoring requirements are greatly
simplified. PaaSTA simply looks at the number of tasks that are running and
compares it to the requested task count. If the running task count is under the
configured percentage threshold (defaults to 50%) then an alert will be sent.
No consideration for the distribution of the tasks among latency zones
(habitats, regions, etc) is taken into account.
