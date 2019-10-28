Overview
=========

Clusterman scales the pools in each Mesos cluster independently. In other words, it
treats each group of instances with the same reserved pool in a Mesos cluster as a single unit.
For each pool in a cluster, Clusterman determines the total target capacity by evaluating signals.
These signals are user-defined functions of metrics collected through Clusterman.

Assumptions and Definitions (aka the "Clusterman Contract")
-----------------------------------------------------------

1. A **cluster** is a Mesos cluster, that is, a distributed system managed by `Apache Mesos <https://mesos.apache.org>`_
2. A **pool** is a group of machines belonging to a cluster; a **resource group** is a logical grouping of machines in a
   pool corresponding to a cloud provider's API (for example, an `Amazon Spot Fleet Request <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-fleet-requests.html>`_
   might be a resource group in a pool).  A cluster may have many pools and each pool may have many resource groups.
3. Resource groups must have a way of assigning **weights** to machines running in that resource group.  These weights
   are used by Clusterman to determine how many resources should be added to a particular pool.

   .. note:: It is recommended (but not required) that the weight has a consistent meaning across all the types of
     machines that can appear in the resource group; the definition of weight in a resource group can be arbitrarily
     chosen by the operator, as long as it is consistent.  For example, a resource group may define 1 unit of weight to
     equal 50 vCPUs.  Moreover, each resource group in the pool should use the same definition of weight.

4. An **application** is a Mesos framework running on a pool; note that applications *can* span resource groups but they
   *cannot* span pools; thus each pool has a dedicated "purpose" or "set of applications" that is managed by Clusterman.
5. Every pool has at most one Clusterman **autoscaler**, which is responsible for managing the size of that pool

How Clusterman Works
--------------------

Pool Manager
~~~~~~~~~~~~~~~~
Clusterman manages a group of agents in a pool through a pool manager; the pool manager consists of one or more resource
group units, which represent groups of machines that can be modified together, such as via an AWS spot fleet request.

Signals
~~~~~~~
For each pool, Clusterman determines the target capacity by evaluating signals. Signals reports the estimated resource
requirements (e.g. CPUs, memory) for an application running on that pool. Clusterman compares this estimate to the
current number of resources available and changes the target capacity for the pool accordingly.

These signals are functions of metrics and may be defined per application, by the owners of that application (see
:ref:`adding_signals`).  Each application may define exactly one signal; if there is no custom signal defined for a
application, there is also a default signal defined by Clusterman that will be used.

Metrics
~~~~~~~
Signals are functions of metrics, values collected by Clusterman over time.  Clusterman uses a metrics API layer to
ensure that all metric values are stored in a consistent format that can be used both for autoscaling and simulation
workloads.  At present, all metrics data is stored in DynamoDB.

Application owners may use the metrics library to record application-specific metrics. The clusterman service also
collects a number of metrics that may be used by anyone for autoscaling signals or simulation.

Simulator
~~~~~~~~~
In addition to the live autoscaler, Clusterman comes with a simulator that allows operators to test changes to their
code or signals, experiment with different parameter values on live data, or compute operating costs, all without
impacting production clusters.  The simulator uses the same metrics and signals as the live autoscaler, except that it
does not interact with live resource groups but instead operates in a simulated environment.
