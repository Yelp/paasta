Metrics
=======

Metrics are used by Clusterman to record state about clusters that can be used later for autoscaling or simulation.

Clusterman uses a metrics interface API to ensure that all metric values are stored in a consistent format that can be
used both for autoscaling and simulation workloads.  At present, all metric data is stored in DynamoDB, and accessed
using the :py:class:`.ClustermanMetricsBotoClient`. In the future, the interface layer allows us to transparently change
backends if necessary.

Interacting with the Metrics Client
-----------------------------------

.. _metric_types:

Metric Types
~~~~~~~~~~~~
Metrics in Clusterman can be classified into one of three different types. Each metric type is stored in a
separate namespace. Within each namespace, metric values are uniquely identified by their key and timestamp.

.. data:: clusterman_metrics.APP_METRICS

   metrics collected from client applications (e.g., number of application runs)
.. data:: clusterman_metrics.METADATA

   metrics collected about the cluster (e.g., current spot prices, instance types present)
.. data:: clusterman_metrics.SYSTEM_METRICS

   metrics collected about the cluster state (e.g., CPU, memory allocation)

Application metrics are designed to be read and written by the application owners to provide input into their
autoscaling signals.  System metrics and metadata can be read by application owners, but are written by batch jobs
inside the Clusterman code base.  Metadata metrics cannot be read by application owners and are only used for monitoring
and simulation purposes.

Metric Keys
~~~~~~~~~~~
Metric keys have two components, a metric name and a set of dimensions.  The metric key format is::

    metric_name|dimension1=value1,dimension2=value2

This allows for metrics to be easily converted into SignalFX datapoints, where the metric name is used as the timeseries
name, and the dimensions are converted to SignalFX dimensions.  The :py:func:`.generate_key_with_dimensions` helper
function will return the full metric key in its proper format. Use it to get the correct key when reading or writing
metrics.

Reading Metrics
~~~~~~~~~~~~~~~
The metrics client provides a function called :py:meth:`.ClustermanMetricsBotoClient.get_metric_values` which can be
used to query the metrics datastore.

.. note:: In general, signal authors should not need to read metrics through the metrics client, because the
   :py:class:`BaseSignal` takes care of reading metrics for the signal.

Writing Metrics
~~~~~~~~~~~~~~~
The metrics client provides a function called :py:meth:`.ClustermanMetricsBotoClient.get_writer`; this function returns
an "enhanced generator" or coroutine (not an asyncio coroutine) which can be used to write metrics data into the
datastore.  The generator pattern is used to allow writing to be batched together and reduce throughput capacity into
DynamoDB.  See the API documentation for how to use this generator.

Example and Reference
---------------------

DynamoDB Example Tables
~~~~~~~~~~~~~~~~~~~~~~~
The following tables show examples of how our data is stored in DynamoDB:

============= ========== =====
Application Metrics
------------------------------
metric name   timestamp  value
============= ========== =====
app_A,my_runs 1502405756     2
app_B,my_runs 1502405810   201
app_B,metric2 1502405811   1.3
============= ========== =====

================================================= ========== =====
System Metrics
------------------------------------------------- ---------- -----
metric name                                       timestamp  value
================================================= ========== =====
cpus_allocated|cluster=norcal-prod,pool=appA_pool 1502405756    22
mem_allocated|cluster=norcal-prod,pool=appB_pool  1502405810    20
================================================= ========== =====

+---------------------------------------------------------------------------------------------------+-------------------------+-------------------------+
| Metadata                                                                                          |                         |                         |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+
| metric name                                         | timestamp  | value                          | <c3.xlarge, us-west-2a> | <c3.xlarge, us-west-2c> |
+=====================================================+============+================================+=========================+=========================+
| spot_prices|aws_availability_zone=us-west-2a,aws_instance_type=c3.xlarge   | 1502405756 | 1.30                           |                         |                         |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+
| spot_prices|aws_availability_zone=us-west-2c,aws_instance_type=c3.xlarge   | 1502405756 | 5.27                           |                         |                         |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+
| fulfilled_capacity|cluster=norcal-prod,pool=seagull | 1502409314 |                                |                       4 |                      20 |
+-----------------------------------------------------+------------+--------------------------------+-------------------------+-------------------------+

.. _metric_name_reference:

Metric Name Reference
~~~~~~~~~~~~~~~~~~~~~
The following is a list of metric names and dimensions that Clusterman collects:

System Metrics
^^^^^^^^^^^^^^
* ``cpus_allocated|cluster=<cluster name>,pool=<pool>``
* ``mem_allocated|cluster=<cluster name>,pool=<pool>``
* ``disk_allocated|cluster=<cluster name>,pool=<pool>``

Metadata Metrics
^^^^^^^^^^^^^^^^
* ``cpus_total|cluster=<cluster name>,pool=<pool>``
* ``disk_total|cluster=<cluster name>,pool=<pool>``
* ``fulfilled_capacity|cluster=<cluster name>,pool=<pool>`` (separate column per InstanceMarket)
* ``mem_total|cluster=<cluster name>,pool=<pool>``
* ``spot_prices|aws_availability_zone=<availability zone>,aws_instance_type=<AWS instance type>``
* ``target_capacity|cluster=<cluster name>,pool=<pool>``
