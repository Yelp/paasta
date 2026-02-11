====================================
Autoscaling PaaSTA Instances
====================================

PaaSTA allows programmatic control of the number of replicas (Pods) a service has.
It uses Kubernetes' Horizontal Pod Autoscaler (HPA) to watch a service's load and scale up or down.

How to use autoscaling
======================

Enabling autoscaling
--------------------

In order to use autoscaling, edit your ``kubernetes-*.yaml`` files in your soa
configs and add a ``min_instances`` and a ``max_instances`` attribute and
(optionally) remove the ``instances`` attribute from each instance you want to autoscale.
When using autoscaling, the ``min_instances`` and ``max_instances`` attributes
become the minimum and maximum (inclusive) number of replicas tasks Kubernetes will
create for your job.

If load history for your service is missing in Prometheus for some/all replicas, the Prometheus query will assume that each missing replica is at 100% load.
The reasoning behind this is that during a situation where there is missing data, scaling a service up is generally the safest course of action.
This behavior may mean that your service is scaled up unnecessarily when you first enable autoscaling.
Don't worry - the autoscaler will soon learn what the actual load on your service is, and will scale back down to the appropriate level.

If you use autoscaling it is highly recommended that you make sure your service has a readiness probe.
If your service is registered in Smartstack, each Pod automatically gets a readiness probe that checks whether that Pod is available in the service mesh.
Non-smartstack services may want to configure a ``healthcheck_mode``, and either ``healthcheck_cmd`` or  ``healthcheck_uri`` to ensure they have a readiness probe.
The HPA will ignore the load on your Pods between when they first start up and when they are ready.
This ensures that the HPA doesn't incorrectly scale up due to this warm-up CPU usage.

Autoscaling parameters are stored in an ``autoscaling`` attribute of your instances as a dictionary.
Within the ``autoscaling`` attribute, setting ``metrics_providers`` will allow you to specify one or more methods to determine the utilization of your service.
If a metrics provider isn't provided, the ``cpu`` metrics provider will be used.
Specifying a ``setpoint`` allows you to specify a target utilization for your service.
The default ``setpoint`` is 0.8 (80%).

Let's look at sample kubernetes config file:

.. sourcecode:: yaml

   ---
   main:
     cpus: 1
     mem: 300
     min_instances: 30
     max_instances: 50
     autoscaling:
       metrics_providers:
         - type: cpu
           setpoint: 0.5

This makes the instance ``main`` autoscale using the ``cpu`` metrics provider.
PaaSTA will aim to keep this service's CPU utilization at 50%.

Autoscaling components
----------------------

Metrics providers
^^^^^^^^^^^^^^^^^

The currently available metrics providers are:

:cpu:
  The default autoscaling method if none is provided.
  Measures the CPU usage of your service's container.

:worker-load:
  With the ``worker-load`` metrics provider, Paasta will scale your service based on worker utilization metrics.
  The autoscaler will use the ``setpoint`` value (default 0.8) to determine the target worker utilization.
  For example, with a setpoint of 0.8, the autoscaler will try to keep your workers at 80% utilization on average.

  .. note::
    This metric provider is compatible for both uwsgi and gunicorn services and is preferred over the uwsgi/gunicorn metric providers.
    If you have configured your service to use a non-default stats port (8889), PaaSTA will not scale your service correctly!

:uwsgi:
  With the ``uwsgi`` metrics provider, Paasta will configure your Pods to be scraped from your uWSGI master via its `stats server <http://uwsgi-docs.readthedocs.io/en/latest/StatsServer.html>`_.
  We currently only support uwsgi stats on port 8889, and Prometheus will attempt to scrape that port.

  .. note::

    If you have configured your service to use a non-default stats port (8889), PaaSTA will not scale your service correctly!

:gunicorn:
  With the ``gunicorn`` metrics provider, Paasta will configure your Pods to run an additional container with the `statsd_exporter <https://github.com/prometheus/statsd_exporter>`_ image.
  This sidecar will listen on port 9117 and receive stats from the gunicorn service. The ``statsd_exporter`` will translate the stats into Prometheus format, which Prometheus will scrape.

:active-requests:
  With the ``active-requests`` metrics provider, Paasta will use Envoy metrics to scale your service based on the amount
  of incoming traffic.  Note that, instead of using ``setpoint``, the active requests provider looks at the
  ``desired_active_requests_per_replica`` field of the autoscaling configuration to determine how to scale.

:piscina:
  This metrics provider is only valid for the Yelp-internal server-side-rendering (SSR) service. With the ``piscina``
  metrics provider, Paasta will scale your SSR instance based on how many Piscina workers are busy.

:arbitrary-promql:
  The ``arbitrary-promql`` metrics provider allows you to specify any Prometheus query you want using the `Prometheus
  query language (promql) <https://prometheus.io/docs/prometheus/latest/querying/basics/>`_.  This is useful when you
  have a custom metric that represents the load on your service.

  Configuration options:

  :metrics_query: (required) The PromQL query that returns your metric value. This can be any valid PromQL expression
    that returns an
    `instant vector <https://prometheus.io/docs/prometheus/latest/querying/basics/#expression-language-data-types>`_
    with a single value.

    This query is evaluated on the ``compute-infra-hpa`` Prometheus shard in the local cluster. If you need metrics
    from another shard, you'll need to replicate them into the ``compute-infra-hpa`` shard.

  :setpoint: (optional, default: 1.0) The target value for your metric. The HPA will scale your service to try to keep
    the metric at this value. Can be any positive number.

  :target_type: (optional, default: ``AverageValue``) How the HPA interprets the metric value:

    - ``AverageValue``: The HPA calculates desired replicas as ``ceil(metric_value / setpoint)``. With the default
      setpoint of 1.0, **the metric value is interpreted directly as the desired number of replicas**. This is the
      recommended mode for most use cases.

      Setting a setpoint of N is mathematically equivalent to dividing by N at the end of your PromQL query. For
      example, ``setpoint: 100`` with a query returning 500 will result in 5 desired replicas (500 / 100 = 5).

    - ``Value``: The HPA calculates desired replicas as ``ceil(current_replicas × metric_value / setpoint)``. With
      a setpoint of 1.0, **the metric value is treated as a scaling factor**. A value of 1.0 means no change, 1.2
      means scale up by 20%, and 0.5 means scale down by 50%. If the setpoint is something besides 1.0, the result
      of the query is first divided by the setpoint.

      This mode is useful when your query computes a ratio or scaling factor rather than an absolute value. However,
      it has dangerous caveats.

  :series_query: (optional) Advanced users can provide a custom series query for metric discovery. If omitted, PaaSTA
    automatically generates one and wraps your ``metrics_query`` with the appropriate labels. You probably don't need
    this.

  :resources: (optional) Advanced users can provide custom resource mappings for the prometheus-adapter. This controls
    how Prometheus labels are mapped to Kubernetes resources. If omitted, PaaSTA uses a default mapping that expects
    ``deployment`` and ``namespace`` labels. Only needed if you provide a custom ``series_query``.

  **Example 1: Desired replicas (AverageValue, default)**

  If you have a PromQL query that directly computes the number of replicas you want:

  .. sourcecode:: yaml

     ---
     main:
       min_instances: 1
       max_instances: 100
       autoscaling:
         metrics_providers:
           - type: arbitrary-promql
             metrics_query: |
               ceil(sum(rate(my_queue_depth{service="myservice"}[5m])) / 10)

  With the default ``target_type: AverageValue`` and ``setpoint: 1.0``, if this query returns 15, the HPA will
  scale to 15 replicas.

  **Example 2: Load-based scaling (AverageValue with setpoint)**

  If you want to scale based on a load metric with a target per-replica value:

  .. sourcecode:: yaml

     ---
     main:
       min_instances: 5
       max_instances: 50
       autoscaling:
         metrics_providers:
           - type: arbitrary-promql
             setpoint: 100
             metrics_query: |
               sum(rate(my_service_requests_total{service="myservice"}[2m]))

  If this query returns 1000 requests/second total, the HPA calculates 1000 / 100 = 10 desired replicas,
  which should put 100 RPS (the ``setpoint``) on each replica.

  **Example 3: Scaling factor (Value)**

  If your query computes a scaling factor, perhaps based on the average load on each replica:

  .. sourcecode:: yaml

     ---
     main:
       min_instances: 5
       max_instances: 50
       autoscaling:
         metrics_providers:
           - type: arbitrary-promql
             setpoint: 100
             target_type: Value
             metrics_query: |
               avg(rate(my_service_requests_total{service="myservice"}[2m]))

  If this query returns 150 request/second/replica and you currently have 10 replicas,
  the HPA would calculate ``value/setpoint*current_replicas`` = 150/100*10 = 15 desired replicas.

  .. warning:: It is very easy for the HPA to overshoot its target with this style of query. If the metric
    takes a while to detect new pods, then you can end up in a situation where the metric is averaging over
    an old number of pods while the HPA multiplies by the current number. For the example above, if the
    metric continues to return 150 for a little while, then the HPA might scale up by a factor of 1.5 again,
    resulting in ceil(22.5) = 23 replicas. This can also happen in reverse.

    For this reason, **we recommend using ``target_type: AverageValue`` and calculating the sum of the load
    instead.**

  **Potentially useful queries**

  Some potentially useful things you can do with metrics_query:

    * ``kube_deployment_spec_replicas{paasta_cluster="pnw-prod", deployment="service--name-instance--name"}`` would scale proportional to ``service_name.instance_name`` in the pnw-prod cluster.
      (Underscores get replaced with ``--`` and the dot gets replaced with ``-`` in the deployment name.)

    * ``(hour(vector(time())) == bool 13) * (day_of_week(vector(time())) == bool 3) * 100`` would scale to 100 between 13:00 and 14:00 every Wednesday (UTC), and 0 otherwise.
      You probably want to have another metrics_provider based on actual load (e.g. ``cpu`` or ``worker-load``) alongside this one.

    * ``(sum(kube_pod_status_phase{phase=~"Running|Pending",pod=~"service--name\\..*"} * on (pod) group_left () (kube_pod_annotations{annotation_paasta_yelp_com_instance="job.action",annotation_paasta_yelp_com_service="service_name",namespace="tron"})) > bool 0) * 50 or vector(0)``
      would scale to 50 whenever the tron job ``service_name.job.action`` starts running.

Decision policies
^^^^^^^^^^^^^^^^^

The currently available decicion policies are:

:proportional:
  (This is the default policy.)
  Uses a simple proportional model to decide the correct number of instances
  to scale to, i.e. if load is 110% of the setpoint, scales up by 10%.

  Extra parameters:

  :moving_average_window_seconds:
    The number of seconds to load data points over in order to calculate the average.
    Defaults to 1800s (30m).
    Currently, this is only supported for ``metrics_provider: uwsgi``.

:bespoke:
  Allows a service author to implement their own autoscaling.
  This policy results in no HPA being configured.
  An external process should periodically decide how many replicas this service needs to run, and use the Paasta API to tell Paasta to scale.
  See the :ref:`How to create a custom (bespoke) autoscaling method` section for details.

Using multiple metrics providers
--------------------------------

Paasta allows you to configure multiple metrics providers for your service, from the list above.  The service autoscaler
will scale your service up if *any* of the configured metrics are exceeding their target value; conversely, it will
scale down only when *all* of the configured metrics are below their target value.  You can configure multiple metrics
providers using a list in the ``autoscaling.metrics_providers`` field, as follows:

.. sourcecode:: yaml

   ---
   main:
     cpus: 1
     mem: 300
     min_instances: 30
     max_instances: 50
     autoscaling:
       metrics_providers:
         - type: cpu
           setpoint: 0.5
         - type: active-requests
           desired_active_requests_per_replica: 10

There are a few restrictions on using multiple metrics for scaling your service, namely:

1. You cannot specify the same metrics provider multiple times
2. You cannot use bespoke autoscaling (see Decision Policies, above) with multiple metrics providers
3. For Yelp-internal services, you cannot use the PaaSTA autotuner on cpu metrics combined with multiple metrics
   providers, if one of the metrics providers is CPU scaling.  You must explicitly opt-out of autotuning by setting a
   ``cpus`` value for this service instance.

If you run ``paasta validate`` for your service, it will check these conditions for you.


How to create a custom (bespoke) autoscaling method
---------------------------------------------------

The current number of instance for a service can be accessed through the PaaSTA
api from the endpoint ``/v1/services/SERVICE_NAME/INSTANCE_NAME/autoscaler``.
Sending an HTTP GET request will return an integer describing how many
instances PaaSTA thinks your sevice should have. This endpoint also accepts an
HTTP POST request with a JSON payload with the format ``{'desired_instances':
NUMBER_OF_DESIRED_INSTANCES}``. This endpoint can be used to control the number
of instances PaaSTA thinks your service should have.

Finally, remember to set the ``decision_policy`` of the ``autoscaling``
parameter for each service instance to ``"bespoke"`` or else PaaSTA will
attempt to autoscale your service with the default autoscaling method.


``max_instances`` alerting
--------------------------

In order to make you aware of when your ``max_instances`` may be too low, causing issues with your service, Paasta will send you ``check_autoscaler_max_instances`` alerts if all of the following conditions are true:

  * The autoscaler has scaled your service to ``max_instances``.

  * The load on your service (as measured by the ``metrics_provider`` you specified, e.g. your worker utilization or CPU utilization) is above ``max_instances_alert_threshold``.

The default value for ``max_instances_alert_threshold`` is whatever your ``setpoint`` is.
This means by default the alert will trigger when the autoscaler wants to scale up but is prevented from doing so by your ``max_instances`` setting.
If this alert is noisy, you can try setting ``max_instances_alert_threshold`` to something a little higher than your ``setpoint``.
Setting a very high value (a utilization value your metrics_provider would never measure) will effectively disable this alert.

If this alert reports an UNKNOWN status, this indicates an error with your metrics provided by the ``metrics_provider`` you've specified.  Please review the metric_provider and service configuration to ensure metrics can be collected as expected.
