====================================
Autoscaling PaaSTA Instances
====================================

PaaSTA allows programmatic control of the number of replicas (pods) a service has.
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
If your service is registered in Smartstack, each pod automatically gets a readiness probe that checks whether that pod is available in the service mesh.
Non-smartstack services may want to configure a ``healthcheck_mode``, and either ``healthcheck_cmd`` or  ``healthcheck_uri`` to ensure they have a readiness probe.
The HPA will ignore the load on your pods between when they first start up and when they are ready.
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

:uwsgi:
  With the ``uwsgi`` metrics provider, Paasta will configure your pods to be scraped from your uWSGI master via its `stats server <http://uwsgi-docs.readthedocs.io/en/latest/StatsServer.html>`_.
  Setpoint refers to the worker utilization, which is the percentage of workers that are busy.
  We currently only support uwsgi stats on port 8889, and Prometheus will attempt to scrape that port.

  You can specify ``moving_average_window_seconds`` (default ``1800``, or 30 minutes) to adjust how long of a time period your worker utilization is averaged over: set a smaller value to autoscale more quickly, or set a larger value to ignore spikes.

  .. note::

    If you have configured your service to use a non-default stats port (8889), PaaSTA will not scale your service correctly!


:gunicorn:
  With the ``gunicorn`` metrics provider, Paasta will configure your pods to run an additional container with the `statsd_exporter <https://github.com/prometheus/statsd_exporter>`_ image.
  This sidecar will listen on port 9117 and receive stats from the gunicorn service. The ``statsd_exporter`` will translate the stats into Prometheus format, which Prometheus will scrape.

  You can specify ``moving_average_window_seconds`` (default ``1800``, or 30 minutes) to adjust how long of a time period your worker utilization is averaged over: set a smaller value to autoscale more quickly, or set a larger value to ignore spikes.

:active-requests:
  With the ``active-requests`` metrics provider, Paasta will use Envoy metrics to scale your service based on the amount
  of incoming traffic.  Note that, instead of using ``setpoint``, the active requests provider looks at the
  ``desired_active_requests_per_replica`` field of the autoscaling configuration to determine how to scale.

  You can specify ``moving_average_window_seconds`` (default ``1800``, or 30 minutes) to adjust how long of a time period the number of active requests is averaged over: set a smaller value to autoscale more quickly, or set a larger value to ignore spikes.

:piscina:
  This metrics provider is only valid for the Yelp-internal server-side-rendering (SSR) service. With the ``piscina``
  metrics provider, Paasta will scale your SSR instance based on how many Piscina workers are busy.

  You can specify ``moving_average_window_seconds`` (default ``1800``, or 30 minutes) to adjust how long of a time period your worker utilization is averaged over: set a smaller value to autoscale more quickly, or set a larger value to ignore spikes.

:arbitrary_promql:
  The ``arbitrary_promql`` metrics provider allows you to specify any Prometheus query you want using the `Prometheus
  query language (promql) <https://prometheus.io/docs/prometheus/latest/querying/basics/>`.  The autoscaler will attempt
  to scale your service to keep the value of this metric at whatever setpoint you specify.

  .. warning:: Using arbitrary prometheus queries to scale your service is challenging, and should only be used by
  advanced users.  Make sure you know exactly what you're doing, and test your changes thoroughly in a safe environment
  before deploying to production.

Decision policies
^^^^^^^^^^^^^^^^^

:bespoke:
  Allows a service author to implement their own autoscaling.
  This policy results in no HPA being configured.
  An external process should periodically decide how many replicas this service needs to run, and use the Paasta API to tell Paasta to scale.
  See the :ref:`How to create a custom (bespoke) autoscaling method` section for details.
  This is most commonly used by the Kew autoscaler.

:Anything other value:
  The default autoscaling method.
  Paasta will configure a Kubernetes HPA to scale the service based on the metrics providers and setpoints.


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
