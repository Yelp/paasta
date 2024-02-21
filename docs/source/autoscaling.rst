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
Within the ``autoscaling`` attribute, setting a ``metrics_provider`` will allow you to specify a method that determines the utilization of your service.
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
       metrics_provider: cpu
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
  We currently only support uwsgi stats on port 8889, and Prometheus will attempt to scrape that port.

  .. note::

    If you have configured your service to use a non-default stats port (8889), PaaSTA will not scale your service correctly!


:gunicorn:
  With the ``gunicorn`` metrics provider, Paasta will configure your pods to run an additional container with the `statsd_exporter <https://github.com/prometheus/statsd_exporter>`_ image.
  This sidecar will listen on port 9117 and receive stats from the gunicorn service. The ``statsd_exporter`` will translate the stats into Prometheus format, which Prometheus will scrape.


Decision policies
^^^^^^^^^^^^^^^^^

The currently available decicion policies are:

:proportional:
  (This is the default policy.)
  Uses a simple proportional model to decide the correct number of instances
  to scale to, i.e. if load is 110% of the setpoint, scales up by 10%.

  Extra parameters:

  :offset:
    Float between 0.0 and 1.0, representing expected baseline load for each container.
    Defaults to 0.0.

    **DEPRECATED** - while it was previously more complicated, offset is now simply subtracted from your setpoint.
    For example, ``setpoint: 0.6`` with ``offset: 0.25`` is equivalent to ``setpoint: 0.35`` with no ``offset``.
    We recommend you just lower your setpoint by the same amount and remove the ``offset``.

    Previously, offset was used to counteract the fake utilization that would be seen by our old uWSGI metrics provider.
    Under the old system, the uWSGI metrics provider would always see 1 extra worker busy, because the metrics query was proxied through the actual uWSGI workers.
    Having the autoscaler understand how much load was fake and how much was real helped it converge faster to your target load.
    Nowadays, we measure uWSGI utilization in a different way that does not use a uWSGI worker, so this is no longer necessary.
    Support for ``offset`` was only retained to provide a smooth transition from the old system to the new system.

  :good_enough_window:
    **Not currently supported**
    An array of two utilization values [low, high].
    If utilization per container at the forecasted total load is within the window, instances will not scale.
    Optional parameter (defaults to None).

    This is not currently supported under Kubernetes (see PAASTA-17262), but Kubernetes has a `global 10% tolerance by default. <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details>`_
    This is equivalent to a good_enough_window of ``[0.9*setpoint, 1.1*setpoint]``
  :moving_average_window_seconds:
    The number of seconds to load data points over in order to calculate the average.
    Defaults to 1800s (30m).
    Currently, this is only supported for ``metrics_provider: uwsgi``.

:bespoke:
  Allows a service author to implement their own autoscaling.
  This policy results in no HPA being configured.
  An external process should periodically decide how many replicas this service needs to run, and use the Paasta API to tell Paasta to scale.
  See the :ref:`How to create a custom (bespoke) autoscaling method` section for details.

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

In order to make you aware of when your ``max_instances`` may be too low, causing issues with your service, paasta will send you alerts if all of the following conditions are true:

  * The autoscaler has scaled your service to ``max_instances``.

  * The load on your service (as measured by the ``metrics_provider`` you specified, e.g. your worker utilization or CPU utilization) is above ``max_instances_alert_threshold``.

The default value for ``max_instances_alert_threshold`` is whatever your ``setpoint`` is.
This means by default the alert will trigger when the autoscaler wants to scale up but is prevented from doing so by your ``max_instances`` setting.
If this alert is noisy, you can try setting ``max_instances_alert_threshold`` to something a little higher than your ``setpoint``.
Setting a very high value (a utilization value your metrics_provider would never measure) will effectively disable this alert.
