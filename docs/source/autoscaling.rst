====================================
Autoscaling PaaSTA Instances
====================================

The section below introduces autoscaling in Marathon.
For services in Kubernetes, the following configurations are allowed in configuration files,
for compatibility with marathon.yaml; but not all of them are respected.
The mechanism is also different. If you are creating a new service on Kubernetes or are modifying
autoscaling configurations for your services that already running on Kubernetes, please
check :ref:`here <hpa>`.

PaaSTA allows programmatic control of the number of marathon tasks a service has.
It does this by using Zookeeper as a data store to record the number of tasks it
thinks should be running for each instance of a service on a per-cluster basis.


How to use autoscaling
======================

Enabling autoscaling
--------------------

In order to use autoscaling, edit your ``marathon-*.yaml`` files in your soa
configs and add a ``min_instances`` and a ``max_instances`` attribute and
remove the ``instances`` attribute from each instance you want to autoscale.
When using autoscaling, the ``min_instances`` and ``max_instances`` attributes
become the minimum and maximum (inclusive) number of marathon tasks paasta will
create for your job.

If autoscaling information for your service is not available in Zookeeper (e.g.
you've just created your service) PaaSTA will "fail safe" and default to
creating ``max_instances`` instances. The reasoning behind this is that during
a situation where there is a loss of autoscaling state, a service up is the
safest course of action until the autoscaler can scale back down to a "normal"
instance count.

If you use autoscaling it is highly recommended that you also configure a
healthcheck. This ensures that PaaSTA will not autoscale marathon tasks that are
just starting up or unhealthy. If you have no healthcheck defined autoscaling will
run as soon as the service is deployed. If you have defined a healthcheck,
autoscaling will run as soon as the healthcheck passes. This is so new tasks that
have higher-than-average load when starting up are ignored.

Autoscaling parameters are stored in an ``autoscaling`` attribute of your instances as a dictionary.
Within the ``autoscaling`` attribute, setting a ``metrics_provider`` will allow you to specify a method that determines the utilization of your service.
If a metrics provider isn't provided, the ``"mesos_cpu"`` metrics provider will be used.
Within the ``autoscaling`` attribute, setting a ``decision_policy`` will allow you to specify the logic that determines when to autoscale your service.
If a decision policy isn't provided, the ``"proportional"`` decision policy will be used.
Specifying a ``setpoint`` allows you to specify a target utilization for your service.
The default ``setpoint`` is 0.8 (80%).
Decision policies and metrics providers have their own optional keyword arguments that may be placed into the ``autoscaling`` dictionary as well.

Let's look at sample marathon config file:

.. sourcecode:: yaml

   ---
   main:
     cpus: 1
     mem: 300
     min_instances: 30
     max_instances: 50
     autoscaling:
       decision_policy: pid
       metrics_provider: mesos_cpu
       setpoint: 0.5

This makes the instance ``main`` autoscale using the ``pid`` decision policy
and the ``mesos_cpu`` metrics provider. PaaSTA will aim to keep this service's
utilization at 50%.

Autoscaling components
----------------------

Metrics providers
^^^^^^^^^^^^^^^^^

The currently available metrics providers are:

:mesos_cpu:
  The default autoscaling method if none is provided. Tries to use cpu usage to
  predict when to autoscale.
:http:
  Makes a request on a HTTP endpoint on your service. Expects a JSON-formatted
  dictionary with a ``'utilization'`` field containing a number between 0 and
  1.

  Extra parameters:

  :endpoint:
    the path to perform the HTTP request on (the requested URL will be
    ``http://$HOST:$PORT/$endpoint``). Defaults to 'status'.
    Only status is supported in Kubernetes currently.

:uwsgi:
  Makes a request on a HTTP endpoint on your service. Expects a response with a
  JSON body containing the current uwsgi state (see `this page
  <http://uwsgi-docs.readthedocs.io/en/latest/StatsServer.html>`_ for the
  expected format). Uses the percentage of non-idle workers as the utilization
  metric.

  Extra parameters:

  :endpoint:
    the path to perform the HTTP request on (the requested URL will be
    ``http://$HOST:$PORT/$endpoint``). Defaults to 'status/uwsgi'.
    Only status/uwsgi is supported in Kubernetes currently.

Decision policies
^^^^^^^^^^^^^^^^^

The currently available decicion policies are:

:proportional:
  Uses a simple proportional model to decide the correct number of instances
  to scale to, i.e. if load is 110% of the setpoint, scales up by 10%.

  Extra parameters:

  :offset:
    Float between 0.0 and 1.0, representing expected baseline load for each container.
    Defaults to 0.0.
  :forecast_policy:
    See "Forecast policies" below.
  :good_enough_window:
    An array of two utilization values [low, high].
    If utilization per container at the forecasted total load is within the window, instances will not scale.
    Optional parameter (defaults to None).

:pid (Not Supported in Kubernetes):
  Uses a PID controller to determine when to autoscale a service.
  See `this page <https://en.wikipedia.org/wiki/PID_controller>`_ for more information on PIDs.

:threshold (Not Supported in Kubernetes):
  Autoscales when a service's utilization exceeds beyond a certain threshold.

:bespoke:
  Allows a service author to implement their own autoscaling.
  Currently set to max_instances in Kuberentes.

Forecast policies (Not Supported in Kubernetes)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Forecast policies are used by the proportional decision policy.
Two forecast policies have been implemented:

:current:
  Assumes current load will remain the same as the current value for the near future.

:moving_average:
  Assumes total load will remain near the average of data points within a window.

  Extra parameters:
   :moving_average_window_seconds:
     The number of seconds to load data points over in order to calculate the average. Defaults
     to 1800s (30m).

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
