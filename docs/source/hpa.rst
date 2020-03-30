.. _hpa:
====================================
Autoscaling Kubernetes pods in PaaSTA
====================================

This section introduces a new configuration for services running on Kubernetes.
It allows developers to utilize the full power of the new autoscaling system on Kubernetes.
If you are interested in the architectural design, please read `this blog post <https://yelpwiki.yelpcorp.com/display/PAASTA/2019/12/09/Service+Autoscaling+with+HPA>`_


Algorithms
======================

Here is a list of different Horizontal Pod Autoscaler(HPA) metric sources used for the three different `HPA  metrics <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#support-for-metrics-apis>`_.

:Resource Metrics:
  cpu/memory

:Custom Metrics:
  http/uwsgi when signalflow_metrics_query are not provided

:External Metrics:
  your_own_average_value
  http/uwsgi when signalflow_metrics_query are provided

The algorithms for all metrics are mostly the same but vary in detail.
In short, resource metrics and custom metrics scales your service proportionally based on the average value of metrics provided by all ready (healthy and running) pods in the same cluster.

External metrics scales your service proportionally based on the value of metrics provided by your signalflow query.
For example, if your target_value is 10, the value returned by your signalflow_metrics_query is 50.
Then, the desired replica is probably (target_value / signalflow_metrics_query) * current_ready_pods.

Note that you can add any number of metrics you want to.
The max value of desired number of replicas calculated will be used.

For more on the algorithm, please check out `this doc <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details>`_


Configurations
======================

Example
-------

Here is an example that includes all configuration

.. sourcecode:: yaml

   ---
   my-instance:
     ...
     horizontal_autoscaling:
       max_replicas: 3
       min_replicas: 1
       uwsgi:
         target_average_value: 0.53
         dimensions:
             paasta_instance: main_uswest1_autoscaling
             some_unique_signalfx_dimension: value_of_dimension
             more_dimension: more_values
         signalflow_metrics_query: "data('your_own_sfx_metrics', filter('some_dimension', 'value')).mean(over="30m").publish()"
       http:
         target_average_value: 53
       cpu:
         target_average_value: 0.7
       memory:
         target_average_value: 0.7
       your-own-sfx-metrics:
         target_value: 2333
         signalflow_metrics_query: "data('your_own_sfx_metrics', filter('some_dimension', 'value')).mean(over="30m").publish()"
       another-random-sfx-metrics:
         target_value: 2333
         signalflow_metrics_query: "data('your_own_sfx_metrics2', filter('some_dimension', 'value')).max(over="1d").publish()"
       ...

Parameters
----------

min_replicas (default=0)
^^^^^^^^^^^^^^^^^^^^^^^^
The minimum number of replicas(pods) of your service.

max_replicas (required)
^^^^^^^^^^^^^^^^^^^^^^^
The maximum number of replicas(pods) of your service.

horizontal_autoscaling
^^^^^^^^^^^^^^^^^^^^^^^^
This overrides ``autoscaling``, ``max_instances``, and ``min_instances``.

cpu
^^^
Please check here for `algorthm detail <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details>`_

:target_average_value (required):
  A number from 0 (exclusive) to 1 (inclusive), (0 - 1].
  This is the percentage of cpu used.

memory
^^^^^^
Same as cpu.

http
^^^^
Makes a request on a HTTP endpoint on your service.
Expects a JSON-formatted dictionary with a ``utilization`` field containing a number between 0 and 1.
Note that this is the same as in Marathon autoscaler.
The endpoint is ``/status`` and cannot be changed as of Paasta v0.93.0.

When ``signalflow_metrics_query`` is not provided, the average values of all HTTP metrics exposed by HTTP endpoints of all running pods is compared with ``target_average_value``, and current number of running pods to calculate desired number of pods.
You can find your HTTP metrics and its dimensions on SignalFX. Some common dimensions include `paasta_cluster`, `paasta_instance`, and `paasta_service`

When ``signalflow_metrics_query`` is provided, the value retrieved from signalfx with
``signalflow_metrics_query``
is used together with ``target_average_value``, and current number of running pods to calculate the desired number of pods.
This field exists to make it easier for folks who want to do autoscaling across clusters with their existing http metrics.
You can achieve the same function with your own custom metrics.
Any suggestions/demands are welcome.

:target_average_value (required):
  A number

:dimensions:
  Any number of custom key value pairs that are strings.
  Both key and value need to conform to `SignalFX requirement <https://developers.signalfx.com/metrics/metric_data_overview.html#_dimensions_2>`_
  Also, the dimension needs to conform to Kubernetes metrics name requirement.
  The regex used to validate is ``[a-z]([-a-z0-9]*[a-z0-9])``.
  **Please make sure your dimensions are different from the dimensions used others**
  HPAMetrics Collector will collect metrics from your HTTP endpoints, add your specified dimensions, and send them to SignalFX.
  So please make sure your service won't cause troubles for other people.

uwsgi
^^^^^
Makes a request on a HTTP endpoint on your service.
Expects a response with a JSON body containing the current uwsgi state (see `this page <http://uwsgi-docs.readthedocs.io/en/latest/StatsServer.html>`_ for the expected format).
Uses the percentage of non-idle workers as the utilization metric.
Only "/status/uwsgi" is used as the endpoint.

:target_average_value (required):
  Same as HTTP.

:dimensions:
  Same as HTTP.


your-own-sfx-metrics
^^^^^^^^^^^^^^^^^^^^
You can autoscale your service with any number of any random signalfx metrics you want.
The metrics name cannot be cpu/uwsgi/http/memory.
It needs to follow ``(^[a-z]([-a-z0-9]*[a-z0-9])?$)``.
You are responsible for writing your own signalfx query.
Note that the autoscaler checks metrics every 15s.

This use case makes it possible to autoscale an instance across clusters by reusing the same SignalFX metrics.
Also, users can use external metrics to set canary instances to any % of production instances.

:target_value (required):
  Any none-zero number.

:signalflow_metrics_query (required):
  String. This is not validated so you are responsible for making it right.
  Please refer to this doc for how to write `Signalflow queries <https://yelpwiki.yelpcorp.com/display/METRICS/SignalFlow+Example+Programs>`_
  In general, if your metrics fluctuate a lot, it is recommended to use mean() over a range of time.

bespoke
^^^^^^^^^^^^^^^^^^^^^^^^
To be supported.
