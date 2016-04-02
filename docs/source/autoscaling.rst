====================================
Autoscaling Marathon tasks in PaaSTA
====================================

PaaSTA allows programmatic control of the number of marathon tasks a service has. It does this by using Zookeeper as a data store to record the number of tasks it thinks should be running for each instance of a service on a per-cluster basis.

How to use autoscaling
======================

Enabling autoscaling
--------------------

In order to use autoscaling, edit your ``marathon-*.yaml`` files in your soa configs and add a ``min_instances`` and a ``max_instances`` attribute and remove the ``instances`` attribute from each instance you want to autoscale. When using autoscaling, the ``min_instances`` and ``max_instances`` attributes become the minimum and maximum (inclusive) number of marathon tasks paasta will create for your job. If autoscaling information for your service is not available in Zookeeper (e.g. you've just created your service) PaaSTA will default to creating ``min_instances`` instances.

Autoscaling parameters are stored in an ``autoscaling`` attribute of your instances as a dictionary. Within the ``autoscaling`` attribute, setting a ``metrics_provider`` will allow you to specify a method that determines the utilization of your service. If a metrics provider isn't provided, the ``"mesos_cpu_ram"`` metrics provider will be used. Within the ``autoscaling`` attribute, setting a ``decision_policy`` will allow you to specify the logic that determines when to autoscale your service. If a decision policy isn't provided, the ``"pid"`` decision policy will be used. Decision policies and metrics providers have their own optional keyword arguments that may be placed into the ``autoscaling`` dictionary as well.

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
       metrics_provider: mesos_cpu_ram
       delay: 300
       setpoint: 0.5

Autoscaling components
----------------------

Metrics providers
^^^^^^^^^^^^^^^^^

The currently available metrics providers are:

:mesos_cpu_ram:
  The default autoscaling method if none is provided. Tries to use cpu and ram usage to predict when to autoscale.
:http:
  Makes a request on a HTTP endpoint on your service. Expects a JSON-formatted dictionary with a 'utilization' field containing a number between 0 and 1.

  Autoscaling parameters:

  :endpoint: the path to perform the HTTP request on (the requested URL will be \http://$HOST:$PORT/endpoint). Defaults to 'status'.

Decision policies
^^^^^^^^^^^^^^^^^

The currently available decicion policies are:

:pid:
  Uses a PID controller to determine when to autoscale a service.

  Autoscaling parameters:

  :setpoint: the target utilization the controller aims for. Defaults to 0.8 (80%).
  :delay: the number of seconds the decision policy must wait before fetching new data. Defaults to 600.
:threshold:
  Autoscales when a service's utilization exceeds beyond a certain threshold.

  Autoscaling parameters:

  :setpoint: the target utilization the controller aims for. Defaults to 0.8 (80%).
  :threshold: the amount by which the setpoint must be exceeded in either direction before autoscaling is triggered. Defaults to 0.1 (10%).
  :delay: the number of seconds the decision policy must wait before fetching new data. Defaults to 600.
:bespoke:
  Allows a service author to implement their own autoscaling.

How to create a custom autoscaling method
-----------------------------------------

To set the number of instances for a particular service and instance, have your service write to its local Zookeeper cluster. The node that controls the number of instances for a particular service is ``'/mesos-CLUSTER_NAME/autoscaling/SERVICE_NAME/INSTANCE_NAME/instances'``.

Finally, remember to set the ``decision_policy`` of the ``autoscaling`` parameter for each service instance to ``"bespoke"`` or else PaaSTA will attempt to autoscale your service with the default autoscaling method.
