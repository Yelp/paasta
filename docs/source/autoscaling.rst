====================================
Autoscaling Marathon tasks in PaaSTA
====================================

PaaSTA allows programmatic control of the number of marathon tasks a service has. It does this by using Zookeeper as a data store to record the number of tasks it thinks should be running for each instance of a service on a per-cluster basis.

How to use autoscaling
======================

Enabling autoscaling
--------------------

In order to use autoscaling, edit your ``marathon-*.yaml`` files in your soa configs and add a ``min_instances`` and a ``max_instances`` attribute and remove the ``instances`` attribute from each instance you want to autoscale. When using autoscaling, the ``min_instances`` and ``max_instances`` attributes become the minimum and maximum (inclusive) number of marathon tasks paasta will create for your job. If autoscaling information for your service is not available in Zookeeper (e.g. you've just created your service) PaaSTA will default to createing ``min_instances`` instances.

Adding a ``autoscaling_method`` attribute will allow you to specify the logic that determines when to scale up and scale down your instance. If an autoscaling method isn't provided, the ``"default"`` autoscaling method will be used.

Autoscaling methods
-------------------

The current autoscaling methods are:

:default:
  The default autoscaling method if none is provided. Tries to use cpu and ram usage to predict when to autoscale. (NOT YET IMPLEMENTED)
:bespoke:
  Allows service authors to implement their own autoscaling.

How to create a custom autoscaling method
-----------------------------------------

To set the number of instances for a particular service and instance, have your service write to its local Zookeeper cluster. The node that controls the number of instances for a particular service is ``'/mesos-CLUSTER/autoscaling/SERVICE_NAME/INSTANCE_NAME/instances'``.

Finally, remember to set your ``autoscaling_method`` parameter for each service to ``"bespoke"`` or else PaaSTA attempt to autoscale your service with the default autoscaling method.
