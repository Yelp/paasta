================================
Autoscaling clusters with PaaSTA
================================

PaaSTA allows scaling AWS Auto Scaling Groups (ASGs) and Spot Fleet Requests
(SRFs) based on current utilization.  It does this by looking at the current
utilization for each region✕pool pair and a configurable target utilization for
the same, and then scaling up or down each "resource" (ASG or SFR) in that
region✕pool by the percent difference.

How to use cluster autoscaling
==============================

Configuration
-------------

PaaSTA expects to find a configuration dictionary for each resource (ASG or SFR)
that you want to scale, under the key ``cluster_autoscaling_resources``.  For
example, in ``/etc/paasta/cluster_autoscaling/paasta-autoscaler-dev/`` you might
have a file called ``paasta_dev.json`` with the contents:

.. sourcecode:: json

    {
      "cluster_autoscaling_resources": {
        "aws_autoscaling_group.dev.paasta_dev": {
          "type": "aws_autoscaling_group",
          "id": "paasta_dev",
          "region": "us-west-1",
          "pool": "default",
          "min_capacity": 1,
          "max_capacity": 10
        }
      }
    }

which would indicate an autoscaling group, in the us-west-1 region, named
"paasta_dev", which the cluster autoscaler will scale between 1 and 10 instances.

A spot fleet would have the type ``aws_spot_fleet_request``, and an id matching
spot fleet id (eg ``sfr-00000000-abcd-abcd-999999999999``).

The default target utilization is 0.8 (80%), which can be overridden per pool
under the ``resource_pool_setting`` configuration option.  For example, a config
file ``/etc/paasta/pools.json`` might have the contents:

.. sourcecode:: json

    {
      "resource_pool_settings": {
        "default": {
          "target_utilization": 0.6
        },
        "other": {
          "target_utilization": 0.9
        }
      }
    }

which will tell the cluster autoscaler to try to keep the utilization for the
``default`` pool around 60% and the ``other`` pool around 90%.

Running
-------

The debian package will install the script ``/usr/bin/paasta_autoscale_cluster``
which will read the configuration and run the autoscaler.  Note that it will
require proper AWS configuration to modify ASGs and/or SFRs.

How it works
============

Utilization error
-----------------

For each combination of region and pool, the autoscaler will first calculate a
"utilization error".  In short, this is the difference between the current
utilization and the target utilization (either defined in /etc/paasta or the
default of 0.8).

Current utilization
^^^^^^^^^^^^^^^^^^^

PaaSTA will first add up all cpu, disk, and memory used by tasks in the
region✕pool, and the total cpu, disk, and memory available in the same, and uses
this to calculate the percent utilization of cpus, disk, and mem.  THe current
utilization is the maximum of those three.

The result of this is the cluster autoscaler will scale so the most used
resource type is close to the target value, and the others should be less.

Scaling up
----------

When the current utilization is greater than the target utilization, the
utilization error is positive, and the region✕pool must scale up.  For this,
the cluster autoscaler will loop through each resource (ASG or SFR) in this
region✕pool, and set the target capacity to
``ceil(current_capacity * (1+utilization_error))``.

Scaling down
------------

Scaling down happens when the error is negative.  It takes more work as we
cannot just terminate enough instances to get to the target capaicty without the
risk that services become under-replicated.

To scale down safely, the cluster autoscaler will start draining instances until
enough capacity worth of instances are draining to bring it to just above the
target, then kill instances as they become safe to kill, or one every 5 minutes.
This happens independantly per resource (SFR/ASG), so with 4 SFRs at least 4
instances could be terminated every 5 minutes.

Draining can be disabled by setting ``"cluster_autoscaling_draining_enabled": false``
in the paasta system config.  In this case, each resource will simply have one
instance terminated every 5 minutes.

Notes on Spot Fleet Requests
============================

target_capacity
---------------

The target capacity for a spot fleet is an interger.  However, spot fleet requests
can be made will multiple instance types included, each with a different (possibly
decimal) weight.  When scaling up, spot fleet will attempt to get the capacity
just at or above the target.

The cluster autoscaler will read the instance weights from the spot fleet info,
and use those when calculating how much of an SFRs capacity is made up of each
instance.

The target capacity for an SFR cannot go below 1.  The cluster autoscaler will
not attempt to scale below 1 unless the SFR is in the ``cancelled_running``
state.

cancelled_running resources
---------------------------

If a spot fleet request is cancelled with terminate instances turned off, it will
be left in a ``cancelled_running`` state.  When the cluster autoscaler encounters
a SFR in this state, it will set the error to -1 and the min_instances to 0,
indicating that the resource should be scaled to 0.  However, the cluster
autoscaler will refuse to cancel more than 20% of a resource in one run (see
MAX_CLUSTER_DELTA), so it will effectively scale down 20% at a time until it hits
target capacity of 1.  At capacity 1, the cluster autoscaler will attempt to
terminate the remaining instances.

Additionally, if the region✕pool needs to scale up, the cluster autoscaler
will ignore ``cancelled_running`` SFRs.

If the region✕pool neeeds to scale down, and ``cancelled_running`` SFRs exist
in said region✕pool, the cluster autoscaler will ignore active ASGs and SFRs
in that region✕pool.
