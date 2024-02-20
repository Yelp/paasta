==================
Persistent Volumes
==================

What are Persistent Volumes?
----------------------------
Persistent Volumes are a Kubernetes feature that allows you to attach stateful storage (like an EBS volume) to Pods (i.e., your PaaSTA instance).

Caveats
-------
In general, we discourage the use of Persistent Volumes in favor of totally stateless services (i.e., where the state is separated from the service itself in a DB, S3, etc.)

That said, there are several things to keep in mind before deciding to use Persistent Volumes:
   - PaaSTA does not provide monitoring for Persistent Volumes - you are responsible for staying on top of your usage (i.e., there is no alerting for full or almost full volumes)
   - Persistent Volumes cannot be resized online - if you run out of space and need to expand your volume: there *will* be downtime
      - Additionally, PaaSTA cannot automatically handle this resize: you *will* need to find an engineer on the Compute Infrastructure team to do this
   - If you need to delete the Persistent Volume for your service for whatever reason, you will need an engineer on the Compute Infrastructure team to do this for you
   - As of this writing (2024-02-16), we've occasionally noticed some large (double-digit minute) delays where Kubernetes is unable to attach the EBS volume backing a Persistent Volume because of AWS errors.
      - In other words, until the cause of this issue is identified, it's possible that a Spot interruption (or other sort of disruption) could potentially cause downtime for your service

How do I use Persistent Volumes in PaaSTA?
------------------------------------------
If the above is acceptable, adding a block like:

.. sourcecode:: yaml

   persistent_volumes:
   - container_path: /path/to/mount
     # if you're a power-user, know what you're doing, and need something more specific than a bog-standard GP3 EBS volume - come talk to use in #paasta
     storage_class_name: ebs-retain-gp3
     size: 10 # in GB
     mode: RW  # unless you're populating the EBS volume externally, you likely want to be able to write to the volume :)

to your instance config will attach a Persistent Volume with 10GB of storage to every replica of your PaaSTA instance at ``/path/to/mount``.

NOTE: a Persistent Volume will be created *per-replica* - they are *not* shared between replicas.
