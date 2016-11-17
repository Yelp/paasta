=================================
Running Adhoc Instances in PaaSTA
=================================

PaaSTA allows users to pre-define and run adhoc (a.k.a. one-off) instances.
These instances can be used to run batch jobs, test suites, etc.

Creating an adhoc instance
==========================

Adhoc instances can be defined by creating an ``adhoc-[clustername].yaml`` file
in a service's ``soa_configs``. The specification for these files is defined in
the `yelpsoa configs documentation <yelpsoa_configs.html>`_.

Running an adhoc instance
=========================

Adhoc instances can be run using ``paasta local-run`` like any other instance.
A sample use case where one needs to ssh onto an adhoc batch machine and run
the adhoc instance ``example_instance`` for the service ``example_service``
would use the command:

  ``paasta local-run --pull --service example_service --instance example_instance``

The 'interactive' instance
--------------------------

Running ``paasta local-run`` without specifying the ``--instance`` flag
launches an interactive instance of a service running a bash shell. This
interactive instace can be used to run adhoc jobs that aren't run frequently
enough to be added to ``soa_configs.`` The defaults values for the cpu, mem and
disk that are allocated to the interactive instance are very generous, but they
can be further increased by editing ``adhoc-[clustername].yaml`` for the
cluster the interactive service is being used in and creating an
``interactive`` instance config. The interactive instance can be configured
like any other adhoc instance, e.g. by adding additional mounted volumes or
changing the networking type.
