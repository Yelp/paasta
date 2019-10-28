Cluster Management
==================

Clusterman comes with a number of command-line tools to help with cluster management.

Discovery
---------

The ``clusterman list-clusters`` and ``clusterman list-pools`` commands can aid in determining what clusters and pools
Clusterman knows about:

.. program-output:: python -m clusterman.run list-clusters --help
   :cwd: ../../

.. program-output:: python -m clusterman.run list-pools --help
   :cwd: ../../

Management
----------

The ``clusterman manage`` command can be used to directly change the state of the cluster:

.. program-output:: python -m clusterman.run manage --help
   :cwd: ../../

The ``--target-capacity`` option allows users to directly change the size of the Mesos cluster specified by the
``--cluster`` and ``--pool`` arguments.

Note that there can be up to a few minutes of "lag time" between when the manage command is issued and when
changes are reflected in the cluster.  This is due to potential delays introduced into the pipeline while AWS finds and
procures new instances for the cluster.  Therefore, it is not recommended to run ``clusterman manage`` repeatedly in
short succession, or immediately after the autoscaler batch has run.

.. note:: Future versions of Clusterman may include a rate-limiter for the manage command

.. note:: By providing the existing target capacity value as the argument to ``--target-capacity``, you can force
   Clusterman to attempt to prune any :attr:`fulfilled capacity <MesosPoolManager.fulfilled_capacity>` that is above the
   desired :attr:`target capacity <MesosPoolManager.target_capacity>`.

Status
------

The ``clusterman status`` command provides information on the current state of the cluster:

.. program-output:: python -m clusterman.run status --help
   :cwd: ../../

As noted above, the state of the cluster may take a few minutes to equilibrate after a ``clusterman manage`` command or
the autoscaler has run, so the output from ``clusterman status`` may not accurately reflect the desired status.
