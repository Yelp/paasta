Signals
========

Each Clusterman autoscaler instance manages the capacity for a single pool in a Mesos cluster.  Clusterman determines
the target capacity by evaluating signals.  Signals are a function of metrics and represent the estimated resources
(e.g. CPUs, memory) required by an application running on that pool.  Clusterman compares this estimate to the current
number of resources available and changes the target capacity for the pool accordingly (see :ref:`scaling_logic`).

Signal Evaluation
-----------------

During each autoscaling run, Clusterman evaluates each signal defined for the pool; any metrics requested by the signal
are automatically read from the metrics datastore by the autoscaler, and passed in to the signal, along with any
additional parameters that the signal needs in order to run.  The signal then returns a resource request, indicating how
many resources the application wants for the current period.  Clusterman combines the resource requests from all the
signals to determine how many resources to add or remove from the pool; these resource requests are subject to final
capacity limits on the cluster to ensure that the pool does not ever contain too many or too few resources (which might
cost extra money or impact availability).

A signal's resource request is defined as follows:

 .. code-block:: json

    {
      "Resources": {
        "cpus": requested_cpus,
        "mem": requested_memory_in_MB,
        "disk": requested_disk_in_MB
      }
    }


If an application does not define its own signal, or if Clusterman is unable to load or evaluate the application's
signal for any reason, Clusterman will fall back to using a default signal, defined in Clusterman's own service
configuration file.  See the configuration file and the ``clusterman`` namespace within ``clusterman_signals`` package
for the latest definitions.  In general, the default signal uses recent values of allocated CPUs, memory, and disk to
estimate the resources required in the future.

.. _adding_signals:

How to Write a Custom Signal
----------------------------
Code for custom signals should be defined in the ``clusterman_signals`` package. Once a signal is defined there, the
:ref:`pool_configuration` section below describes how Clusterman can be configured to use it for a pool.

Signal code
~~~~~~~~~~~
In ``clusterman_signals``, there is a separate directory for each application (called the **signal namespace**).  If
there is not already a namespace for your signal already, create a directory within ``clusterman_signals`` and create an
``__init__.py`` file within that directory.

Within that directory, application owners may choose how to organize signal classes within files.  The only requirement
is that the signal class must be able to be imported directly from that subpackage, i.e. ``from clusterman_signals.poolA
import MyCustomSignal``. Typically, in the ``__init__.py``, you would import the class and then add it to ``__all__``::

    from clusterman_signals.poolA.custom_signal import MyCustomSignal
    ...

    __all__ = [MyCustomSignal, ...]

Define a new class that implements :py:class:`clusterman_signals.base_signal.BaseSignal` (the class name should be
unique).  In this class, you only need to overwrite the :py:meth:`value` method.  :py:meth:`value` should use metric
values to return a :py:class:`clusterman_signals.base_signal.SignalResources` tuple, where the units
of the tuple should match the Mesos units: shares for CPUs, MB for memory and disk.

When you :ref:`configure your custom signal <pool_configuration>`, you specify the metric names that your signal
requires and how far back the data for each metric should be queried. The autoscaler handles the querying of metrics for
you, and passes these into the :py:meth:`value` method, along with the current UNIX timestamp.  The format of metrics
argument is a dictionary of metric timeseries data, keyed by the timeseries name and where where each metric timeseries
is a list of ``(unix_timestamp_seconds, value)`` pairs, sorted from oldest to most recent.

The signal also has available any configuration parameters that you specified in the :py:attr:`parameters` dict, and the
cluster and pool that the signal is operating on are available in the :py:attr:`cluster` and :py:attr:`pool` attributes
on the signal.

.. note:: For application metrics, the clusterman metrics client will automatically prepend the application name to the
   metric key to avoid conflicts between metrics for different applications.  However, Clusterman strips this prefix
   from the metric name before sending it to the signal, so you do not need to handle this in your signal code.

.. note:: For system metrics, the metrics client will add the cluster and pool as dimensions to the metric name to
   prevent conflicts between different clusters and pools.  These dimensions are also stripped from the metric name
   before being sent to the client, since they are accessible via the :py:attr:`cluster` and :py:attr:`pool` attributes
   in the signal.

Example
~~~~~~~
A custom signal class that averages ``cpus_allocated`` values::

    from clusterman_signals.base_signal import BaseSignal
    from clusterman_signals.base_signal import SignalResources

    class AverageCPUAllocation(BaseSignal):

        def value(self):
           cpu_values = [val for timestamp, val in self.metrics_cache['cpus_allocated']
           average = sum(cpu_values) / len(cpu_values)
           return SignalResources(cpus=average)

And configuration for a pool, so that the autoscaler will evaluate that signal every 10 minutes, over data from the last
20 minutes:

.. code-block:: yaml

    autoscaling_signal:
        name: AverageCPUAllocation
        branch_or_tag: v1.0.0
        period_minutes: 10
        required_metrics:
            - name: cpus_allocated
              type: system_metrics
              minute_range: 20

Under the hood (supervisord)
----------------------------

In order to ensure that the autoscaler can work with multiple clients that specify different versions of the
``clusterman_signals`` repo, we do not import ``clusterman_signals`` into the autoscaler.  Instead, Clusterman launches
each signal in a separate process and communicates with them over `abstract Unix domain sockets
<http://man7.org/linux/man-pages/man7/unix.7.html>`_.  The orchestration of the signal subprocesses and the autoscaler
is performed by `supervisord <http://supervisord.org>`_, a client/server system that controls the operation of all the
independent subprocesses.  In turn, supervisord is controlled by an autoscaler bootstrap batch daemon.  The way this
works is outlined in detail below:

0. When a new signal version is written, tagged, and pushed to master, Jenkins builds a virtual environment for that
   signal, creates a tarball of the virtualenv, and uploads it to S3.
1. When the autoscaler bootstrap batch starts, it reads the ``CMAN_CLUSTER`` and ``CMAN_POOL`` environment variables
   to determine what cluster and pool it should be operating on.
2. The autoscaler bootstrap script reads the version of the signal that should be used for this specific cluster and
   pool from the configuration.  It sets all of the :ref:`environment variables <supervisord_env_vars>` needed for
   ``supervisord`` to run.  Once the bootstrap initialization is complete, it starts ``supervisord``.
3. Since there may be multiple applications running on the pool, and each application can pin a different version of the
   signal code, we may need to download multiple different versions of the signal code.  The first thing ``supervisord``
   does when it starts, therefore, is to download all needed versions of the signal from S3 as specified
   in the ``CMAN_VERSIONS_TO_FETCH`` environment variable.

   ``supervisord`` uses a so-called `homogeneous process group <http://supervisord.org/configuration.html#program-x-section-settings>`_
   to fetch the signals.  That is, it runs one copy of the signal-fetcher script for each version of the signal code
   that needs to be downloaded; it reports completion only when all of the processes in the group have completed
   successfully.  The ``CMAN_NUM_VERSIONS`` environment variable controls the size of this process group, and each
   fetcher script takes ``%(process_num)`` as an argument to determine its task.
4. The autoscaler bootstrap waits for that step to complete, and then triggers supervisord to start the signal
   process(es) running via the ``CMAN_SIGNAL_NAMESPACES``, ``CMAN_SIGNAL_NAMES``, and ``CMAN_SIGNAL_APPS`` environment
   variables.  As above, ``supervisord`` runs the signals in homogeneous process groups.

   Each signal listens for incoming connections on an abstract Unix domain socket named
   ``\0{signal_namespace}-{signal_name}-{app}-socket``, where ``signal_namespace`` is the subdirectory of
   ``clusterman_signals`` containing the signal specified by ``signal_name``, and ``app`` is the application running
   the signal.

   .. note:: the name of the default signal is ``__default__``

   If the signal process dies for any reason, ``supervisord`` will automatically restart it, and the autoscaler will
   attempt to reconnect on the next iteration.
5. The autoscaler bootstrap waits for that step to complete and then starts the autoscaler batch daemon, which connects
   to all running signals and then proceeds to autoscale the pool.
6. The autoscaler bootstrap periodically polls files in srv-configs and AWS keys, and will restart the entire process if
   any of these files change.

Running the Signal Process
~~~~~~~~~~~~~~~~~~~~~~~~~~
To initialize the signal, ``run.py`` is called in the ``clusterman_signals`` repo; this script takes two command-line
arguments: the pool of the signal to load, and the name of the signal to load.  The socket name is constructed from
these two parameters, which (should) guarantee that different pools communicate over different processes.  The script
then connects to the specified Unix socket and waits for the autoscaler to initialize the signal.  The JSON object for
signal initialization looks like the following:

.. code-block:: json

    {
        "cluster": what cluster this signal is operating on,
        "pool": what pool this signal is operating on for the specified cluster,
        "parameters": the values for any parameters from configuration that the signal should reference
    }

Once the signal is properly initialized, the ``run.py`` script waits for input from the autoscaler indefinitely.  Since
metrics data could be arbitrarily large, the communication protocol for this data looks like the following:

#. First the autoscaler must send the length of the encoded metrics data object as an unsigned integer
#. The signal run loop must ACK the length by sending ``0x1`` back to the autoscaler
#. The autoscaler then must send the actual metrics data, broken up into chunks if necessary
#. When the signal run loop has received all data, it must ACK the data by sending ``0x1`` back to the autoscaler,
   unless the run loop detects some error in the communication; in this case, it must send ``0x2`` to the autoscaler
#. If the autoscaler receives ``0x2`` from the signal, it will throw an exception; otherwise, it will wait for a
   response from the signal

The metrics input data takes the form of the following JSON blob:

.. code-block:: json

    {
        "metrics": {
            "metric-name-1": [[timestamp, value1], [timestamp, value2], ...],
            "metric-name-2": [[timestamp, value1], [timestamp, value2], ...],
            ...
        }
    }

In other words, the autoscaler passes in all of the ``required_metrics`` values for the signal, which have been
collected over the last ``period_minutes`` window for each metric.  The signal then will give the following response to
the autoscaler:

 .. code-block:: json

    {
      "Resources": {
        "cpus": requested_cpus,
        "mem": requested_memory_in_MB,
        "disk": requested_disk_in_MB
      }
    }

The value in this response is the result from running the signal with the specified data.

.. _supervisord_env_vars:

supervisord Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``CMAN_CLUSTER``: the name of the cluster to autoscale
* ``CMAN_POOL``: the name of the pool to autoscale
* ``CMAN_ARGS``: any additional arguments to pass to the autoscaler batch job
* ``CMAN_VERSIONS_TO_FETCH``: a space-separated list of signal versions to fetch from S3
* ``CMAN_SIGNAL_VERSIONS``: a space-separated list of versions to use for each signal
* ``CMAN_SIGNAL_NAMESPACES``: a space-separated list of namespaces to use for each signal
* ``CMAN_SIGNAL_NAMES``: a space-separated list of signal names
* ``CMAN_SIGNAL_APPS``: a space-separated list of applications scaled
* ``CMAN_NUM_VERSIONS``: the number of signal versions to fetch from S3
* ``CMAN_NUM_SIGNALS``: the number of signals to run
* ``CMAN_SIGNALS_BUCKET``: the location of the signal artifact bucket in S3
