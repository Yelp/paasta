How PaaSTA Bounces New Code and SOA-config changes
==================================================

In the context of this document, "Bouncing" refers to the procedure of
replacing old tasks with new ones. With long-running services bouncing
is implemented by controlling Marathon Apps. For periodic tasks, see the
section on "Chronos Bouncing."

A "Bounce" can happen for one of these reasons:

* A new version of the code is deployed (a git sha change)
* A change of `soa-configs <yelpsoa_configs.html>`_ for a service. (Change in ram, cpu, environment variables, etc)
* * With the exception of these keys:

.. program-output:: python -c "from paasta_tools.marathon_tools import CONFIG_HASH_BLACKLIST; print ', '.join(CONFIG_HASH_BLACKLIST)"

* An issue of a ``paasta restart`` (or partially from a start/stop)
* A change in system-wide PaaSTA configuration (defaults for volumes, ram, cpu, etc)

By default, PaaSTA will do the safest thing possible and favors service uptime
over speed. Read further for the actual details of this default procedure in the
``crossover`` bounce.

If service uptime is irrelevant then the ``brutal`` bounce can be selected.

The ``upthendown`` and ``downthenup`` provide for other use cases where more
predictable bouncing behavior is desired.

Read more in the next section for exact details and differences amongst these
bounce method.

Note that in **no case** will PaaSTA "revert code or config" back to previous
versions after a failed bounce. While some bounce methods have some protection
against failure situations, PaaSTA will never undo the intent implied by a change.
Addressing this problem should be done at at a layer *above PaaSTA*.


Bounce Methods
^^^^^^^^^^^^^^

crossover
"""""""""

The ``crossover`` bounce is the default bounce in PaaSTA. It is the safest
bounce method, as it only removes old tasks after verifying that the new tasks
are alive and healthy. The ``crossover`` bounce can actually protect
infrastructure against bad code, because it won't even proceed if the new
copies of the code are not healthy!

The cost of the ``crossover`` bounce is time. It takes time for new tasks to be
healthy, and the ``crossover`` bounce will take as much time as it takes for
the new copies of the service to start up and become healthy, plus the time it
takes to drain and kill the old tasks. In the case of HTTP services using
SmartStack, **no web requests should be lost under normal circumstances**.

.. image:: bounce_crossover.png
   :scale: 100%


brutal
""""""

The ``brutal`` bounce replaces old tasks with new tasks with no regards to
safety. It launches new tasks immediately and kills old tasks immediately
without draining them.

The ``brutal`` bounce is probably best for apps that don't have safety concerns
and want the fastest bounce possible.

.. image:: bounce_brutal.png
   :scale: 100%

**Note1**: Services *will* incur downtime during the ``brutal`` bounce and
there is no protection against unhealthy services. You can relax the monitoring
to account for this::

    main:
      bounce_method: 'brutal'
      # Relax the monitoring because it will be down during the bounce
      # things are going to be down and we don't care
      monitoring:
        alert_after: 20m


upthendown
""""""""""

The ``upthendown`` bounce will wait till there is a full healthy copy of a new
app before draining and gracefully killing the old one.

This bounce takes the longest and requires 2X the normal resources for a
service at peak.

Choose this bounce method with care, as it will refuse to proceed until the new
app is fully deployed and healthy, which may not be possible with large apps
combined with a limited cluster size.

.. image:: bounce_upthendown.png
   :scale: 100%

downthenup
""""""""""

The ``downthenup`` bounce will wait until all old copies of a service are gone
before launching any new copies of a service. This bounce method is commonly
used in conjunction with configuration for a single copy of a service, to
ensure only copy is running at a time::

    my_important_batch:
      instances: 1
      # Make sure the old code is down before bringing up the new code
      # to try to make sure only one copy runs at any given time
      bounce_method: 'downthenup'
      # Relax the monitoring because it will be down during the bounce
      # any we only have one copy
      monitoring:
        alert_after: 20m

**Note1**: Using the ``downthenup`` bounce is not a strict guarantee that only
one copy of code will run at a time. If a strict guarantee is needed, use a
stronger locking mechanisms like Zookeeper and don't depend on ``instances: 1``.

**Note2**: Services will incur downtime during the ``downthenup`` bounce and there
is no protection against unhealthy services.

.. image:: bounce_downthenup.png
   :scale: 100%

How to Select A Bounce Method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A service author can select a bounce method by setting ``bounce_method`` in
the marathon configuration file. (e.g. ``marathon-SHARED.yaml``) This setting
is set per-instance. If not set, it will default to the ``crossover`` method.
See the docs on the `marathon config <yelpsoa_configs.html#marathon-clustername-yaml>`_ file.

Additionally, a service author can configure how the bounce code determines
which instances are healthy by setting ``bounce_health_params``. This
dictionary is passed in as keyword arguments to `get_happy_tasks <generated/paasta_tools.bounce_lib.html#bounce_lib.get_happy_tasks>`_.
Valid options are:

* ``min_task_uptime``: Minimum number of seconds that a task must be running
  before we consider it healthy. Useful if tasks take a while to start up.
* ``check_haproxy``: Whether to check the local haproxy to make sure this task
  has been registered and discovered.

Chronos Bouncing
^^^^^^^^^^^^^^^^

Almost all of the topics described above don't really apply to Chronos Jobs.

In PaaSTA Chronos jobs are simply configured to use new code or config **on the
next execution of the job**. In progress jobs are not adjusted or killed.
