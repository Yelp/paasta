Preparation: paasta_tools and yelpsoa-configs
=========================================================

paasta_tools reads configuration about services from several YAML
files in `soa-configs <soa_configs.html>`_:

Each object inside of these YAML files is called an "instance" of a PaaSTA
service. It describes a unique way to run the service, with a unique command,
cpu and ram requirements, etc.

Duplication can be reduced by using YAML `anchors and merges <https://gist.github.com/bowsersenior/979804>`_.
PaaSTA will **not** attempt to run any definition prefixed with ``_``,
so you are free to use them for YAML templates.

**Note** that service names (the name of the folder where your config file is located) should be no more than 63 characters.
For kubernetes services(config files with kubernetes as prefix), the instance names should be no more than 63 characters as well.
_ is counted as two character. We convert _  to -- because underscore is not allowed in kubernetes pod names.

Example::

    _template: &template
        env:
            foo: bar

    main:
        <<: *template
        cpus: 1.0
        mem: 1000
        command: busybox httpd

    worker:
        <<: *template
        cpus: 0.1
        mem: 100
        command: python -m worker


``Common Settings``
-------------------

All configuration files that define something to launch on a PaaSTA Cluster can
specify the following options:

  * ``cpus``: Number of CPUs an instance needs. Defaults to 1. CPUs in Mesos
    are "shares" and represent a minimal amount of a CPU to share with a task
    relative to the other tasks on a host.  A task can burst to use any
    available free CPU, but is guaranteed to get the CPU shares specified.  For
    a more detailed read on how this works in practice, see the docs on `isolation <isolation.html>`_.

  * ``cpu_burst_add``: Maximum number of additional CPUs an instance may use while bursting; if unspecified, PaaSTA defaults to 1 for long-running services, and 0 for scheduled jobs (Tron).
    For example, if a service specifies that it needs 2 CPUs normally and 1 for burst, the service may go up to 3 CPUs, if needed.

  * ``mem``: Memory (in MB) an instance needs. Defaults to 4096 (4GB). In Mesos
    memory is constrained to the specified limit, and tasks will reach
    out-of-memory (OOM) conditions if they attempt to exceed these limits, and
    then be killed.  There is currently not way to detect if this condition is
    met, other than a ``TASK_FAILED`` message. For more a more detailed read on
    how this works, see the docs on `isolation <isolation.html>`_

  * ``disk``: Disk (in MB) an instance needs. Defaults to 1024 (1GB). Disk limits
    may or may not be enforced, but services should set their ``disk`` setting
    regardless to ensure the scheduler has adequate information for distributing
    tasks.

  * ``env``: A dictionary of environment variables that will be made available
    to the container. PaaSTA additionally will inject the following variables automatically (keep in mind all environment variables are strings in a shell):

    * ``PAASTA_SERVICE``: The service name
    * ``PAASTA_INSTANCE``: The instance name
    * ``PAASTA_CLUSTER``: The cluster name
    * ``PAASTA_HOST``: The hostname of the actual server the container is runnig on
    * ``PAASTA_PORT``: The configured port the service should listen on
    * ``PAASTA_DOCKER_IMAGE``: The docker image name
    * ``PAASTA_GIT_SHA``: The short git sha of the code the container has
    * ``PAASTA_DEPLOY_GROUP``: The `deploy group <deploy_group.html>`_ specified
    * ``PAASTA_MONITORING_TEAM``: The team that is configured to get alerts.
    * ``PAASTA_LAUNCHED_BY``: May not be present. If present, will have the username of the user who launched the paasta container
    * ``PAASTA_RESOURCE_CPUS``: Number of cpus allocated to a container
    * ``PAASTA_RESOURCE_MEM``: Amount of ram in MB allocated to a container
    * ``PAASTA_RESOURCE_DISK``: Amount of disk space in MB allocated to a container
    * ``PAASTA_RESOURCE_GPUS``: Number of GPUS (if requested) allocated to a container


  * ``extra_volumes``: An array of dictionaries specifying extra bind-mounts
    inside the container. Can be used to expose filesystem resources available
    on the host into the running container. Common use cases might be to share
    secrets that exist on the host, or mapping read-write volumes for shared
    data location. For example::

      extra_volumes:
        - {containerPath: /etc/secrets, hostPath: /etc/secrets, mode: RO}
        - {containerPath: /tmp, hostPath: /tmp, mode: RW}

    Note: The format of these dictionaries must match the specification for the
    `Mesos Docker containers schema
    <https://mesosphere.github.io/marathon/docs/native-docker.html>`_, no
    error-checking is performed.

    Note: In the case of a conflict between the ``extra_volumes`` and the
    system-configured volumes, ``extra_volumes`` will take precedence.

    **WARNING**: This option should be used sparingly. Any specified bind-mount
    must exist on the filesystem beforehand, or the container will not run.
    Additionally it is possible for a service to be defined with a read-write
    volume on a sensitive part of the filesystem, as root. PaaSTA does not
    validate that the bind mounts are "safe".


Placement Options
-----------------

Placement options provide control over how PaaSTA schedules a task, whether it
is scheduled by Marathon (on Mesos), Kubernetes, Tron, or ``paasta remote-run``.
Most commonly, it is used to restrict tasks to specific locations.

.. _general-placement-options:

General
^^^^^^^

These options are applicable to tasks scheduled through Mesos or Kubernetes.

  * ``deploy_blacklist``: A list of lists indicating a set of locations to *not* deploy to. For example:

      ``deploy_blacklist: [["region", "uswest1-prod"]]``

   would indicate that PaaSTA should not deploy the service to the ``uswest1-prod`` region.

  * ``deploy_whitelist``: A list of lists indicating a set of locations where deployment is allowed.  For example:

      ``deploy_whitelist: ["region", ["uswest1-prod", "uswest2-prod"]]``

    would indicate that PaaSTA can **only** deploy in ``uswest1-prod`` or ``uswest2-prod``.  If this list
    is empty (the default), then deployment is allowed anywhere.  This is superseded by the blacklist; if
    a host is both whitelisted and blacklisted, the blacklist will take precedence.  Only one location type
    of whitelisting may be specified.

  * ``pool``: The pool of machines a PaaSTA app runs in. If no pool is set,
    an app will automatically be set to run in ``default`` pool.

    Warning: In order for an service to be launched in a particular pool, there
    *must* exist some nodes that already exist with that particular
    pool attribute set.

.. _k8s-placement-options:

Kubernetes
^^^^^^^^^^

These options are only applicable to tasks scheduled on Kubernetes.

  * ``topology_spread_constraints``: A set of rules to spread Pods across topologies, for example to try spreading Pods evenly across both nodes and availability zones::

      topology_spread_constraints:
        - max_skew: 1
          topology_key: "topology.kubernetes.io/zone"
          when_unsatisfiable: "ScheduleAnyway"
        - max_skew: 1
          topology_key: "kubernetes.io/hostname"
          when_unsatisfiable: "ScheduleAnyway"

    These can be configured per cluster (or globally) and will be added to every Pod Spec template, using `paasta.yelp.com/service` and `paasta.yelp.com/instance` as selectors.

    To avoid conflicts with the `deploy_whitelist` and `deploy_blacklist`, please only use `when_unsatisfiable: "ScheduleAnyway"` (at least until PAASTA-17951 is resolved).

    For more information, see the official Kubernetes
    documentation on `topology spread constraints
    <https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/>`_.

  * ``node_selectors``: A map of labels a node is required to have for a task
    to be launched on said node. There are several ways to define a selector.
    The simplest is a key-value pair. For example, this selector restricts a
    task to c3.8xlarge instances::

      node_selectors:
        instance_type: c3.8xlarge

    The value can also be a list of multiple values. For example, this selector
    restricts a task to both c3.8xlarge and m5.2xlarge instances::

      node_selectors:
        instance_type: ["c3.8xlarge", "m5.2xlarge"]

    For more complex cases, an operator, and optionally a value (or values) must
    be set. Here are some examples:

    * Restricts a task to instances that are not c3.8xlarges::

        node_selectors:
          instance_type:
            - operator: NotIn
              values: ["c3.8xlarge"]

    * Requires that a node have the ``ssd`` label::

        node_selectors:
          ssd:
            - operator: Exists

    * Requires that a node not have the ``ssd`` label::

        node_selectors:
          ssd:
            - operator: DoesNotExist

    * Requires that a node have a label ``priority`` with a value greater than 1
      and less than 5::

        node_selectors:
          priority:
            - operator: Gt
              value: 1
            - operator: Lt
              value: 5

    .. note::

      The label ``instance_type`` is special. If set as a node selector,
      PaaSTA will automatically convert it to a canonical version set by
      Kubernetes on all AWS nodes.

  * ``anti_affinity``: A set of rules define when a node *should not* be
    selected for spawning a task in terms of task running on the node.
    This can be used to schedule a single task per node and provide better
    resource isolation for resource intensive tasks. For example::

      anti_affinity:
        service: acron

    for a service ``acron`` indicates to not schedule any 2 instances
    ``acron`` service on the same host. This can be extended to
    service ``instances`` also. For example::

      anti_affinity:
        service: acron
        instance: test

    would indicate to not schedule any 2 instances with name ``test``
    of service ``acron`` on the same host.
    Multiple anti_affinities rules can also be used which will result
    ``AND-ing`` of all the rules. For example::

      anti_affinity:
        - service: acron
        - service: kafka-k8s

    would indicate the scheduler to not select a node when both
    ``acron`` and ``kafka-k8s`` is running on the node
    **Note:** ``anti_affinity`` rules should be used with judiciously
    and with caution as they require substantial processing and
    may slow down scheduling significantly in large clusters

For more information on selector operators, see the official Kubernetes
documentation on `node affinities
<https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity>`_.

  * ``pod_management_policy``: An option for applications managed with `StatefulSets <https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/>`_ to determine if the pods are managed in parallel or in order.

    The default value is `OrderedReady <https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/#orderedready-pod-management>`_.
    It can be set to `Parallel <https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/#parallel-pod-management>`_. For example::

      pod_management_policy: Parallel


.. _mesos-placement-options:

Mesos
^^^^^

These options are applicable only to tasks scheduled on Mesos.

  * ``constraints``: Overrides the default placement constraints for services.
    Should be defined as an array of arrays (E.g ``[["habitat", "GROUP_BY"]]``
    or ``[["habitat", "GROUP_BY"], ["hostname", "UNIQUE"]]``). Defaults to
    ``[["<discover_location_type>, "GROUP_BY"], ["pool", "LIKE", <pool>],
    [<deploy_blacklist_type>, "UNLIKE", <deploy_blacklist_value>], ...]``
    where ``<discover_location_type>`` is defined by the ``discover`` attribute
    in ``smartstack.yaml``, ``<pool>`` is defined by the ``pool`` attribute in
    ``marathon.yaml``, and ``deploy_blacklist_type`` and
    ``deploy_blacklist_value`` are defined in the ``deploy_blacklist`` attribute
    in marathon.yaml. For more details and other constraint types, see the
    official `Marathon constraint documentation
    <https://mesosphere.github.io/marathon/docs/constraints.html>`_.

  * ``extra_constraints``: Adds to the default placement constraints for
    services. This acts the same as ``constraints``, but adds to the default
    constraints instead of replacing them. See ``constraints`` for details on
    format and the default constraints.

``kubernetes-[clustername].yaml``
-------------------------------

**Note:** All values in this file except the following will cause PaaSTA to
`bounce <workflow.html#bouncing>`_ the service:

* ``min_instances``
* ``instances``
* ``max_instances``
* ``backoff_seconds``

Top level keys are instance names, e.g. ``main`` and ``canary``. Each
instance MAY have:

  * Anything in the `Common Settings`_.

  * Anything from :ref:`General Placement Options <general-placement-options>`
    and :ref:`Kubernetes Placement Options <k8s-placement-options>`.

  * ``cap_add``: List of capabilities that are passed to Docker. Defaults
    to empty list. Example::

      "cap_add": ["IPC_LOCK", "SYS_PTRACE"]

  * ``instances``: Kubernetes will attempt to run this many instances of the Service

  * ``min_instances``: When autoscaling, the minimum number of instances that
    kubernetes will create for a service. Defaults to 1.

  * ``max_instances``: When autoscaling, the maximum number of instances that
    kubernetes will create for this service.
    If specified, ``instances`` is ignored.

  * ``registrations``: A list of SmartStack registrations (service.namespace)
    where instances of this PaaSTA service ought register in. In SmartStack,
    each service has difference pools of backend servers that are listening on
    a particular port. In PaaSTA we call these "Registrations". By default, the
    Registration assigned to a particular instance in PaaSTA has the *same name*,
    so a service ``foo`` with a ``main`` instance will correspond to the
    ``foo.main`` Registration. This would correspond to the SmartStack
    namespace defined in the Registration service's ``smartstack.yaml``. This
    ``registrations`` option allows users to make PaaSTA instances appear
    under an *alternative* namespace (or even service). For example
    ``canary`` instances can have ``registrations: ['foo.main']`` to route
    their traffic to the same pool as the other ``main`` instances.

  * ``container_port``: Specify the port to expose when in ``bridge`` mode.
    Defaults to ``8888``.

  * ``bounce_method``: Controls the bounce method; see `bounce_lib <generated/paasta_tools.bounce_lib.html>`_
    Note: the upthendown bounce is not available to kubernetes instances.

  * ``bounce_health_params``: A dictionary of parameters for get_happy_tasks.

    * ``check_haproxy``: Boolean indicating if PaaSTA should check the local
      haproxy to make sure this task has been registered and discovered
      (Defaults to ``True`` if service is in SmartStack)

    * ``min_task_uptime``: Minimum number of seconds that a task must be
      running before we consider it healthy (Disabled by default)

    * ``haproxy_min_fraction_up``: if ``check_haproxy`` is True, we check haproxy on up to 20 boxes to see whether a task is available.
      This fraction of boxes must agree that the task is up for the bounce to treat a task as healthy.
      Defaults to 1.0 -- haproxy on all queried boxes must agree that the task is up.

  * ``bounce_margin_factor``: proportionally increase the number of old instances
    to be drained when the crossover bounce method is used.
    0 < bounce_margin_factor <= 1. Defaults to 1 (no influence).
    This allows bounces to proceed in the face of a percentage of failures.
    It doesn’t affect any other bounce method but crossover.
    See `the bounce docs <bouncing.html>`_ for a more detailed description.

  * ``bounce_start_deadline``: a floating point number of seconds to add to the deadline when deployd notices a change
    to soa-configs or the marked-for-deployment version of an instance.
    Defaults to 0. (deadline = now)
    When deployd has a queue of instances to process, it will choose to process instances with a lower deadline first.
    Set this to a large positive number to allow deployd to process other instances before this one, even if their
      soa-configs change or mark-for-deployment happened after this one.
    This setting only affects the first time deployd processes an instance after a change --
      instances that need to be reprocessed will be reenqueued normally.

  * ``drain_method``: Controls the drain method; see `drain_lib
    <generated/paasta_tools.drain_lib.html>`_. Defaults to ``noop`` for
    instances that are not in Smartstack, or ``hacheck`` if they are.

  * ``drain_method_params``: A dictionary of parameters for the specified
    drain_method. Valid parameters are any of the kwargs defined for the
    specified bounce_method in `drain_lib <generated/paasta_tools.drain_lib.html>`_.

  * ``cmd``: The command that is executed. If a string, will be wrapped in ``/bin/sh -c``.
    If a list, will be executed directly as is with no shell parsing.

  * ``args``: An array of docker args if you use the `"entrypoint"
    <https://docs.docker.com/reference/builder/#entrypoint>`_ functionality.

  * ``monitoring``: See the `monitoring.yaml`_ section for details.

  * ``autoscaling``: See the `autoscaling docs <autoscaling.html>`_ for details

    * ``metrics_providers``: A list of data sources to use for autoscaling:

        * ``type``: Which method the autoscaler will use to determine a service's utilization.
          Should be ``cpu``, ``uwsgi``, ``active-reqeusts``, ``piscina``, ``gunicorn``, or ``arbitrary_promql``.

        * ``decision_policy``: Which method the autoscaler will use to determine when to autoscale a service.
          Should be ``proportional`` or ``bespoke``.

        * ``setpoint``: The target utilization (as measured by your ``metrics_provider``) that the autoscaler will try to achieve.
          Default value is 0.8.

        * ``desired_active_requests_per_replica``: Only valid for the ``active-requests`` metrics provider.  The
          target number of requests per second each pod should be receiving.

        * ``max_instances_alert_threshold``: If the autoscaler has scaled your service to ``max_instances``,
          and the service's utilization (as measured by your ``metrics_provider``) is above this value, you'll get an alert.
          The default is the same as your ``setpoint``.

        * ``moving_average_window_seconds``: A smoothing function to apply to the data received from your metrics
          provider.

        * ``prometheus_adapter_config``: **(advanced users only)** Custom prometheus configuration for the
          ``arbitrary_promql`` metrics provider.

    * ``scaledown_policies``: Custom configuration for the Kubernetes HPA controlling when the service will scale down;
      this parameter exactly follows the `Kubernetes HPA schema <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#scaling-policies>`
      for scaling policies.

  * ``deploy_group``: A string identifying what deploy group this instance belongs
    to. The ``step`` parameter in ``deploy.yaml`` references this value
    to determine the order in which to build & deploy deploy groups. Defaults to
    ``clustername.instancename``. See the deploy group doc_ for more information.

  * ``replication_threshold``: An integer representing the percentage of instances that
    need to be available for monitoring purposes. If less than ``replication_threshold``
    percent instances of a service's backends are available, the monitoring
    scripts will send a CRITICAL alert.

  * ``healthcheck_mode``: One of ``cmd``, ``tcp``, ``http``, or ``https``.
    If set to ``http`` or ``https``, a ``curl`` command will be executed
    inside the container.

    If set to ``cmd`` then PaaSTA will execute ``healthcheck_cmd`` and
    examine the return code. It must return 0 to be considered healthy.

    If the service is registered in SmartStack, the healthcheck_mode will
    automatically use the same setings specified by ``smartstack.yaml``.

    If not in smartstack, the default healthcheck is "None", which means
    the container is considered healthy unless it crashes.

    A http healthcheck is considered healthy if it returns a 2xx or 3xx
    response code.

  * ``healthcheck_cmd``: If ``healthcheck_mode`` is set to ``cmd``, then this
    command is executed inside the container as a healthcheck. It must exit
    with status code 0 to signify a successful healthcheck. Any other exit code
    is treated as a failure. This is a required field if ``healthcheck_mode``
    is ``cmd``.

  * ``healthcheck_grace_period_seconds``: Kubernetes will wait this long
    after the container has started before liveness probe is initiated.
    Defaults to 60 seconds. Readiness probes will always start after 10 seconds.
    The application should able to receive traffic as soon as the readiness probe
    is successful. Keep this in mind for any expensive "warm-up" requests.

    A failing readiness probe will not restart the instance, it will however be
    removed from the mesh and not receive any new traffic.

    To add an additional delay after the pod has started and before probes should
    start, see ``min_task_uptime``.

  * ``healthcheck_interval_seconds``: Kubernetes will wait this long between
    healthchecks. Defaults to 10 seconds.

  * ``healthcheck_timeout_seconds``: Kubernetes will wait this long for a
    healthcheck to return before considering it a failure. Defaults to 10
    seconds.

  * ``healthcheck_max_consecutive_failures``: Kubernetes will kill the current
    task if this many healthchecks fail consecutively. Defaults to 30 attempts.

  * ``healthcheck_uri``: The url of the service to healthcheck if using http.
    Defaults to the same uri specified in ``smartstack.yaml``, but can be
    set to something different here.

  * ``prometheus_shard``: Optional name of Prometheus shard to be configured to
    scrape the service. This shard should already exist and will not be
    automatically created.

  * ``prometheus_path``: Optional path the Prometheus shard to be configured with
    to scrape the service. This shard should already exist and will not be
    automatically created.

  * ``prometheus_port``: Optional port, not equal to ``container_port``, to
    expose for prometheus scraping.

  * ``routable_ip``: Optionally assign this instance a routable IP so it can be
    accessed externally. This option is implied when registered to smartstack or
    when specifying a ``prometheus_port``. Defaults to ``false``

  * ``weight``: Load balancer/service mesh weight to assign to pods belonging to this instance.
    Pods should receive traffic proportional to their weight, i.e. a pod with
    weight 20 should receive 2x as much traffic as a pod with weight 10.
    Defaults to 10.
    Must be an integer.
    This only makes a difference when some pods in the same load balancer have different weights than others, such as when you have two or more instances with the same ``registration`` but different ``weight``.

  * ``lifecycle``: A dictionary of additional options that adjust the termination phase of the `pod lifecycle <https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination>`_:
    This currently supports two sub-keys:

    * ``pre_stop_command``: The command to run in your container before stopping.  This could handle gracefully stopping or checkpointing your worker, for example.
      This can be a list of strings (command + arguments) or a single string (which gets turned into a single-element list by Paasta.)

    * ``termination_grace_period_seconds``: the number of seconds to allow before forcibly killing your instance.  Note that the instance will be forcibly killed after this period, so your pre_stop_command should complete well within this time period!

  * ``namespace``:
    **Currently in development, do not use.**
    The Kubernetes namespace where Paasta will create objects related to this service.
    Defaults to ``paastasvc-service--name`` (that is, the service name will have underscores replaced with ``--``.)

**Note**: Although many of these settings are inherited from ``smartstack.yaml``,
their thresholds are not the same. The reason for this has to do with control
loops and infrastructure stability. The load balancer tier can be pickier
about which copies of a service it can send requests to, compared to Mesos.

A load balancer can take a container out of service and put it back in a few
seconds later. Minor flaps and transient errors are tolerated.

The healthchecks specified here in this file signal to the infrastructure that
a container is unhealthy, and the action to take is to completely destroy it and
launch it elsewhere. This is a more expensive operation than taking a container
out of the load balancer, so it justifies having less sensitive thresholds.

``marathon-[clustername].yaml``
-------------------------------

e.g. ``marathon-pnw-prod.yaml``, ``marathon-mesosstage.yaml``. The
clustername is usually the same as the ``superregion`` in which the cluster
lives (``pnw-prod``), but not always (``mesosstage``). It MUST be all
lowercase. (non alphanumeric lowercase characters are ignored)

**Note:** All values in this file except the following will cause PaaSTA to
`bounce <workflow.html#bouncing>`_ the service:

* ``min_instances``
* ``instances``
* ``max_instances``
* ``backoff_seconds``

Top level keys are instance names, e.g. ``main`` and ``canary``. Each
instance MAY have:

  * Anything in the `Common Settings`_.

  * Anything from :ref:`General Placement Options <general-placement-options>`
    and :ref:`Mesos Placement Options <mesos-placement-options>`.

  * ``cap_add``: List of capabilities that are passed to Docker. Defaults
    to empty list. Example::

      "cap_add": ["IPC_LOCK", "SYS_PTRACE"]

  * ``instances``: Marathon will attempt to run this many instances of the Service

  * ``min_instances``: When autoscaling, the minimum number of instances that
    marathon will create for a service. Defaults to 1.

  * ``max_instances``: When autoscaling, the maximum number of instances that
    marathon will create for a service

  * ``registrations``: A list of SmartStack registrations (service.namespace)
    where instances of this PaaSTA service ought register in. In SmartStack,
    each service has difference pools of backend servers that are listening on
    a particular port. In PaaSTA we call these "Registrations". By default, the
    Registration assigned to a particular instance in PaaSTA has the *same name*,
    so a service ``foo`` with a ``main`` instance will correspond to the
    ``foo.main`` Registration. This would correspond to the SmartStack
    namespace defined in the Registration service's ``smartstack.yaml``. This
    ``registrations`` option allows users to make PaaSTA instances appear
    under an *alternative* namespace (or even service). For example
    ``canary`` instances can have ``registrations: ['foo.main']`` to route
    their traffic to the same pool as the other ``main`` instances.

  * ``backoff_factor``: PaaSTA will automatically calculate the duration of an
    application's backoff period in case of a failed launch based on the number
    of instances. For each consecutive failure that duration is multiplied by
    ``backoff_factor`` and added to the previous value until it reaches
    ``max_launch_delay_seconds``. See `Marathon's API docs <https://mesosphere.github.io/marathon/docs/rest-api.html>`_
    for more information. Defaults to 2.

  * ``max_launch_delay_seconds``: The maximum time marathon will wait between attempts
    to launch an app that previously failed to launch. See `Marathon's API docs
    <https://mesosphere.github.io/marathon/docs/rest-api.html>`_ for more information. Defaults to 300 seconds.

  .. _net:

  * ``net``: Specify which kind of
    `networking mode <https://docs.docker.com/engine/reference/run/#network-settings>`_
    instances of this service should be launched using. Defaults to ``'bridge'``.

  * ``container_port``: Specify the port to expose when in ``bridge`` mode.
    Defaults to ``8888``.

  * ``bounce_method``: Controls the bounce method; see `bounce_lib <generated/paasta_tools.bounce_lib.html>`_

  * ``bounce_health_params``: A dictionary of parameters for get_happy_tasks.

    * ``check_haproxy``: Boolean indicating if PaaSTA should check the local
      haproxy to make sure this task has been registered and discovered
      (Defaults to ``True`` if service is in SmartStack)

    * ``min_task_uptime``: Minimum number of seconds that a task must be
      running before we consider it healthy (Disabled by default)

    * ``haproxy_min_fraction_up``: if ``check_haproxy`` is True, we check haproxy on up to 20 boxes to see whether a task is available.
      This fraction of boxes must agree that the task is up for the bounce to treat a task as healthy.
      Defaults to 1.0 -- haproxy on all queried boxes must agree that the task is up.

  * ``bounce_margin_factor``: proportionally increase the number of old instances
    to be drained when the crossover bounce method is used.
    0 < bounce_margin_factor <= 1. Defaults to 1 (no influence).
    This allows bounces to proceed in the face of a percentage of failures.
    It doesn’t affect any other bounce method but crossover.
    See `the bounce docs <bouncing.html>`_ for a more detailed description.

  * ``bounce_start_deadline``: a floating point number of seconds to add to the deadline when deployd notices a change
    to soa-configs or the marked-for-deployment version of an instance.
    Defaults to 0. (deadline = now)
    When deployd has a queue of instances to process, it will choose to process instances with a lower deadline first.
    Set this to a large positive number to allow deployd to process other instances before this one, even if their
      soa-configs change or mark-for-deployment happened after this one.
    This setting only affects the first time deployd processes an instance after a change --
      instances that need to be reprocessed will be reenqueued normally.

  * ``drain_method``: Controls the drain method; see `drain_lib
    <generated/paasta_tools.drain_lib.html>`_. Defaults to ``noop`` for
    instances that are not in Smartstack, or ``hacheck`` if they are.

  * ``drain_method_params``: A dictionary of parameters for the specified
    drain_method. Valid parameters are any of the kwargs defined for the
    specified bounce_method in `drain_lib <generated/paasta_tools.drain_lib.html>`_.

  * ``cmd``: The command that is executed. Can be used as an alternative to
    args for containers without an `entrypoint
    <https://docs.docker.com/reference/builder/#entrypoint>`_. This value is
    wrapped by Mesos via ``/bin/sh -c ${app.cmd}``. Parsing the Marathon config
    file will fail if both args and cmd are specified [#note]_.

  * ``args``: An array of docker args if you use the `"entrypoint"
    <https://docs.docker.com/reference/builder/#entrypoint>`_ functionality.
    Parsing the Marathon config file will fail if both args and cmd are
    specified [#note]_.

  * ``monitoring``: See the `monitoring.yaml`_ section for details.

  * ``autoscaling``: See the `autoscaling docs <autoscaling.html>`_ for valid options and how they work

    * ``metrics_provider``: Which method PaaSTA will use to determine a service's utilization.

    * ``decision_policy``: Which method PaaSTA will use to determine when to autoscale a service.

  * ``deploy_group``: A string identifying what deploy group this instance belongs
    to. The ``step`` parameter in ``deploy.yaml`` references this value
    to determine the order in which to build & deploy deploy groups. Defaults to
    ``clustername.instancename``. See the deploy group doc_ for more information.

  * ``replication_threshold``: An integer representing the percentage of instances that
    need to be available for monitoring purposes. If less than ``replication_threshold``
    percent instances of a service's backends are not available, the monitoring
    scripts will send a CRITICAL alert.

In addition, each instancename MAY configure additional Marathon healthcheck
options (Read the official
`mesos documentation <https://mesos.readthedocs.io/en/latest/health-checks/>`_
for more low-level details:

  * ``healthcheck_mode``: One of ``cmd``, ``tcp``, ``http``, or ``https``.
    If set to ``http`` or ``https``, a ``curl`` command will be executed
    inside the container.

    If set to ``cmd`` then PaaSTA will execute ``healthcheck_cmd`` and
    examine the return code. It must return 0 to be considered healthy.

    If the service is registered in SmartStack, the healthcheck_mode will
    automatically use the same setings specified by ``smartstack.yaml``.

    If not in smartstack, the default healthcheck is "None", which means
    the container is considered healthy unless it crashes.

    A http healthcheck is considered healthy if it returns a 2xx or 3xx
    response code.

  * ``healthcheck_cmd``: If ``healthcheck_mode`` is set to ``cmd``, then this
    command is executed inside the container as a healthcheck. It must exit
    with status code 0 to signify a successful healthcheck. Any other exit code
    is treated as a failure. This is a required field if ``healthcheck_mode``
    is ``cmd``.

  * ``healthcheck_grace_period_seconds``: Marathon will wait this long for a
    service to come up before counting failed healthchecks. Defaults to 60
    seconds.

  * ``healthcheck_interval_seconds``: Marathon will wait this long between
    healthchecks. Defaults to 10 seconds.

  * ``healthcheck_timeout_seconds``: Marathon will wait this long for a
    healthcheck to return before considering it a failure. Defaults to 10
    seconds.

  * ``healthcheck_max_consecutive_failures``: Marathon will kill the current
    task if this many healthchecks fail consecutively. Defaults to 30 attempts.

  * ``healthcheck_uri``: The url of the service to healthcheck if using http.
    Defaults to the same uri specified in ``smartstack.yaml``, but can be
    set to something different here.

**Note**: Although many of these settings are inherited from ``smartstack.yaml``,
their thresholds are not the same. The reason for this has to do with control
loops and infrastructure stability. The load balancer tier can be pickier
about which copies of a service it can send requests to, compared to Mesos.

A load balancer can take a container out of service and put it back in a few
seconds later. Minor flaps and transient errors are tolerated.

The healthchecks specified here in this file signal to the infrastructure that
a container is unhealthy, and the action to take is to completely destroy it and
launch it elsewhere. This is a more expensive operation than taking a container
out of the load balancer, so it justifies having less sensitive thresholds.

**Footnotes**:

.. [#note] The Marathon docs and the Docker docs are inconsistent in their
   explanation of args/cmd:

    The `Marathon docs
    <https://mesosphere.github.io/marathon/docs/rest-api.html#post-/v2/apps>`_
    state that it is invalid to supply both cmd and args in the same app.

    The `Docker docs <https://docs.docker.com/reference/builder/#entrypoint>`_
    do not state that it's incorrect to specify both args and cmd. Furthermore,
    they state that "Command line arguments to docker run <image> will be
    appended after all elements in an exec form ENTRYPOINT, and will override
    all elements specified using CMD" which implies that both cmd and args can
    be provided, but cmd will be silently ignored.

    To avoid issues resulting from this discrepancy, we abide by the stricter
    requirements from Marathon and check that no more than one of cmd and args
    is specified. If both are specified, an exception is thrown with an
    explanation of the problem, and the program terminates.

.. _doc: deploy_groups.html

``tron-[clustername].yaml``
--------------------------------

This file stores configuration for periodically scheduled jobs for execution on
`Tron <https://github.com/yelp/tron>`_.

The documentation here is for the PaaSTA-specific options. For all other
settings, please see the
`canonical docs <https://tron.readthedocs.io/en/latest/jobs.html>`_.


Example Job
^^^^^^^^^^^

::

    ---
    convert_logs:
      node: paasta
      schedule:
        start_time: 04:00:00
      actions:
        verify_logs_present:
          command: "ls /var/log/app/log_%(shortdate-1)s.txt"
          executor: ssh
        convert_logs:
          requires: [verify_logs_present]
          command: "convert_logs /var/log/app/log_%(shortdate-1)s.txt /var/log/app_converted/log_%(shortdate-1)s.txt"
          service: test_service
          deploy_group: prod
          cpus: .5
          mem: 100

PaaSTA-Specific Options
^^^^^^^^^^^^^^^^^^^^^^^

Each Tron **job** configuration MAY specify the following options:

  * ``deploy_group``: A string identifying what deploy group this instance belongs
    to. The ``step`` parameter in ``deploy.yaml`` references this value
    to determine the order in which to build & deploy deploy groups.
    See the deploy group doc_ for more information.

  * ``service``: Configures PaaSTA to use the docker image from an alternative service.
    This setting picks the default service for the whole job, but ``service`` may
    also be set on a per-action basis. Tron jobs may be composed of multiple actions
    that use commands from multiple different services.

  * ``monitoring``: See the `monitoring.yaml`_ section for details.

Each Tron **action** of a job MAY specify the following:

  * Anything in the `Common Settings`_.

  * Anything from :ref:`General Placement Options <general-placement-options>`
    and :ref:`Mesos Placement Options <mesos-placement-options>` (currently, Tron
    only supports Mesos workloads).

  * ``service``: Uses a docker image from different service. When ``service`` is set
    for an action, that setting takes precedence over what is set for the job.

  * ``executor``: Configures Tron to execute the command in a particular way.
    Set to ``paasta`` to configure Tron to launch the job on the PaaSTA cluster.
    Defaults to ``paasta``. When ``executor``
    is NOT ``paasta`` (and is using ``ssh``), all of these paasta-specific options
    listed here in this documentation will have no effect. It is OK to have a job
    composed of mixed ``paasta`` and ``ssh`` actions.

  * ``deploy_group``: Same setting as the ``Job``, but on a per-action basis. Defaults
    to the setting for the entire job.

  * ``command``: The command to run. If the action is configured with ``executor: paasta`` (default),
    then the command should be something available in the docker container (it should NOT
    start with ``paasta local-run``).

If a Tron **action** of a job is of executor type ``spark``, it MAY specify the following:

  * ``spark_paasta_cluster``: The Paasta cluster on which to run spark jobs (spark executors).
    Default to the same cluster the tron job (spark driver) is running on.

  * ``spark_paasta_pool``: The Paasta pool on which to run spark jobs (spark executors).
    Default to ``batch`` pool if not specified.

  * ``spark_args``: Dictionary of spark configurations documented in
    https://spark.apache.org/docs/latest/configuration.html. Note some configurations are non-
    user-editable as they will be populated by paasta tools. See
    https://github.com/Yelp/service_configuration_lib/blob/master/service_configuration_lib/spark_config.py#L9
    for a complete list of such configurations.

  * ``aws_credentials_yaml``: Path to the yaml file containing credentials to be set in the task's
    AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables. Default to
    ``/etc/boto_cfg/<your-service-name>.yaml``. If the file path does not exist, or the file does
    not contain keys for aws_access_key_id and aws_secret_access_key, those variables will be unset.

``adhoc-[clustername].yaml``
-------------------------------

The yaml where adhoc instances are defined. Top-level keys are instance names.
Each instance MAY have:

  * Anything in the `Common Settings`_.

  * ``net``

  * ``cmd``

  * ``args``

  * ``deploy_group``

See the `marathon-[clustername].yaml`_ section for details for each of these parameters.

``smartstack.yaml``
-------------------

Configure service registration, discovery, load balancing and error alerting.

Here is an example smartstack.yaml::

    ---
    main:
      extra_advertise:
        ecosystem:stagec
        - ecosystem:stagec
        region:uswest1-prod
        - region:uswest2-prod
      proxy_port: 20028
      timeout_server_ms: 5000

The ``main`` key is the service namespace.  Namespaces were introduced for
PaaSTA services in order to support running multiple daemons from a single
service codebase. In PaaSTA, each instance in your marathon.yaml maps to a
smartstack namespace of the same name, unless you specify a different
``registrations``.

We now describe which keys are supported within a namespace.  Note that all but
proxy_port are optional.

Available Options
~~~~~~~~~~~~~~~~~

Basic HTTP and TCP options
``````````````````````````

 * ``proxy_port``: integer-valued (or ``null``) port on which HAProxy listens
   for requests. If ``null`` this service will be "discovery only" meaning that
   it will generate synapse discovery files on every host, but no listening
   port will be allocated. This must be unique across all environments where
   PaaSTA (or synapse) runs. At Yelp, we pick from the range [19000, 21000].
   Feel free to pick the next available value -- paasta fsm will do this for
   you automatically!

 * ``mode``: string of value ``http`` or ``tcp``, specifying whether the service
   is an HTTP or TCP service respectively.  Defaults to ``http``.

 * ``extra_headers``: for use in http mode. Headers that should be added to the
   request before being forwarded to the server. Example: ::

      extra_headers:
        X-Mode: ro

 * ``proxied_through``: for specifying a service that will proxy requests
   transparently. This can be used to, for example, to proxy requests to caching
   services.

   - The value of this directive needs to be another smartstack-
   namsespace, for example, ``servicename.main``

   - If the proxy service is down, then smartstack will automatically failover
   to the default backend

   - Example:

       proxied_through: servicename.main


Advertisement/Discovery
```````````````````````

The following three keys have to do with how your service instances advertise
and how other services discover your service. If you want to change these you
should understand how PaaSTA environments are laid out and classified into
latency zones. For information on this, see the `environment_tools documentation
<https://github.com/Yelp/environment_tools/blob/master/README.md>`_.

 * ``advertise``: List of location types that must be defined in your
   ``location_types.json``. This is the level of your location hierarchy that
   your service instances advertise themselves at, and should match up with how
   your service is deployed. Think of this as the answer to “where does nerve
   register me”. Defaults to ``['region']``

   - If you set this to a wider location type than where your ZooKeeper clusters
     are deployed, this will cause additional ZooKeeper load. Be careful, and
     consult with the team responsible for your ZooKeeper clusters.

   - This is a list so that you can do a graceful change in ``discover`` (see
     immediately below). You add both types to the advertise list and then when
     you’ve switched ``discover`` you shrink it back down to one.

 * ``discover``: Location type that SmartStack should discover your service
   instances at. Think of this as the answer to “where does synapse look for
   registrations”. Defaults to ‘region’.

   - This *must* be one of the location types listed in your ``advertise`` key
     to make sense.
   - Changing this key has potential to bring down your service everywhere all
     at once, change it only if you are sure what you are doing.

 * ``extra_advertise``: Mapping of location to locations, where locations are
   specified as ``<location_type>:<location_instance>`` strings. For example,
   ``habitat:devc`` or ``ecosystem:prod``. These are ad-hoc advertisements that
   read logically left to right. If you have X: [Y] in your extra_advertise
   object, then that will cause service instances in X to be available in Y.
   Defaults to an empty dictionary.

   - Be careful when putting a large location on the right side of this mapping:
     your service instances may be advertised into multiple ZooKeeper clusters.

   - The right-hand side must be the same or more general than your ``discover``
     level. For instance, if your service discovers at the region level, you
     cannot advertise to a habitat, but you could advertise to a region or
     superregion.

   - Here's an example of how to advertise an sf-devc service instance into
     uswest1-devc::

       extra_advertise:
           region:sf-devc: ['region:uswest1-devc']


Healthchecks
````````````

You can control your healthchecks with the following keys.

 * ``healthcheck_mode``: specifies the mode for the healthcheck (``http`` or
   ``tcp``). Defaults to the ``mode`` of the service.

 * ``healthcheck_port``: an alternative port to use for healthchecking your
   service. This is not required; it defaults to the port your service instance
   is running on.

 * ``healthcheck_uri``: string specifying the URI which SmartStack should use to
   healthcheck the service. Defaults to ``/status``. This is ignored if
   ``healthcheck_mode`` is ``tcp``.

 * ``healthcheck_timeout_s``: maximum number of seconds that a nerve
   healthcheck may take; defaults to 1.

 * ``updown_timeout_s``: maximum number of seconds after which a service
   instance needs to be able to respond to healthchecks after updown_service has
   been called.

 * ``extra_healthcheck_headers``: Extra headers/values that are appended to the
   healthcheck requests. Example::

     extra_healthcheck_headers:
         X-Mode: ro

Routing and Reliability
```````````````````````

 * ``retries``: Number of HAProxy connection failure `retries <http://cbonte.github.io/haproxy-dconv/configuration-1.5.html#retries>`_,
   defaults to 1.

 * ``allredisp``: If set, haproxy will redispatch (choose a different server) on
   every connection retry. It only makes sense to set this option if you have a
   low connection timeout, and a number of retries > 1. This is useful for when
   machines crash or network partitions occur because your service doesn’t waste
   any retries on the dead server, and immediately redispatches to other
   functional backends. For example, for a latency sensitive service you may
   want to set ``timeout_connect_ms`` to 100ms, with 3-5 retries and
   ``allredisp`` set to ``true``.

 * ``timeout_connect_ms``: HAProxy `server connect timeout
   <http://cbonte.github.io/haproxy-dconv/configuration-1.5.html#4.2-timeout%20connect>`_
   in milliseconds, defaults to 200.
 * ``timeout_server_ms``: HAProxy `server inactivity timeout <http://cbonte.github.io/haproxy-dconv/configuration-1.5.html#4.2-timeout%20server>`_
   in milliseconds, defaults to 1000.
 * ``lb_policy``: Envoy `lb_policy https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/cluster/v3/cluster.proto#envoy-v3-api-enum-config-cluster-v3-cluster-lbpolicy`_
    Defaults to `"ROUND_ROBIN"`.
 * ``endpoint_timeouts``: Allows you to specify non-default server timeouts for
   specific endpoints. This is useful for when there is a long running endpoint
   that requires a large timeout value but you would like to keep the default
   timeout at a reasonable value.

   Endpoints use prefix-matching by default; for example ``/specials/bulk/v1``
   will match both ``/specials/bulk/v1/foo`` and ``/specials/bulk/v1/bar``.

   Endpoints can also use regex matching, provided that the regex string begins
   with a caret ``^`` and backslashes within the string are properly escaped.
   For example, ``^/specials/[^/]+/v2/\\d`` will match the endpoints
   ``/specials/bulk/v2/1`` and ``^/specials/milk/v2/2``.

   Example::

     endpoint_timeouts:
         "/specials/bulk/v1": 15000
         "^/specials/[^/]+/v2/\\d": 11000

Fault Injection
```````````````

These keys are meant to control the built-in fault and delay injection features
of Envoy, the new proxy we are using to replace HAProxy, and thus won't work
with the latter.

**Note:** The described Fault Injection features are currently only available
internally at Yelp.

 * ``fixed_delay``: A map of locations to delays. Controls the injection of a
   particular delay for a particular service, in a particular environment and
   for a particular percentage of requests. From the example: ::

      main:
        proxy_port: 20028
        timeout_server_ms: 5000
        fixed_delay:
          ecosystem:stagec:
            duration_ms: 3000
            percent: 20
          superregion:uswest1-prod:
            duration_ms: 1000
            percent: 40

   - ``duration_ms``: The duration of the delay in milliseconds.
   - ``percent``: Percentage of requests (0-100) to be randomly affected by this
     delay. Note that due to Envoy's current definition of *percentage* as an
     integer, this cannot be specified as a floating-point number.

Error Alerting
``````````````

These keys provide optional overrides for the default alerting behaviour.

 * ``monitoring``: Override default alerting behaviour. For example: ::

      main:
        monitoring:
          team: frontend
          slack_channel: "notifications"
          project: "FRONTEND"
          ticket: true
          page: false
          page_nonprod: true
          error_threshold_ratio: 0.02
          minimum_error_rps: 10
    - ``team``: Override the default team for alerting.
    - ``slack_channel``: Error alerts notify the first channel in the monitoring.yaml slack_channels list.
      Use this key if you prefer a different channel.
    - ``project``: Override the default JIRA project for alerting.
    - ``ticket``: Override the default ticketing behaviour. Error Alert ticketing defaults to **false** but also
      respects the ticketing behaviour set in the monitoring.yaml file. Override that here if required.
    - ``page``: Override the default paging behaviour. Error Alert paging defaults to **true** but also
      respects the paging behaviour set in the monitoring.yaml file. Override that here if required.
    - ``page_nonprod``: Override the default paging behaviour for non-production
      environments. Defaults to **false**.
    - ``error_threshold_ratio``: Error threshold ratio (0-1). Defaults to **0.01**.
    - ``minimum_error_rps``: Minimum error rate per second, minimum is zero. Defaults to **5**.

Moving a Service to a different location type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you don’t care about dropping traffic you can just change ``discover`` and
``advertise`` and then wait until the configuration and registrations propagate.
If you want to do it gracefully you have to ensure that nerve registrations have
time to propagate before you switch synapse’s ``discover`` key.

An example of switching from region to superregion discovery:

1a. Append to your advertise key::

    - advertise: [region]
    + advertise: [region, superregion]

1b. When moving from a large grouping to a smaller grouping (like
moving from superregion => region) you must add an additional constraint
to ensure Marathon balances the tasks evenly::

    extra_constraints: [['region', 'GROUP_BY', 2]]

2. (Optional) Use zkCli.sh to monitor your new registrations for each
superregion you are changing::

    $ /usr/share/zookeeper/bin/zkCli.sh
    [zk: localhost:2181(CONNECTING) 0] connect 10.40.5.6:22181
    [zk: 10.40.5.6:22181(CONNECTED) 1] ls /nerve/superregion:norcal-devc/servicename.main
    [host1-uswest1adevc_0000015910, host2-uswest1cdevc_0000015898, host3-uswest1cdevc_0000015893]
    [zk: 10.40.5.6:22181(CONNECTED) 2]

2b. Run ``paasta status -v`` to verify that Marathon has balanced services
across the infrastructure as expected.

3. Once zookeeper shows the proper servers, switch the discovery key::

    - discover: region
    + discover: superregion

4. Wait a while, usually about 10 minutes, Then change advertise to just superregion::

    - advertise: [region, superregion]
    + advertise: [superregion]

``monitoring.yaml``
-------------------

The yaml where monitoring for the service is defined.

Defaults for a *team* can be set globally with the global Sensu configuration.
``team`` is the only mandatory key, but overrides can be set for the entire
service with ``monitoring.yaml``.

Additionally these settings can be overridden on a *per-instance* basis via the
`monitoring` option. For example a ``canary`` instance can be set with
``page: false`` and ``team: devs``, while the ``main`` instance can bet set to
``page: true`` and ``team: ops``, and the ``dailyadsjob`` instance can be set
with ``ticket: true`` and ``team: ads``. See the Examples section for more
examples.

Here is a list of options that PaaSTA will pass through:

 * ``team``: Team that will be notified by Sensu.

 * ``page``: Boolean to indicate if an instance should page if it is failing.
   Defaults to **false**.

 * ``runbook``: An optional but *highly* recommended field. Try to use
   shortlinks when possible as sometimes the runbook url may need to be
   copied from a small screen.

 * ``tip``: An optional one-line version of the runbook to help with
   common issues. For example: "Check to see if it is Bing first!"

 * ``notification_email``: String representing an email address to send
   notifications to. This will default to the team email address if is is
   already set globally. For multiple emails, use a comma separated list.

 * ``irc_channels``: Array of irc_channels to post notifications to.

 * ``slack_channels``: Array of slack_channels to post notifications to.

 * ``ticket``: Boolean to indicate if an alert should make a JIRA ticket.
   Defaults to **false**.

 * ``project``: String naming the project where JIRA tickets will be created.
   Overrides the global default for the team.

 * ``priority``: A JIRA ticket priority to use. This value should be a string
   value like ``'0'``, ``'1'``, ``'3.14'``, etc. If not set, the default will
   be the ``default_priority`` setting for the sensu team or the default
   priority used for the JIRA project.

 * ``tags``: An list of tags that are used as labels when creating a JIRA
   ticket. Note that this list of tags does not overwrite the default values
   added for sensu checks (tags like ``SENSU`` for example), it just adds to
   that existing list.

 * ``component``: Array of components affected by this check. These are used as
   components when creating a JIRA ticket.

 * ``description``: A description giving more context on the check or event.
   This should be a longer expansion of information than what is included in
   the ``tip`` option.

 * ``alert_after``: Time string that represents how long a a check should be
   failing before an actual alert should be fired. Currently defaults to ``10m``
   for the replication alert.

 * ``realert_every``: An integer (not a time unit) representing how many checks
   to execute between sending alerts. The default settings is ``-1``, which is
   a special value indicating exponential backoff. For example, given a check
   that runs once per minute, a setting of ``-1`` would fire alerts on minutes
   1,2,4,8,16, etc.

   A setting of ``60`` would fire events on minutes 1, 61, 121, etc.

   How often alerts are actually sent out is a function of this ``realert_every``
   setting and the frequency at which a check runs, which in PaaSTA is once per
   minute.

 * ``check_every``: **Not a setting that can be configured!!** This setting is
   described for completeness. In PaaSTA the check interval is not user-configurable
   and is hard-coded at ``1m``.

 * ``check_oom_events``: Boolean to indicate if an instance should alert when
   the Out Of Memory killer kills processes in the instance containers.
   This alert sends an email to ``notification_email`` and post notifications
   to ``irc_channels``. It does not page. Defaults to **true**.


Monitoring Examples
^^^^^^^^^^^^^^^^^^^

An example of a service that only pages on a cluster called "prod"::

    # monitoring.yaml
    team: devs
    page: false

    # marathon-prod.yaml
    main:
      instances: 3
      monitoring:
         page: true

A service that pages everywhere, but only makes a ticket for a tron job::

    # monitoring.yaml
    team: backend
    page: true

    # tron-prod.yaml
    nightly_batch:
      schedule: .....
      monitoring:
        page: false
        ticket: true

A marathon/kubernetes service that overrides options on different instances (canary)::

    # monitoring.yaml
    team: frontend
    page: false

    # marathon-prod.yaml or kubernetes-prod.yaml
    main:
      instances: 20
      monitoring:
        team: ops
        page: true
    canary:
      instances: 1
      registrations: ['service.main']
      monitoring:
        page: false
        ticket: true

A tron job that pages for a specific job in a service::

  # monitoring.yaml
  team: midend
  page: false

  # tron-prod.yaml
  jobs:
    foo:
      monitoring:
        page: true

``service.yaml``
----------------

Various PaaSTA utilities look at the following keys from service.yaml

 * ``git_url``
 * ``description``
 * ``external_link``
 * ``docker_registry`` This is optional. Set this to override the `system-wide docker registry <system_configs.html#configuration-options>`_, and specify an alternate docker registry for your service.

Where does paasta_tools look for yelpsoa-configs?
-------------------------------------------------------------

By default, paasta_tools uses the system yelpsoa-configs dir,
``/nail/etc/services``. Scripts should allow this to be overridden with ``-d``
or ``--soa-dir``. Normally you would only do this for testing or debugging.
