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

  * ``cpus``: Number of CPUs an instance needs. Defaults to .25. CPUs in Mesos
    are "shares" and represent a minimal amount of a CPU to share with a task
    relative to the other tasks on a host.  A task can burst to use any
    available free CPU, but is guaranteed to get the CPU shares specified.  For
    a more detailed read on how this works in practice, see the docs on `isolation <isolation.html>`_.

  * ``cpu_burst_add``: Maximum number of additional CPUs an instance may use
    while bursting; if unspecified, PaaSTA defaults to 1. For example, if a
    service specifies that it needs 2 CPUs normally and 1 for burst, the service
    may go up to 3 CPUs, if needed.

  * ``mem``: Memory (in MB) an instance needs. Defaults to 1024 (1GB). In Mesos
    memory is constrained to the specified limit, and tasks will reach
    out-of-memory (OOM) conditions if they attempt to exceed these limits, and
    then be killed.  There is currently not way to detect if this condition is
    met, other than a ``TASK_FAILED`` message. For more a more detailed read on
    how this works, see the docs on `isolation <isolation.html>`_

  * ``docker_init``: Bool. If set ``false``, will disable the ``--init`` functionality
    of Docker. Without ``--init``, it is up to the user to properly respond to
    signals when behaving as PID #1 in a container. (See
    `dumb-init <https://github.com/Yelp/dumb-init#why-you-need-an-init-system>`_
    as an example of how to run a program in a container properly without
    ``--init``) Defaults to ``true``.

  * ``env``: A dictionary of environment variables that will be made available
    to the container. PaaSTA additionally will inject the following variables:

    * ``PAASTA_SERVICE``: The service name
    * ``PAASTA_INSTANCE``: The instance name
    * ``PAASTA_CLUSTER``: The cluster name
    * ``PAASTA_DOCKER_IMAGE``: The docker image name
    * ``PAASTA_DEPLOY_GROUP``: The `deploy group <deploy_group.html>`_ specified
    * ``PAASTA_MONITORING_TEAM``: The team that is configured to get alerts.
    * ``PAASTA_LAUNCHED_BY``: May not be present. If present, will have the username
      of the user who launched the paasta container.

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


``Placement Options (Constraints)``
-----------------------------------

Constraint options control how Mesos schedules a task, whether it is scheduled by
Marathon, Chronos, Tron, or ``paasta remote-run``.

  * ``deploy_blacklist``: A list of lists indicating a set of locations to *not* deploy to. For example:

      ``deploy_blacklist: [["region", "uswest1-prod"]]``

   would indicate that PaaSTA should not deploy the service to the ``uswest1-prod`` region. By default the ``monitoring_blacklist`` will use the ``deploy_blacklist`` if it exists.

  * ``deploy_whitelist``: A list of lists indicating a set of locations where deployment is allowed.  For example:

      ``deploy_whitelist: ["region", ["uswest1-prod", "uswest2-prod"]]``

    would indicate that PaaSTA can **only** deploy in ``uswest1-prod`` or ``uswest2-prod``.  If this list
    is empty (the default), then deployment is allowed anywhere.  This is superseded by the blacklist; if
    a host is both whitelisted and blacklisted, the blacklist will take precedence.  Only one location type
    of whitelisting may be specified.

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

  * ``pool``: Changes the "pool" constrained automatically added to all PaaSTA
    Marathon apps. The default pool is ``default``, which equates to::

       ["pool", "LIKE", "default"]

    This constraint is automatically appended to the list of constraints for
    a service unless overridden with the ``constraints`` input.

    Warning: In order for an service to be launched in a particular pool, there
    *must* exist some Mesos slaves that already exist with that particular
    pool attribute set.

``marathon-[clustername].yaml``
-------------------------------

e.g. ``marathon-norcal-prod.yaml``, ``marathon-mesosstage.yaml``. The
clustername is usually the same as the ``superregion`` in which the cluster
lives (``norcal-prod``), but not always (``mesosstage``). It MUST be all
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

  * Anything in the `Placement Options (Constraints)`_.

  * ``disk``: Disk (in MB) an instance needs. Defaults to 1024 (1GB). Disk limits
    may or may not be enforced, but services should set their ``disk`` setting
    regardless to ensure the scheduler has adequate information for distributing
    tasks.

  * ``ulimit``: Dictionary of ulimit values that are passed to Docker. Defaults
    to empty dictionary. Each ulimit value is a dictionary with the soft limit
    specified under the 'soft' key and the optional hard limit specified under
    the 'hard' key. Ulimit values that are not set are inherited from the
    default ulimits set on the Docker daemon. Example::

      ulimit:
        - nofile: {"soft": 1024, "hard": 2048}
        - nice: {"soft": 20}

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

  * ``bounce_priority``: an integer priority that informs paasta-deployd which service
    instances should take priority over each other. The default priority is 0 and higher numbers
    are considered higher priority. For example: if there are three service instances that need
    bouncing: the first with a ``bounce_priority`` -1, the second with no ``bounce_priority`` and the
    third with ``bounce_priority`` 1. Then paasta-deployd will prioritise the bounce of the third
    service instance, then the second service instance and finally the first service instance.

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

  * ``monitoring_blacklist``: A list of lists indicating a set of locations to
    *not* monitor for Smartstack replication. For example:

      ``monitoring_blacklist: [["region", "uswest1-prod"]]``

   would indicate that PaaSTA should ignore the ``uswest1-prod`` region. PaaSTA
   currently assumes that the instance count in *other* regions include
   instances that would have otherwise gotten deployed to ``uswest1-prod``. In
   other words, the ``monitoring_blacklist`` assumes that instances are not
   deployed there as well. For example, suppose the total instance count was
   10, and there are two regions, one of which is blacklisted.  The monitoring
   logic will assume that there are no instances in the blacklisted region,
   implying that we should expect all 10 in the non-blacklisted region.

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
    task if this many healthchecks fail consecutively. Defaults to 6 attempts.

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

``chronos-[clustername].yaml``
------------------------------

The yaml where Chronos jobs are defined. Top-level keys are the job names.

NB: Yelp maintains its own fork of Chronos at https://github.com/Yelp/chronos,
and this is the version deployed in the paasta clusters at Yelp. The fork is
based off the 2.4 release of upstream Chronos. The most notable change is the
support for specifying schedules in crontab format, but also contains various
stability fixes. We have not backported any of the new features to hit 3.0.
Consequently, the list shown here is the most accurate documentation of
supported fields; the docs upstream may describe keys that are not supported or
have different behaviour to those mentioned here.

Each job configuration MUST specify the following options:

  * One of ``schedule`` and ``parents``. If both are present, then ``schedule``
    takes precedence and ``parents`` is ignored.

Each job configuration MAY specify the following options:

  * Anything in the `Common Settings`_.

  * Anything in the `Placement Options (Constraints)`_.

  * ``schedule``: When the job should run. This can be in either ISO8601 notation,
    or in cron notation.  For more details about ISO8601 formats, see the
    `wikipedia page <https://en.wikipedia.org/wiki/ISO_8601>`_; for more details on the Cron format,
    see `crontab(5) <http://man7.org/linux/man-pages/man5/crontab.5.html>`_. Note that
    the extensions mentioned in that page are *not* supported at this time.

    * **Note:** Although Chronos supports an empty start time to indicate that
      the job should start immediately, we do not allow this. In a situation
      such as restarting Chronos, all jobs with empty start times would start
      simultaneously, causing serious performance degradation and ignoring the
      fact that the job may have just run.

    * **Warning**: Chronos does *not* allow overlapping jobs. If a job has a
      ``schedule`` set to repeat every hour, and the task takes longer than
      an hour, Chronos will *not* schedule the next task while the previous
      one is still running. (if N starts and overflows to the next time slot,
      N+1 and any future runs will be canceled until N finishes)

  * ``parents``: An array of parents jobs. If specified, then the job will not run
    until *all* of the jobs in this array have completed. The parents jobs should be
    in the form of ``service.instance``. For example::

        cat myservice/chronos-testcluster.yml
        ---
        job_one:
          schedule: R/2014-10-10T18:32:00Z/PT60M

        job_two:
          schedule: R/2014-10-10T19:32:00Z/PT60M

        child_job:
          parents:
            - myservice.parent_one
            - myservice.parent_two



  * ``cmd``: See the `marathon-[clustername].yaml`_ section for details
    Additionally ``cmd`` strings with time or date strings that Tron
    understands will be interpreted and replaced. ``shortdate``, ``year``,
    ``month``, ``day``, and ``daynumber`` are supported. Read more in the
    official `tron documentation
    <http://tron.readthedocs.io/en/latest/command_context.html#built-in-cc>`_
    for more information on how to use these variables.

    * **WARNING**: Chronos ``cmd`` parsing is done via `python string
      replacement
      <https://docs.python.org/2/library/string.html#format-string-syntax>`_,
      which means that the special character strings like ``%`` must
      be escaped in order to be used literally.

  * ``args``: See the `marathon-[clustername].yaml`_ section for details

  * ``epsilon``: If Chronos misses the scheduled run time for any reason, it
    will still run the job if the time is within this interval. The value must
    be formatted like an ISO 8601 Duration. See:
    https://en.wikipedia.org/wiki/ISO_8601#Durations. Defaults to 'PT60S',
    indicating that a job may be launched up to a minute late.

  * ``retries``: Number of retries to attempt if a command returns a
    non-zero exit status. Defaults to 2.

  * ``net``: Specify which kind of
    `networking mode <https://docs.docker.com/engine/reference/run/#network-settings>`_
    instances of this service should be launched using. Defaults to ``'bridge'``.

  * ``disabled``: If set to ``True``, this job will not be run. Defaults to ``False``

  * ``bounce_method``: Controls what happens to the old version(s) of a job
    when a new version is deployed. Currently the only option is ``graceful``,
    which disable the old versions but allows them to finish their current run.
    If unspecified, defaults to ``graceful``.

  * ``monitoring``: See the `monitoring.yaml`_ section for details.

  * ``deploy_group``: Same as ``deploy_group`` for marathon-\*.yaml.

  * ``schedule_time_zone``: The time zone name to use when scheduling the job.
    Unlike schedule, this is specified in the tz database format, not the ISO 8601 format.

    * This field takes precedence over any time zone specified in schedule.
    * See list of `tz database time zones <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.
    * For example, the effective time zone for the following is America/Los_Angeles::


        ---
        main:
          schedule: R/2014-10-10T18:32:00Z/PT60M
          schedule_time_zone: America/Los_Angeles

``tron-[tron-clustername].yaml``
--------------------------------

This file stores configuration for periodically scheduled jobs for execution on
`Tron <https://github.com/yelp/tron>`_.

The documentation here is for the PaaSTA-specific options. For all other
settings, please see the
`canonical docs <https://tron.readthedocs.io/en/latest/jobs.html>`_.

.. warning:: The PaaSTA-Tron Integration is currently in an ALPHA state. Do not use it unless directed to.

Example Job
^^^^^^^^^^^

::

  jobs:
      - name: convert_logs
        node: node1
        schedule:
          start_time: 04:00:00
        actions:
          - name: verify_logs_present
            command: "ls /var/log/app/log_%(shortdate-1)s.txt"
            executor: ssh
          - name: convert_logs
            requires: [verify_logs_present]
            command: "convert_logs /var/log/app/log_%(shortdate-1)s.txt /var/log/app_converted/log_%(shortdate-1)s.txt"
            executor: paasta
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

  * Anything in the `Placement Options (Constraints)`_.

  * ``service``: Uses a docker image from different service. When ``service`` is set
    for an action, that setting takes precedence over what is set for the job.

  * ``executor``: Configures Tron to execute the command in a particular way.
    Set to ``paasta`` to configure Tron to launch the job on he PaaSTA cluster.
    Defaults to ``ssh``, which is the classic Tron execution method. When ``executor``
    is NOT ``paasta`` (and is using ``ssh``), all of these paasta-specific options
    listed here in this documentation will have no effect. It is OK to have a job
    composed of mixed ``paasta`` and ``ssh`` actions.

  * ``deploy_group``: Same setting as the ``Job``, but on a per-action basis. Defaults
    to the setting for the entire job.

  * ``command``: The command to run. If the action is configured with ``executor: paasta``,
    then the command should be something available in the docker container (it should NOT
    start with ``paasta local-run``).

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

Configure service registration, discovery, and load balancing.

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
   PaaSTA (or synapse) runs. At Yelp, we pick from the range [20000, 21000].
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
 * ``timeout_client_ms``: HAProxy `client inactivity timeout <http://cbonte.github.io/haproxy-dconv/configuration-1.5.html#4.2-timeout%20client>`_
   in milliseconds, defaults to 1000.

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
   failing before an actual alert should be fired. Currently defaults to ``2m``
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
   to ``irc_channels``. It neither pages nor makes a JIRA ticket. Defaults to **true**.


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

A service that pages everywhere, but only makes a ticket for a chronos job::

    # monitoring.yaml
    team: backend
    page: true

    # chronos-prod.yaml
    nightly_batch:
      schedule: .....
      monitoring:
        page: false
        ticket: true

A marathon service that overrides options on different instances (canary)::

    # monitoring.yaml
    team: frontend
    page: false

    # marathon-prod.yaml
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
