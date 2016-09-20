Preparation: paasta_tools and yelpsoa-configs
=========================================================


paasta_tools reads configuration about services from several YAML
files in `soa-configs <soa_configs.html>`_:

``marathon-[clustername].yaml``
-------------------------------

e.g. ``marathon-norcal-prod.yaml``, ``marathon-mesosstage.yaml``. The
clustername is usually the same as the ``superregion`` in which the cluster
lives (``norcal-prod``), but not always (``mesosstage``). It MUST be all
lowercase. (non alphanumeric lowercase characters are ignored)

**Note:** All values in this file except the following will cause PaaSTA to
`bounce <workflow.html#bouncing>`_ the service:

.. program-output:: python -c "from paasta_tools.marathon_tools import CONFIG_HASH_BLACKLIST; print ', '.join(CONFIG_HASH_BLACKLIST)"

Top level keys are instancenames, e.g. ``main`` and ``canary``. Each
instance MAY have:

  * ``cpus``: Number of CPUs an instance needs. Defaults to .25. CPUs in Mesos
    are "shares" and represent a minimal amount of a CPU to share with a task
    relative to the other tasks on a host.  A task can burst to use any
    available free CPU, but is guaranteed to get the CPU shares specified.  For
    a more detailed read on how this works in practice, see the docs on `isolation <isolation.html>`_.

  * ``mem``: Memory (in MB) an instance needs. Defaults to 1024 (1GB). In Mesos
    memory is constrained to the specified limit, and tasks will reach
    out-of-memory (OOM) conditions if they attempt to exceed these limits, and
    then be killed.  There is currently not way to detect if this condition is
    met, other than a ``TASK_FAILED`` message. For more a more detailed read on
    how this works, see the docs on `isolation <isolation.html>`_

  * ``disk``: Disk (in MB) an instance needs. Defaults to 1024 (1GB). In Mesos
    disk is constrained to the specified limit, and tasks will recieve 'No space
    left on device' errors if they attempt to exceed these limits, and then be
    unable to write any more data to disk.

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

  * ``nerve_ns``: Specifies that this namespace should be routed to by another
    namespace in SmartStack. In SmartStack, each service has difference pools
    of backend servers that are listening on a particul port. In PaaSTA we call
    these "Nerve Namespaces". By default, the Namespace assigned to a particular
    instance in PaaSTA has the *same name*, so the ``main`` instance will correspond
    to the ``main`` Nerve namespace defined in ``smartstack.yaml``. This ``nerve_ns``
    option allows users to make particular instances appear under an *alternative*
    namespace. For example ``canary`` instances can have ``nerve_ns: main`` to route
    their traffic to the same pool as the other ``main`` instances.

  * ``backoff_factor``: PaaSTA will automatically calculate the duration of an
    application's backoff period in case of a failed launch based on the number
    of instances. For each consecutive failure that duration is multipled by
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

  * ``bounce_method``: Controls the bounce method; see `bounce_lib <generated/paasta_tools.bounce_lib.html>`_

  * ``bounce_method_params``: A dictionary of parameters for the specified bounce_method.

    * ``check_haproxy``: Boolean indicating if PaaSTA should check the local
      haproxy to make sure this task has been registered and discovered
      (Defaults to ``True`` if service is in SmartStack)

    * ``min_task_uptime``: Minimum number of seconds that a task must be
      running before we consider it healthy (Disabled by default)

  * ``bounce_margin_factor``: proportionally increase the number of old instances
    to be drained when the crossover bounce method is used.
    0 < bounce_margin_factor <= 1. Defaults to 1 (no influence).
    This allows bounces to proceed in the face of a percentage of failures.
    It doesn’t affect any other bounce method but crossover.

  * ``drain_method``: Controls the drain method; see `drain_lib
    <generated/paasta_tools.drain_lib.html>`_. Defaults to ``noop`` for
    instances that are not in Smartstack, or ``hacheck`` if they are.

  * ``drain_method_params``: A dictionary of parameters for the specified
    drain_method. Valid parameters are any of the kwargs defined for the
    specified bounce_method in `drain_lib <generated/paasta_tools.drain_lib.html>`_.

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

  * ``cmd``: The command that is executed. Can be used as an alternative to
    args for containers without an `entrypoint
    <https://docs.docker.com/reference/builder/#entrypoint>`_. This value is
    wrapped by Mesos via ``/bin/sh -c ${app.cmd}``. Parsing the Marathon config
    file will fail if both args and cmd are specified [#note]_.

  * ``args``: An array of docker args if you use the `"entrypoint"
    <https://docs.docker.com/reference/builder/#entrypoint>`_ functionality.
    Parsing the Marathon config file will fail if both args and cmd are
    specified [#note]_.

  .. _env:

  * ``env``: A dictionary of environment variables that will be made available
    to the container. PaaSTA additionally will inject the following variables:

    * ``PAASTA_SERVICE``: The service name
    * ``PAASTA_INSTANCE``: The instance name
    * ``PAASTA_CLUSTER``: The cluster name
    * ``PAASTA_DOCKER_IMAGE``: The docker image name

    Additionally, when scheduled under Marathon, there are ``MARATHON_`` prefixed variables available.
    See the `docs <https://mesosphere.github.io/marathon/docs/task-environment-vars.html>`_ for more information about these variables.

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

    **WARNING**: This option should be used sparingly. Any specified bind-mount
    must exist on the filesystem beforehand, or the container will not run.
    Additionally it is possible for a service to be defined with a read-write
    volume on a sensitive part of the filesystem, as root. PaaSTA does not
    validate that the bind mounts are "safe".

  * ``monitoring``: A dictionary of values that configure overrides for
    monitoring parameters that will take precedence over what is in
    `monitoring.yaml`_. These are things like ``team``, ``page``, etc.

  * ``autoscaling``: See the `autoscaling docs <autoscaling.html>`_ for valid options and how they work

    * ``metrics_provider``: Which method PaaSTA will use to determine a service's utilization.

    * ``decision_policy``: Which method PaaSTA will use to determine when to autoscale a service.

  * ``deploy_blacklist``: A list of lists indicating a set of locations to *not* deploy to. For example:

      ``deploy_blacklist: [["region", "uswest1-prod"]]``

   would indicate that PaaSTA should not deploy the service to the ``uswest1-prod`` region. By default the ``monitoring_blacklist`` will use the ``deploy_blacklist`` if it exists.

  * ``deploy_whitelist``: A list of lists indicating a set of locations where deployment is allowed.  For example:

      ``deploy_whitelist: ['region', ['uswest1-prod", 'uswest2-prod]]``

    would indicate that PaaSTA can **only** deploy in ``uswest1-prod`` or ``uswest2-prod``.  If this list
    is empty (the default), then deployment is allowed anywhere.  This is superseded by the blacklist; if
    a host is both whitelisted and blacklisted, the blacklist will take precedence.  Only one location type
    of whitelisting may be specified.

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
    to. The ``step`` parameter in ``deploy.yaml`` refererences this value
    to determine the order in which to build & deploy deploy groups. Defaults to
    ``clustername.instancename``. See the deploy group doc_ for more information.

  * ``replication_threshold``: An integer representing the percentage of instances that
    need to be available for monitoring purposes. If less than ``replication_threshold``
    percent instances of a service's backends are not available, the monitoring
    scripts will send a CRITICAL alert.

In addition, each instancename MAY configure additional Marathon healthcheck
options:

  * ``healthcheck_mode``: One of ``cmd``, ``tcp``, or ``http``. If your
    service uses Smartstack, then this must match the value of the ``mode`` key
    defined for this instance in ``smartstack.yaml``. If set to ``cmd`` then
    PaaSTA will execute ``healthcheck_cmd`` and examine the return code.

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


Many of these keys are passed directly to Marathon. Their docs aren't super
clear about all these but start there:
https://mesosphere.github.io/marathon/docs/rest-api.html

Notes:

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

``chronos-[clustername].yaml``
------------------------------

The yaml where Chronos jobs are defined. Top-level keys are the job names.

Most of the descriptions below are taken directly from the Chronos API docs,
which can be found here:
https://mesos.github.io/chronos/docs/api.html#job-configuration

Each job configuration MUST specify the following options:

  * One of ``schedule`` and ``parents``. If both are present, then ``schedule``
    takes precedence and ``parents`` is ignored.

Each job configuration MAY specify the following options:

  * ``schedule``: When the job should run. The value must be specified in the
    cryptic ISO 8601 format. For more details about the schedule format, see:
    https://en.wikipedia.org/wiki/ISO_8601 and
    https://mesos.github.io/chronos/docs/api.html#adding-a-scheduled-job

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
    <https://pythonhosted.org/tron/command_context.html#built-in-command-context-variables>`_
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

  * ``cpus``: See the `marathon-[clustername].yaml`_ section for details

  * ``mem``: See the `marathon-[clustername].yaml`_ section for details

  * ``bounce_method``: Controls what happens to the old version(s) of a job
    when a new version is deployed. Currently the only option is ``graceful``,
    which disable the old versions but allows them to finish their current run.
    If unspecified, defaults to ``graceful``.

  * ``monitoring``: See the `marathon-[clustername].yaml`_ section for details

  * ``env``: See the `marathon-[clustername].yaml`_ section for details

  * ``extra_volumes``: See the `marathon-[clustername].yaml`_ section for details

  * ``constraints``: Array of rules to ensure jobs run on slaves with specific
    Mesos attributes. See the `official documentation
    <https://mesos.github.io/chronos/docs/api.html#constraints>`_ for more
    information.

  * ``extra_constraints``: Adds to the default placement constraints for
    services. This acts the same as ``constraints``, but adds to the default
    constraints instead of replacing them. See ``constraints`` for details on
    format and the default constraints.

    *Note*: While this parameter is the same as ``extra_constraints`` in ``marathon-$cluster.yaml``,
    the Marathon constrain language isn't exactly like the Marathon constraint language.
    Be sure to read the constraint documentation for Chronos referenced in the ``constraints``
    section.

  * ``pool``: See the `marathon-[clustername].yaml`_ section for details

  * ``deploy_whitelist``: See the `marathon-[clustername].yaml`_ section for details

  * ``deploy_blacklist``: *Not currently supported*.

  * ``deploy_group``: Same as ``deploy_group`` for marathon-*.yaml.

  * ``schedule_time_zone``: The time zone name to use when scheduling the job.
    Unlike schedule, this is specified in the tz database format, not the ISO 8601 format.

    * This field takes precedence over any time zone specified in schedule.
    * See list of `tz database time zones <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.
    * For example, the effective time zone for the following is America/Los_Angeles::


        ---
        main:
          schedule: R/2014-10-10T18:32:00Z/PT60M
          schedule_time_zone: America/Los_Angeles

.. _doc: deploy_groups.html

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
``nerve_ns``.

We now describe which keys are supported within a namespace.  Note that all but
proxy_port are optional.

Available Options
~~~~~~~~~~~~~~~~~

Basic HTTP and TCP options
``````````````````````````

 * ``proxy_port``: integer-valued port on which HAProxy listens for requests.
   This must be unique across all environments where PaaSTA (or synapse) runs.
   At Yelp, we pick from the range [20000, 21000]. Feel free to pick the next
   available value -- paasta fsm will do this for you automatically!

 * ``mode``: string of value ``http`` or ``tcp``, specifying whether the service
   is an HTTP or TCP service respectively.  Defaults to ``http``.

 * ``extra_headers``: for use in http mode. Headers that should be added to the
   request before being forwarded to the server. Example: ::

      extra_headers:
        X-Mode: ro

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

Additionally these settings can be overridden on a *per-instance* basis. For
example a ``canary`` instance can be set with ``page: false`` and ``team:
devs``, while the ``main`` instance can bet set to ``page: true`` and ``team:
ops``, and the ``dailyadsjob`` instance can be set with ``ticket: true`` and
``team: ads``. See the Examples section for more examples.

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

 * ``ticket``: Boolean to indicate if an alert should make a JIRA ticket.
   Defaults to **false**.

 * ``project``: String naming the project where JIRA tickets will be created.
   Overrides the global default for the team.

 * ``alert_after``: Time string that represents how long a a check should be
   failing before an actual alert should be fired. Currently defaults to ``2m``
   for the replication alert.


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
      nerve_ns: main
      monitoring:
        page: false
        ticket: true


``service.yaml``
----------------

Various PaaSTA utilities look at the following keys from service.yaml

 * ``git_url``
 * ``description``
 * ``external_link``

Where does paasta_tools look for yelpsoa-configs?
-------------------------------------------------------------

By default, paasta_tools uses the system yelpsoa-configs dir,
``/nail/etc/services``. Scripts should allow this to be overridden with ``-d``
or ``--soa-dir``. Normally you would only do this for testing or debugging.
