Preparation: paasta_tools and yelpsoa-configs
=========================================================

paasta_tools reads configuration about services from several YAML
files in `soa-configs <soa_configs.html>`_:

marathon-[clustername].yaml
---------------------------

e.g. ``marathon-norcal-prod.yaml``, ``marathon-mesosstage.yaml``. The
clustername is usually the same as the ``superregion`` in which the cluster
lives (``norcal-prod``), but not always (``mesosstage``). It MUST be all
lowercase. (non alphanumeric lowercase characters are ignored)

The yaml where marathon jobs are actually defined.

Top level keys are instancenames, e.g. ``main`` and ``canary``. Each
instancename MAY have:

  * ``cpus``: Number of CPUs an instance needs.

  * ``mem``: Memory (in MB) an instance needs.

  * ``instances``: Marathon will attempt to run this many instances of the Service

  * ``nerve_ns``: Specifies that this namespace should be routed to by another
    namespace. E.g. ``canary`` instances have a different configuration but
    traffic from the ``main`` pool reaches them.

  * ``bounce_method``: Controls the bounce method; see `bounce_lib <bounce_lib.html>`_

  * ``bounce_method_params``: A dictionary of parameters for the specified bounce_method.

    * ``check_haproxy``: Boolean indicating if PaaSTA should check the local
      haproxy to make sure this task has been registered and discovered
      (Defaults to ``True`` if service is in Smartstack)

    * ``min_task_uptime``: Minimum number of seconds that a task must be
      running before we consider it healthy (Disabled by default)

  * ``drain_method``: Controls the drain method; see `drain_lib
    <drain_lib.html>`_. Defaults to ``noop`` for instances that are not in
    Smartstack, or ``hacheck`` if they are.

  * ``drain_method_params``: A dictionary of parameters for the specified
    drain_method. Valid parameters are any of the kwargs defined for the
    specified bounce_method in `drain_lib <drain_lib.html>`_.

  * ``constraints``: Specifies placement constraints for services. Should be
    defined as an array within an array (E.g ``[["habitat", "GROUP_BY"]]``).
    Defaults to ``[["<discover_location_type>, "GROUP_BY"]]`` where
    ``<discover_location_type>`` is defined by the ``discover`` attribute in
    ``smartstack.yaml``. For more details and other constraint types, see the
    official `Marathon constraint documentation
    <https://mesosphere.github.io/marathon/docs/constraints.html>`_.

  * ``cmd``: The command that is executed. Can be used as an alternative to
    args for containers without an `entrypoint
    <https://docs.docker.com/reference/builder/#entrypoint>`_. This value is
    wrapped by Mesos via ``/bin/sh -c ${app.cmd}``. Parsing the Marathon config
    file will fail if both args and cmd are specified [#note]_.

  * ``args``: An array of docker args if you use the `"entrypoint"
    <https://docs.docker.com/reference/builder/#entrypoint>`_ functionality.
    Parsing the Marathon config file will fail if both args and cmd are
    specified [#note]_.

  * ``env``: A dictionary of environment variables that will be made available
    to the container.

    * **WARNING**: A PORT variable is provided to the docker image, but it
      represents the EXTERNAL port, not the internal one. The internal service
      MUST listen on 8888, so this PORT variable confuses some service stacks
      that are listening for this variable. Such services MUST overwrite this
      environment variable to function. (``PORT=8888 ./uwsgi.py```) We tried
      to work around this, see `PAASTA-267
      <https://jira.yelpcorp.com/browse/PAASTA-267>`_.

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

  * ``deploy_blacklist``: A list of lists indicating a set of locations to *not* deploy to. For example:

      ``deploy_blacklist: [["region", "uswest1-prod"]]``

   would indicate that PaaSTA should not deploy the service to the ``uswest1-prod`` region. By default the ``monitoring_blacklist`` will use the ``deploy_blacklist`` if it exists.

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

In addition, each instancename MAY configure additional Marathon healthcheck
options:

  * ``healthcheck_mode``: One of ``cmd``, ``tcp``, or ``http``. If your
    service uses Smartstack, then this must match the value of the ``mode`` key
    defined for this instance in ``smartstack.yaml``. If set to ``cmd`` then
    PaaSTA will execute ``healthcheck_cmd`` and examine the return code.

  * ``healthcheck_cmd``: If ``healthcheck_mode`` is set to ``cmd``, then this
    command is executed inside the container as a healthcheck. It must exit
    with status code 0 to signify a successful healthcheck. Any other exit code
    is treated as a failure. Defaults to ``/bin/true`` (that is, always
    indicate good health) if ``healthcheck_mode`` is ``cmd``.

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

chronos-[clustername].yaml
--------------------------

The yaml where Chronos jobs are defined. Top-level keys are the job names.

Most of the descriptions below are taken directly from the Chronos API docs,
which can be found here:
https://mesos.github.io/chronos/docs/api.html#job-configuration

Each job configuration MUST specify the following options:

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

      In PaaSTA this can be worked around to some degree
      by using the ``cmd`` time parsing documented below. For example, if
      a job is scheduled to run every 24 hours, and a ``%(day)`` variable
      substitution is used, PaaSTA will create a new job for *each* new day,
      allowing the previous job to take more than 24 hours.

Each job configuration MAY specify the following options:

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

    * **Note**: Using dynamic date variables in a ``cmd`` will cause PaaSTA
      to create unique jobs, which can allow the unique jobs to overlap
      in their runtime. See the documentation on ``schedule`` for more details.

  * ``args``: See the `marathon-[clustername].yaml`_ section for details

  * ``epsilon``: If Chronos misses the scheduled run time for any reason, it
    will still run the job if the time is within this interval. The value must
    be formatted like an ISO 8601 Duration. See:
    https://en.wikipedia.org/wiki/ISO_8601#Durations. Defaults to 'PT60S',
    indicating that a job may be launched up to a minute late.

  * ``retries``: Number of retries to attempt if a command returns a
    non-zero exit status. Defaults to 2.

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

smartstack.yaml
---------------

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

Available Options
~~~~~~~~~~~~~~~~~

Basic HTTP and TCP options
``````````````````````````

 * ``proxy_port``: integer-valued port on which HAProxy listens for requests.
   This must be unique across all environments where PaaSTA (or synapse) runs.
   At Yelp, we pick from the range [20000, 21000]. Feel free to pick the next
   available value -- paasta fsm will do this for you automatically!

 * ``mode``: string of value ``http`` or ``tcp``, specifying whether the service is an HTTP or TCP service respectively.  Defaults to ``http``.

 * ``extra_headers``: for use in http mode. Headers that should be added to the request before being forwarded to the server. Example: ::

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

 * ``discover``: Location type that smartstack should discover your service
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

 * ``healthcheck_uri``: string specifying the URI which SmartStack should use to
   healthcheck the service. Defaults to ``/status``. This is ignored for TCP
   services.

 * ``healthcheck_port``: an alternative port to use for healthchecking your
   service. This is not required; it defaults to the port your service instance
   is running on.

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

monitoring.yaml
---------------

The yaml where monitoring for the service is defined.

Defaults for a *team* can be set globally with the global Sensu configuration
(distributed via Puppet). ``team`` is the only mandatory key, but overrides can
be set for the entire service with ``monitoring.yaml``.

Additionally these settings can be overridden on a *per-instance* basis. For
example a ``canary`` instance can be set with ``page: false`` and ``team:
devs``, while the ``main`` instance can bet set to ``page: true`` and ``team:
ops``, and the ``dailyadsjob`` instance can be set with ``ticket: true`` and
``team: ads``.

Here is a list of options that PaaSTA will pass through:

 * ``team``: Team that will be notified by Sensu

 * ``page``: Boolean to indicate if an instance should alert PagerDuty if it is failing.

 * ``runbook``: An optional but *highly* recommended field. Try to use
   shortlinks (y/rb-my-service) when possible as sometimes the runbook url
   may need to be copied from a small screen.

 * ``tip``: An optional one-line version of the runbook to help with
   common issues. For example: "Check to see if it is bing first!"

 * ``notification_email``: String representing an email address to send
   notifications to. This will default to the team email address if is is
   already set globally. For multiple emails, use a comma separated list.

 * ``irc_channels``: Array of irc_channels to post notifications to.

 * ``ticket``: Boolean to indicate if an alert should make a JIRA ticket.

 * ``project``: String naming the project where JIRA tickets will be created.
   Overrides the global default for the team.

 * ``alert_after``: Time string that represents how long a a check should be
   failing before an actual alert should be fired. Currently defaults to ``2m``
   for the replication alert.


service.yaml
------------

Various PaaSTA utilities look at the following keys from service.yaml

 * ``git_url``
 * ``description``
 * ``external_link``

Where does paasta_tools look for yelpsoa-configs?
-------------------------------------------------------------

By default, paasta_tools uses the system yelpsoa-configs dir,
``/nail/etc/services``. Scripts should allow this to be overridden with ``-d``
or ``--soa-dir``. Normally you would only do this for testing or debugging.
