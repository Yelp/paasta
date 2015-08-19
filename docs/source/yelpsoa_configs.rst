Preparation: paasta_tools and yelpsoa-configs
=========================================================

paasta_tools reads configuration about services from several YAML
files in `yelpsoa-configs <http://y/cep319>`_:

marathon-[clustername].yaml
---------------------------

e.g. ``marathon-norcal-prod.yaml``, ``marathon-mesosstage.yaml``. The
clustername is usually the same as the ``superregion`` in which the cluster
lives (``norcal-prod``), but not always (``mesosstage``). It MUST be all
lowercase. (non alphanumeric lowercase characters are ignored)

The yaml where marathon jobs are actually defined.

Top level keys are instancenames, e.g. ``main`` and ``canary``. Each instancename MAY have:

  * ``cpus``: Number of CPUs an instance needs.

  * ``mem``: Memory (in MB) an instance needs.

  * ``instances``: Marathon will attempt to run this many instances of the Service

  * ``nerve_ns``: Specifies that this namespace should be routed to by another namespace. E.g. ``canary`` instances have a different configuration but traffic from the ``main`` pool reaches them.

  * ``bounce_method``: Controls the bounce method; see `bounce_lib <bounce_lib.html>`_

  * ``bounce_method_params``: A dictionary of parameters for the specified bounce_method.

    * ``check_haproxy``: Boolean indicating if PaaSTA should check the local haproxy to make sure this task has been registered and discovered (Defaults to ``True`` if service is in Smartstack)

    * ``min_task_uptime``: Minimum number of seconds that a task must be running before we consider it healthy (Disabled by default)

  * ``drain_method``: Controls the drain method; see `drain_lib <drain_lib.html>`_. Defaults to ``noop`` for instances that are not in Smartstack, or ``hacheck`` if they are.

  * ``drain_method_params``: A dictionary of parameters for the specified drain_method. Valid parameters are any of the kwargs defined for the specified bounce_method in `bounce_lib <bounce_lib.html>`_.

  * ``constraints``: Specifies placement constraints for services. Should be defined as an array within an array (E.g ``[["habitat", "GROUP_BY"]]``). Defaults to ``[["<discover_location_type>, "GROUP_BY"]]`` where ``<discover_location_type>`` is defined by the ``discover`` attribute in ``smartstack.yaml``. For more details and other constraint types, see the official `Marathon constraint documentation <https://mesosphere.github.io/marathon/docs/constraints.html>`_.

  * ``cmd``: The command that is executed. Can be used as an alternative to args for containers without an `entrypoint <https://docs.docker.com/reference/builder/#entrypoint>`_. This value is wrapped by Mesos via ``/bin/sh -c ${app.cmd}``. Parsing the Marathon config file will fail if both args and cmd are specified [#note]_.

  * ``args``: An array of docker args if you use the `"entrypoint" <https://docs.docker.com/reference/builder/#entrypoint>`_ functionality. Parsing the Marathon config file will fail if both args and cmd are specified [#note]_.

  * ``env``: A dictionary of environment variables that will be made available to the container.

    * **WARNING**: A PORT variable is provided to the docker image, but it represents the EXTERNAL port, not the internal one. The internal service MUST listen on 8888, so this PORT variable confuses some service stacks that are listening for this variable. Such services MUST overwrite this environment variable to function. (``PORT=8888 ./uwisgi.py```) We tried to work around this, see `PAASTA-267 <https://jira.yelpcorp.com/browse/PAASTA-267>`_.

  *  ``monitoring``: A dictionary of values that configure overrides for monitoring parameters that will take precedence over what is in `monitoring.yaml`_. These are things like ``team``, ``page``, etc.

In addition, each instancename MAY configure additional Marathon healthcheck options:

  *  ``healthcheck_mode``: One of ``cmd``, ``tcp``, or ``http``. If your service uses Smartstack, then this must match the value of the ``mode`` key defined for this instance in ``smartstack.yaml``. If set to ``cmd`` then PaaSTA will execute ``healthcheck_cmd`` and examine the return code.

  *  ``healthcheck_cmd``: If ``healthcheck_mode`` is set to ``cmd``, then this command is executed inside the container as a healthcheck. It must exit with status code 0 to signify a successful healthcheck. Any other exit code is treated as a failure. Defaults to ``/bin/true`` (that is, always indicate good health) if ``healthcheck_mode`` is ``cmd``.

  *  ``healthcheck_grace_period_seconds``: Marathon will wait this long for a service to come up before counting failed healthchecks. Defaults to 60 seconds.

  *  ``healthcheck_interval_seconds``: Marathon will wait this long between healthchecks. Defaults to 10 seconds.

  *  ``healthcheck_timeout_seconds``: Marathon will wait this long for a healthcheck to return before considering it a failure. Defaults to 10 seconds.

  *  ``healthcheck_max_consecutive_failures``: Marathon will kill the current task if this many healthchecks fail consecutively. Defaults to 6 attempts.


Many of these keys are passed directly to Marathon. Their docs aren't super clear about all these but start there: https://mesosphere.github.io/marathon/docs/rest-api.html

Notes:

.. [#note] The Marathon docs and the Docker docs are inconsistent in their explanation of args/cmd:

    The `Marathon docs <https://mesosphere.github.io/marathon/docs/rest-api.html#post-/v2/apps>`_ state that it is invalid to supply both cmd and args in the same app.

    The `Docker docs <https://docs.docker.com/reference/builder/#entrypoint>`_ do not state that it's incorrect to specify both args and cmd. Furthermore, they state that "Command line arguments to docker run <image> will be appended after all elements in an exec form ENTRYPOINT, and will override all elements specified using CMD" which implies that both cmd and args can be provided, but cmd will be silently ignored.

    To avoid issues resulting from this discrepancy, we abide by the stricter requirements from Marathon and check that no more than one of cmd and args is specified. If both are specified, an exception is thrown with an explanation of the problem, and the program terminates.

chronos-[clustername].yaml
--------------------------

The yaml where Chronos jobs are defined. Top-level keys are the job names.

Most of the descriptions below are taken directly from the Chronos API docs, which can be found here: https://mesos.github.io/chronos/docs/api.html#job-configuration

Each job configuration MUST specify the following options:

  * ``schedule``: When the job should run. The value must be specified in the cryptic ISO 8601 format. See: https://en.wikipedia.org/wiki/ISO_8601 and https://mesos.github.io/chronos/docs/api.html#adding-a-scheduled-job

Each job configuration MAY specify the following options:

  * ``cmd``: See the `marathon-[clustername].yaml`_ section for details
    
  * ``args``: See the `marathon-[clustername].yaml`_ section for details

  * ``epsilon``: If Chronos misses the scheduled run time for any reason, it will still run the job if the time is within this interval. The value must be formatted like an ISO 8601 Duration. See: https://en.wikipedia.org/wiki/ISO_8601#Durations. Defaults to 'PT60S'.

  * ``retries``: Number of retries to attempt if a command returns a non-zero exit status. Defaults to 2.

  * ``disabled``: If set to ``True``, this job will not be run. Defaults to ``False``

  * ``cpus``: See the `marathon-[clustername].yaml`_ section for details

  * ``mem``: See the `marathon-[clustername].yaml`_ section for details

  *  ``monitoring``: See the `marathon-[clustername].yaml`_ section for details

smartstack.yaml
---------------

The yaml where nerve namespaces are defined and bound to ports.

See `CEP 319 <http://y/cep319>`_

monitoring.yaml
---------------

The yaml where monitoring for the service is defined.

Defaults for a *team* can be set globally with the global Sensu configuration
(distributed via Puppet). ``team`` is the only mandatory key, but overrides can
be set for the entire service with ``monitoring.yaml``.

Additionally these settings can be overridden on a *per-instance* basis. For
example a ``canary`` instance can be set with ``page: false`` and ``team:
devs``, while the ``main`` instance can bet set to ``page: true`` and ``team:
ops``, and the ``dailyadsjob`` instance can be set with ``ticket: true`` and ``team: ads``.

Here is a list of options that PaaSTA will pass through:

 * ``team``: Team that will be notified by Sensu

 * ``page``: Boolean to indicate if an instance should alert PagerDuty if it is failing.

 * ``runbook``: An optional but *highly* recommended field. Try to use shortlinks (y/rb-my-service) when possible as sometimes the runbook url will need to be copied from a small screen.

 * ``tip``: An optional one-line version of the runbook to help with common issues. For example: "Check to see if it is bing first!"

 * ``notification_email``: String representing an email address to send the notifications to. This will default to the team email address if is is already set globally. For multiple emails, use a comma separated list.

 * ``irc_channels``: Array of irc_channels to post notifications to.

 * ``ticket``: Boolean to indicate if an alert should make a JIRA ticket.

 * ``project``: String naming the project where JIRA tickets will be created. Overrides the global default for the team.


Where does paasta_tools look for yelpsoa-configs?
-------------------------------------------------------------

By default, paasta_tools uses the system yelpsoa-configs dir,
``/nail/etc/services``. Scripts should allow this to be overridden with ``-d``
or ``--soa-dir``. Normally you would only do this for testing or debugging.
