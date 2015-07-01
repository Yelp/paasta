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

  * ``cpus``: Number of CPUs an instance needs

  * ``mem``: Memory (in MB) an instance needs

  * ``instances``: Marathon will attempt to run this many instances of the Service

  * ``nerve_ns``: Specifies that this namespace should be routed to by another namespace. E.g. ``canary`` instances have a different configuration but traffic from the ``main`` pool reaches them.

  * ``bounce_method``: Controls the bounce method; see `bounce_lib <bounce_lib.html>`_

  * ``constraints``: Specifies placement constraints for services. Should be defined as an array within an array (E.g ``[["habitat", "GROUP_BY"]]``). Defaults to ``[["<discover_location_type>, "GROUP_BY"]]`` where ``<discover_location_type>`` is defined by the ``discover`` attribute in ``smartstack.yaml``. For more details and other constraint types, see the official `Marathon constraint documentation <https://mesosphere.github.io/marathon/docs/constraints.html>`_.

  * ``cmd``: The command that is executed. Can be used as an alternative to args for containers without an `entrypoint <https://docs.docker.com/reference/builder/#entrypoint>`_. This value is wrapped by Mesos via ``/bin/sh -c ${app.cmd}``. Parsing the Marathon config file will fail if both args and cmd are specified [#note]_.

  * ``args``: An array of docker args if you use the `"entrypoint" <https://docs.docker.com/reference/builder/#entrypoint>`_ functionality. Parsing the Marathon config file will fail if both args and cmd are specified [#note]_.

  * ``env``: A dictionary of environment variables that will be made available to the container.

    * **WARNING**: A PORT variable is provided to the docker image, but it represents the EXTERNAL port, not the internal one. The internal service MUST listen on 8888, so this PORT variable confuses some service stacks that are listening for this variable. Such services MUST overwrite this environment variable to function. (``PORT=8888 ./uwisgi.py```) We tried to work around this, see `PAASTA-267 <https://jira.yelpcorp.com/browse/PAASTA-267>`_.

Many of these keys are passed directly to Marathon. Their docs aren't super clear about all these but start there: https://mesosphere.github.io/marathon/docs/rest-api.html

.. [#note] The Marathon docs and the Docker docs are inconsistent in their explanation of args/cmd:
    
    The `Marathon docs <https://mesosphere.github.io/marathon/docs/rest-api.html#post-/v2/apps>`_ state that it is invalid to supply both cmd and args in the same app.
    
    The `Docker docs <https://docs.docker.com/reference/builder/#entrypoint>`_ do not state that it's incorrect to specify both args and cmd. Furthermore, they state that "Command line arguments to docker run <image> will be appended after all elements in an exec form ENTRYPOINT, and will override all elements specified using CMD" which implies that both cmd and args can be provided, but cmd will be silently ignored.
    
    To avoid issues resulting from this discrepancy, we abide by the stricter requirements from Marathon and check that no more than one of cmd and args is specified. If both are specified, an exception is thrown with an explanation of the problem, and the program terminates.


smartstack.yaml
---------------

The yaml where nerve namespaces are defined and bound to ports.

See `CEP 319 <http://y/cep319>`_

monitoring.yaml
---------------

The yaml where monitoring for the service is defined.

See `the wiki
<https://trac.yelpcorp.com/wiki/HowToService/Monitoring/monitoring.yaml>`_


Where does paasta_tools look for yelpsoa-configs?
-------------------------------------------------------------

By default, paasta_tools uses the system yelpsoa-configs dir,
``/nail/etc/services``. Scripts should allow this to be overridden with ``-d``
or ``--soa-dir``. Normally you would only do this for testing or debugging.
