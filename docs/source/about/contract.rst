===================
The PaaSTA Contract
===================

The PaaSTA Contract is similar to the `12 Factor App <http://12factor.net/>`_ documented for Heroku.
It specifies what kinds of apps are suitable for PaaSTA and what that app must do to run properly.

Basic requirements
==================

Codebase
--------

PaaSTA assumes that the source for a service is stored in a single Git repository, which can produce a single Docker image.
The image MUST run that service, though the same image MAY support multiple use cases (worker daemon, web task, etc).

Dependencies
------------

The Docker image MUST contain all the code necessary to run the service.

State
-----

PaaSTA services SHOULD be stateless -- that is, no irreplaceable information should be stored by your service.
Generally, services should store their state in external datastores like MySQL, Cassandra, or S3.
Services MAY do filesystem IO, but everything written to disk will be discarded whenever the container dies.
(Containers are killed for many reasons: autoscaling, deploying a new version of your code, most changes to yelpsoa-configs, etc.)

If you want to run a system that stores important state on disk, we recommend running a `Kubernetes Operator <https://yelpwiki.yelpcorp.com/display/COMPUTE/Operators+at+Yelp>`_.

Logs
----

PaaSTA will log the stdout/stderr of your service, and this is accessible via the ``paasta logs`` command.
Services that want structured logs should use an external log system: at Yelp we use Monk (usually accessed via the ``clog`` library).

Bouncing
--------

PaaSTA reserves the right to cause a bounce (our term for when all of your service's containers are replaced) on your service at any time.
Reasons for bouncing your service include:

- To deploy a new version of your service
- To adjust the configuration of your service -- most changes to yelpsoa-configs will require a bounce.
  Some changes to the Paasta codebase may also cause your service to be restarted, e.g. if the default values for configuration change.
- ``paasta restart``.

Please make sure your service can handle this.
Generally, stateless HTTP services should have no trouble being bounced -- Paasta will work with the service mesh to stop traffic going to a container before killing it.
See the docs on `bouncing <../workflow.html#bouncing>`_ for bounce settings that can help make bounces less impactful to your service.


Configuration
-------------

Each service must have one or more "instances" defined in yelpsoa-configs.
Each instance is an independent configuration of your service, specifying things like environment variables, the command to run within your container, and autoscaling parameters.

See the `yelpsoa-configs <../yelpsoa-configs.html>`_ documentation.


HTTP/TCP services
-----------------

* MUST be discoverable by the service mesh:

  * This requires an entry in smartstack.yaml for the ``registration`` used by that instance (which defaults to the instance name, i.e. ``main``).

* MUST bind to the port specified by the ``$PAASTA_PORT`` environment variable.
  (This is almost always 8888, but it is wise to use the variable instead of hard-coding.).


Long-running tasks that don't listen on a port
----------------------------------------------

Some services (or some instances of some services) want to run indefinitely like a HTTP/TCP service, but don't need accept incoming traffic.
Examples include queue workers, chat bots, and other programs that make outbound connections to request work.

To do this:

* Don't create an entry in smartstack.yaml for this instance.
* Don't set ``registrations`` in ``kubernetes-{cluster}.yaml``::

    # kubernetes-norcal-devc.yaml
    ---
    main:
      instances: 3
    worker:
      instances: 1
    # smartstack.yaml
    main:
      proxy_port: 12345
    # (no `worker` definition in smartstack.yaml!)

* MAY set ``healthcheck_mode`` to ``cmd`` and  specify a ``healthcheck_cmd`` in `kubernetes-<cluster>.yaml <../yelpsoa_configs.html#kubernetes-clustername-yaml>`_ to give Kubernetes better insight into the health of a task::

    # kubernetes-clustername.yaml
    ---
    queue_worker:
      healthcheck_mode = "cmd"
      healthcheck_cmd = "/some_status_command.py"

Without specifying a healthcheck command, each replica is considered healthy until it exits (crashes or otherwise stops running).


Deployment Workflow
===================

Jenkins pipeline (Recommended)
------------------------------

Most services will want to use Jenkins to drive the deploy process.
You can think of our Jenkins tooling as a reference implementation of the manual steps (below).
Services that want this:

* MUST contain a Makefile.
  This is how Jenkins will manipulate the service.

* The Makefile MUST respond to ``make test``.

  * This makefile target SHOULD run unit tests.

* The Makefile MUST respond to ``make itest``, which:

   * MUST generate a Docker image which will run with the currently checked-out code.
   * SHOULD respond by running some kind of system-level test (integration, e2e, contract, smoke) which exercises the generated Docker image
   * MUST tag the generated docker image with the tag specified in the ``DOCKER_TAG`` environment variable.
     Jenkins will calculate a tag for the newly-built Docker image and pass it to the service’s build scripts via this parameter::

       # $(DOCKER_TAG) SHOULD default to something sensible when the job is run outside of Jenkins (e.g. during local development).
       # E.g.:
       DOCKER_TAG ?= <servicename>-dev-$(USER)
       ...
       build:
       docker build -t $(DOCKER_TAG) .

* The Makefile SHOULD respond to ``make cook-image`` by generating a Docker image tagged with ``$(DOCKER_TAG)`` for use by local-run.

   * cook-image MAY be added as a dependency of the itest target so that both Jenkins and local-run use the same image creation process::

       itest: cook-image
           ...
       cook-image:
           docker build -t "$$DOCKER_TAG" .

* ``yelpsoa-configs/<your service>`` MUST contain a ``deploy.yaml``.
  See `y/deploy-yaml <https://y.yelpcorp.com/deploy-yaml>`_ for more information about deploy.yaml.

* your service's git repo SHOULD contain a ``jenkins.yaml`` to create Jenkins pipelines based on that deploy.yaml.
  See `y/jenkinsfile <https://y.yelpcorp.com/jenkinsfile>`_ for more info.

Manual Steps (Not Recommended)
------------------------------

This section describes how to operate the PaaSTA deploy system.
If for some reason you don’t want to use Jenkins, you can pull the levers yourself.
The PaaSTA CLI contains subcommands to help with this (these same subcommands are used by Jenkins)

* Docker images MUST be tagged properly.

   * You’ll tag your image at build time with something like ``docker build -t "$DOCKER_TAG"``.
   * Use ``paasta itest`` to do this.
     This calls ``make itest`` (which MUST meet the specification defined above), with the ``DOCKER_TAG`` environment variable set appropriately.

* Docker images MUST be uploaded to the docker-paasta registry

   * Use ``paasta push-to-registry`` to do this.

* To deploy a version of a service to a particular `deploy group <../deploy_groups.html>`_, the service’s git repo MUST have a specially-named tag

   * Use ``paasta mark-for-deployment`` to create this git tag.
   * The SHA pointed to by this tag is used to locate a Docker image in the registry
   * This Docker image is what will PaaSTA will deploy.
   * The format of the tag (and the usage of git tags at all) should be considered an implementation detail, and is subject to change.
     ``paasta mark-for-deployment`` should be considered the stable interface.
