===================
The PaaSTA Contract
===================

The PaaSTA Contract is similar to the `12 Factor App <http://12factor.net/>`_ documented for Heroku. It specifies what kinds of apps are suitable for PaaSTA and what that app must do to run properly

Basic requirements
==================

Codebase
--------

PaaSTA assumes that the source for a service is stored in a single Git
repository, which can produce a single Docker image. The image MUST run that
service, though the same image MAY support multiple use cases (worker daemon,
web task, etc).

Dependencies
------------

The Docker image MUST contain all the code necessary to run the service.

State
-----

PaaSTA services SHOULD be stateless. Services MAY do filesystem IO, but all
disks are ephemeral with Docker (with the possible exception of RW
bind-mounts, which are discouraged).

Logs
----

Services should log to external log processors, at Yelp this is Scribe.

Bouncing
--------

PaaSTA reserves the right to cause a bounce on your service at any time. Please
make sure your service can handle this.  See the docs on `bouncing <../workflow.html#bouncing>`_
for bounce settings that can help make bounces less impactful to your service.


HTTP/TCP services
-----------------

* MUST be discoverable by SmartStack
* MUST bind to port 8888 if using the ``'bridge'`` `networking mode <../yelpsoa_configs.html#net>`_ or ONLY to the ports $MARATHON_PORT if using the ``'host'`` networking mode

Long-running tasks (services that don’t listen on a port, or “batch daemons”)
-----------------------------------------------------------------------------

MUST EITHER:

* Not have a SmartStack configuration file (smartstack.yaml)...
* If they have a SmartStack configuration file (e.g. because a single service codebase provides both an HTTP service and a long-running task) the instance configuration for the long-running task MUST NOT define a proxy_port::

    # marathon.yaml
    ---
    main:
      instances: 3
    worker:
      instances: 1
    # smartstack.yaml
    main:
      proxy_port: 12345
    # (no worker definition in smartstack.yaml!)

* MAY set ``healthcheck_mode`` to ``cmd`` and  specify a ``healthcheck_cmd`` in `marathon-<cluster>.yaml <../yelpsoa_configs.html#marathon-clustername-yaml>`_ to give Mesos better insight into the health of a task::

    # marathon.yaml
    ---
    gearman_worker:
      healthcheck_mode = "cmd"
      healthcheck_cmd = "/some_status_command.py"

Deployment Workflow
===================

Jenkins pipeline (Recommended)
------------------------------

Most services will want to use Jenkins to drive the deploy process. You can think of our Jenkins tooling as a reference implementation of the manual steps (below). Services that want this:

* MUST contain a Makefile. This is how Jenkins will manipulate the service.
* The Makefile SHOULD delegate non-trivial work to helper scripts written in an appropriate language
   * Helper scripts MAY live in paasta/ and MAY be named after the Makefile targets that call them, e.g. paasta/test, paasta/itest,  etc.
* MUST respond to make test
* SHOULD respond to ``make test`` by running unit tests
* MUST respond to ``make itest`` by generating a Docker image which will run with the currently checked-out code
* SHOULD respond by running some kind of system-level test (integration, e2e, contract, smoke) which exercises the generated Docker image
* MUST accept a parameter $(DOCKER_TAG) to make itest. Jenkins will calculate a tag for the newly-built Docker image and pass it to the service’s build scripts via this parameter::

    $(DOCKER_TAG) SHOULD default to something sensible when the job is run outside of Jenkins (e.g. during local development).
    E.g.:
    DOCKER_TAG ?= <servicename>-dev-$(USER)
    ...
    build:
    docker build -t $(DOCKER_TAG) .

* SHOULD respond to make cook-image by generating a Docker image tagged with $(DOCKER_TAG) for use by local-run
* cook-image MAY be added as a dependency of the itest target so that both Jenkins and local-run use the same image creation process::

    itest: cook-image
        ...
    cook-image:
        docker build -t "$$DOCKER_TAG" .

* yelpsoa-configs/<your service> MUST contain a deploy.yaml and SHOULD use jenkins.yaml to create Jenkins pipelines based on that deploy.yaml.

Manual Steps (Not Recommended)
------------------------------

This section describes how to operate the PaaSTA deploy system. If for some
reason you don’t want to use Jenkins, you can pull the levers yourself. The
PaaSTA CLI contains subcommands to help with this (these same subcommands are
used by Jenkins)

* Docker images MUST be tagged
   * You’ll tag your image at build time with something like docker build -t "$DOCKER_TAG" .
   * Use ``paasta itest`` to do this
* Docker images MUST be uploaded to the docker-paasta registry
   * Use ``paasta push-to-registry``
* To deploy a service to a particular Marathon instance, the service’s git repo MUST have a specially-named branch
   * Use ``paasta mark-for-deployment`` for the format of the git ref
   * The SHA at the tip of this branch is used to locate a Docker image in the registry
   * This Docker image is what will be deployed to PaaSTA
