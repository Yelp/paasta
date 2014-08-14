How to configure a service using Marathon
=========================================

This page will outline all of the necessary components to deploy
a service using Marathon to a mesos cluster via docker images.

As an outline, to deploy a service to mesos in a docker image
via Marathon, you'll need three things:

* A docker image in a services-{service_name} repository with a tag set to jenkins-{SHA},
  where SHA is the SHA at the current head of a branch (if you want to use 'branch' keys
  in your config files, for more automated deployment), OR whatever tag you like
  (if you want to use 'docker_image' keys in your config files, for more static
  deployment).
* A configuration directory for all your services, with one subdirectory containing
  configuration files for the service you want to deploy
* A mesos cluster with Marathon running on it

The git repository dockerizing the service, the service config dirname, and
the docker image name in the docker repository should ALL BE THE SAME. If they're
not, the service_deployment_tools glue won't work!

------------------------
Dockerizing your service
------------------------

To get your service running on Marathon, it needs to be in a docker image.
To find out more about docker, hit the docs at https://docs.docker.com/ and
try out the tutorial if you haven't. You'll want to get a docker that exposes
whatever port your service needs and start your service in the docker
such that it never exits (i.e. your service can't run as a daemon in the docker).

There's two ways that this library will attempt to load whatever docker you create,
depending on the name of the key used in the Marathon job configuration for the
service:

1. A 'docker_image' key, which allows you to set a static docker image name and tag
2. A 'branch' key, which will make generate_deployments_json create a map between
   a static {service_name}:{branch} key to a generated services-{service_name}:jenkins-{SHA}
   key, where SHA is the SHA at the tip of the branch name given in the
   service's git repository

The first way is pretty simple- push a docker with some name and tag, and then
put that name in the configuration file for the Marathon job(s) for that service.

The second way is a bit more complicated, but allows for much more automation
of service updates. Here at Yelp, we have a Jenkins promote_to_registry job
that'll build and push the docker to our docker registry, tagged as described above.
Then, the generate_deployments_json script maps to the same tag,
so any time that a branch gets updates, setup_marathon_job knows that
the Marathon job configuration has changed (SHA change = different docker image tag),
and can redeploy the job onto the mesos cluster.

-------------------------------------------
The soa_dir and service configuration YAMLs
-------------------------------------------

You'll see tons of references to a soa_dir variable all around the code in this
module, and text talking about a 'SOA Configuration directory'. But what is it?

To configure your service to be deployed with marathon, there needs to be a central
directory containing SOA (Service Oriented Architecture) for each service that needs
to be deployed. By default, this is set to /nail/etc/services, but can be set
in every script that uses it by using the -d or --soa-dir option. In this directory,
there should be any number of subdirectories corresponding to the services that
you wish to deploy. The names of these directories should be the name of the service.
So, if you want to deploy a Sensu server in the sensu_server service, you'd make a directory
called /nail/etc/services/sensu_server, which will contain the configuration YAMLs
required to deploy the service to Marathon.

#####################################
YAML configuration files for services
#####################################

Three YAML files are required to get a dockerized service deployed through Marathon:

* marathon-{cluster}.yaml: The yaml where marathon jobs are actually defined.

  * Cluster should be the name of the cluster that the jobs are actually for, i.e. devc, prod, etc, allowing for different, tuned jobs per cluster for each service.

  * The yaml has the following format:

      | ---
      | instance_one:
      |   branch: <string>
      |   bounce_method: <string>
      |   cpus: <integer>
      |   docker_image: <string>
      |   instances: <integer>
      |   mem: <integer>
      |   num_ports: <integer>
      |   constraints: <array of strings>
      |   nerve_ns: <string>
      | instance_two:
      |   …

  * Each of the above keys means:

    * branch: The branch on the service’s git repository to use in order to determine the current docker image tag. The branch should have a Dockerfile and have that docker created by the Jenkins promote_to_docker task. Defaults to paasta-{cluster}.{instance}.

    * bounce_method: One of brutal or crossover. Brutal means that old service instances will be killed immediately before the new ones are started, crossover means that new service instances will be spun up before old ones are killed. Defaults to crossover.

    * cpus: The number of CPUs to use for each instance. Defaults to 1.

    * docker_image: The docker image (with tag) to use for this instance. Overrides the branch key.

    * instances: The number of instances of the service to actually run. Defaults to 1.

    * mem: The amount of memory to use for each instance, in megabytes. Defaults to 100.

    * num_ports: The number of ports needed for each instance. Should match the number of ports that the Dockerfile exposes for this service. Defaults to 1.

    * constraints: The constraints to put on a marathon job/service instance. See https://github.com/mesosphere/marathon/wiki/Constraints. Defaults to empty.

    * nerve_ns: The nerve namespace this service instance should bind to. This means that you can have multiple instances defined on the same namespace that do different things- like canaries.

* smartstack.yaml: The yaml where nerve namespaces are defined and bound to ports.

  * The yaml has the following format:

      | ---
      | namespace_one:
      |   healthcheck_timeout_s: <integer>
      |   healthcheck_uri: <string>
      |   proxy_port: <integer>
      |   retries: <integer>
      |   mode: <string>
      |   timeout_connect_ms: <integer>
      |   timeout_server_ms: <integer>
      |   timeout_client_ms: <integer>
      |   routes:
      |   - destinations:
      |     - <string>
      |       <string>
      |     source: <string>
      |   - destinations:
      |       ...

  * Each of the above keys means:

    * healthcheck_timeout_s: The number of seconds until a healthcheck should time out on the namespace.

    * healthcheck_uri: The uri to healthcheck against.

    * proxy_port: The frontend port to eventually bind in HAProxy for this namespace (this’ll be the port you actually hit your service with).

    * retries: The number of retries HAProxy should perform on a backend for healthchecking.

    * mode: One of 'tcp' or 'http', representing the type of connection the service expects

    * timeout_connect_ms: The timeout (in milliseconds) before a frontend connection will time out

    * timeout_server_ms: The timeout (in milliseconds) before a backend connection will time out

    * timeout_client_ms: The timeout (in milliseconds) before an active client connection will time out

    * routes: Allows you to list source datacenters that should proxy into a bunch of destination datacenters.

* monitoring.yaml: The yaml where monitoring keys for the service are defined.

  * This yaml should just be the normal key/value pairs defined for a sensu monitoring_check (whether in puppet, pysensu-yelp, or elsewhere).

  * This is used whenever a deployment or other related service_deployment_tools script fails to do whatever it needed to on your service.

