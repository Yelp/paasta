Example Cluster
---------------

This folder contains a docker-compose setup for a semi-complete PaaSTA
cluster. It is intended for showing how PaaSTA works and as a
development environment for running the PaaSTA tools. It is **not** a
good example of how to configure a PaaSTA cluster in production. At Yelp we
use puppet to do that.

The environment is generally the same as the itest docker-compose
environment. However, it also includes some config files and cron jobs
that make it act more like an actual PaaSTA cluster. It also volume
mounts your working directory into the container so that you can work on
the files locally but run them in the containers.

To get started run: ``docker-compose run playground``. This should give
you a shell with the paasta\_tools package installed in development
mode.

If you have added a new python dependency you may need to run
``docker-compose build`` to re-build the containers. Then you can restart
everything with ``docker-compose down && docker-compose run playground``.

Getting Started
---------------

Mesos
~~~~~
To launch a running Mesos cluster, then run ``docker-compose run playground``
and you'll be dropped into a shell with the paasta\_tools package installed in development mode.

Kubernetes
~~~~~~~~~~
To instead launch a Kubernetes cluster, run

::

    cd example_cluster
    ./dind-cluster-v1.13.sh clean
    ./dind-cluster-v1.13.sh up
    docker-compose -f docker-compose.k8s.yaml run playground


Try it out
----------

The cluster includes a git remote and docker registry. The git remote
contains an example repo but you can add more if you want.

The mesos and marathon webuis are exposed on your docker host
on port 5050, 8080, 8081. So load them up if you want to watch. Then in
the playground container:

::

    cd /tmp
    git clone root@git:dockercloud-hello-world
    cd dockercloud-hello-world
    paasta itest -s hello-world -c `git rev-parse HEAD`
    paasta push-to-registry -s hello-world -c `git rev-parse HEAD` --force
    paasta mark-for-deployment --git-url root@git:dockercloud-hello-world --commit `git rev-parse HEAD` --deploy-group testcluster.everything --service hello-world


Scaling The Cluster
-------------------
If you want to add more capacity to the cluster, you can increase the number of Mesos agents/Kubernetes Nodes:

``docker-compose scale mesosslave=4`` or
``docker-compose scale kubernetes=4``


Interacting with the cluster
----------------------------

Some but not all of the paasta command line tools should work. Try:

::

    paasta metastatus
    paasta status -s hello-world

Scribe is not included with this example cluster. If you are looking for
logs, check ``/var/logs/paasta_logs`` and syslog on the mesosmaster for
the output from cron. Also note that all the slaves share the host's
docker daemon.

Cleanup
-------

``docker-compose down``

However note that this won't remove any containers that PaaSTA has
spawned. So to tidy up you can do something like:
``docker stop $(docker ps |grep localhost|grep paasta|awk '{print $1}')``
