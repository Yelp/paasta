## Example Cluster

This folder contains a docker-compose setup for a semi-complete PaaSta cluster. It is intended for showing how PaaSta works and as a development environment for running the PaaSta tools. It is **not** a good example of how to setup a PaaSta cluster in production. At Yelp we use puppet to do that.

The environment is generally the same as the itest docker-compose environment. However, it also includes some config files and cron jobs that make it act more like an actual PaaSta cluster. It also volume mounts your working directory into the container so that you can work on the files locally but run them in the containers.

To get started run: `docker-compose run playground`. This should give you a shell with the paasta_tools package installed in development mode. The first time this runs it will take a while because we fetch and compile various python packages, however we store a cache on the docker host so that it is quicker next time. If you need to update a python package you probably want to `pip wheel /work --wheel-dir=/var/tmp/pip_cache` and then restart the container.

## Try it out
The cluster includes a git remote and docker registry. The git remote contains an example repo but you can add more if you want.

The mesos, marathon and chronos webuis are exposed on your docker host on port 5050, 8080, 8081. So load them up if you want to watch. Then in the playground container:

```
cd /tmp
git clone root@git:dockercloud-hello-world
cd dockercloud-hello-world
paasta itest -s hello-world -c `git rev-parse HEAD`
paasta push-to-registry -s hello-world -c `git rev-parse HEAD`
paasta mark-for-deployment --git-url root@git:dockercloud-hello-world --commit `git rev-parse HEAD` --clusterinstance testcluster.everything --service hello-world
```

This mimics what jenkins would do to deploy a paasta service. If you end up with some tasks that are stuck waiting it's probably because of capacity. So add some more slaves like: `docker-compose scale mesosslave=4`

Some but not all the command line tools should work. Try:
```
paasta metastatus
paasta status -s hello-world
```

We don't have scribe so check `/var/logs/paasta_logs` and syslog on the mesosmaster for the output from cron. Also note that all the slaves share the hosts docker daemon.

## Cleanup
`docker-compose down`

However note that this won't remove any containers that PaaSta has spawned. So to tidy up you can do something like: `docker stop $(docker ps |grep localhost|grep paasta|awk '{print $1}')`
