==============================================
Resource Isolation in PaaSTA, Mesos and Docker
==============================================

PaaSTA instance definitions include fields that specify the required resources
for your service. The reason for this is two-fold: firstly, so that whichever
Mesos framework (Marathon, Chronos) can evaluate which Mesos agent making
offers have enough capacity to run the task (and pick one of the agents
accordingly); secondly, so that tasks can be protected from especially noisy
neighbours on a box. That is, if a task under-specifies the resources it
requires to run, or in another case, has a bug that means that it consumes far
more resources than it *should* require, then the offending tasks can be
isolated effectively, preventing them from having a negative impact on it's
neighbours.

This document is designed to give a more detailed review of how Mesos
Frameworks such as Marathon and Chronos use these requirements to run tasks on
different Mesos agents, and how these isolation mechanisms are implemented.

Note: Knowing the details of these systems isn't a requirement of using PaaSTA;
most service authors may never need to know the details of such things. In
fact, one of PaaSTA's primary design goals is to *hide* the minutiae of
schedulers and resource isolation. However, this may benefit administrators
of PaaSTA (and, more generally, Mesos clusters), and the simply curious.

Final note: The details herein may, nay, will contain (unintended) inaccuracies.
If you notice such a thing, we'd be super grateful if you could open a pull
request to correct the document!

How Tasks are Scheduled on Hosts
--------------------------------

To first understand how these resources are used, one must understand how
a task is run on a Mesos cluster.

Mesos can run in two modes: Master and Agent. When a node is running Mesos in
Master mode, it is responsible for communicating between agent processes and
frameworks. A Framework is a program which wants to run tasks on the Mesos
cluster.

A master is responsible for presenting frameworks with resource offers.
Resource offers are compute resource free for a framework to run a task. The
details of that compute resource comes from the agent nodes, which regularly
tell the Master agent the resources it has available for running tasks. Using
the correct parlance, Mesos agents make 'offers' to the master.

At Yelp, the resource that a given agent has available is determined by a
couple of things: if the node also runs 'classic' services, then the resources
given to the agent are decided by a script which takes a rough guess at how
much resource is used *without* Mesos running, and subtracts it from the total
available resource. That gives a *very* approximate guess at the resources
available for the Mesos Agent to run tasks.

If, however, the agent node is a dedicated PaaSTA box (defined by having the
puppet role ``paasta``), then all the resources available on the box are given
to the Agent to run tasks. This means that the cpus offered is set to the
total number of processors on the host, and the memory offered is equal to the
total memory available on the host.

Once a master node receives offers from an agent, it forwards it to
a framework. Resource offers are split between frameworks according to
the master's configuration - there may be particular priority given
to some frameworks.

At Yelp, we treat the frameworks we run (at the time of writing, Marathon and
Chronos) equally. That means that frameworks *should* have offers distributed
between them evenly, and all tasks are considered equal.

It is then up to the framework to decide what it wants to do with an offer.
The framework may decide to:

  * Reject the offer, if the framework has no tasks to run.
  * Reject the offer, if the resources included in the offer are not enough to
    match those required by the application.
  * Reject the offer, if attributes on the slave conflict with any constraints
    set by the task.
  * Accept the offer, if there is a task that requires resources less than or
    equal to the resources offered by the Agent.

When rejecting an offer, the framework may apply a 'filter' to the offer. This
filter is then used by the Mesos master to ensure that it does *not* resend
offers that are 'filtered' by a framework. The default filter applied includes
a timeout - a Master will not resend an offer to a framework for a period of 5
seconds.

If a framework decides it wants to accept a resource offer, it then tells the
master to run a task on the agent. The details of the 'acceptance' include a
detail of the task to be run, and the 'executor' used to run the task.

By default, Yelp uses the 'Docker' executor everywhere. This means that *all*
tasks launched by Marathon and Chronos are done so with a Docker container.

How Tasks are isolated from eachother.
-------------------------------------

Given that a slave may run multiple tasks, we need to ensure that tasks cannot
'interfere' with one another. We do this on a file system level using Docker -
without some hard work and security flaws, processes are protected from
each other and the host by using kernel namespaces. Note that the use of kernel
namespaces is a feature of Docker - PaaSTA doesn't do anything 'extra' to
enable this. It's also worth noting that there are other 'container'
technologies that could provide this - the native Mesos 'containerizer'
included.

However, these tasks are still running and consuming resources on the same
host. The next section aims to explain how PaaSTA services are protected from
so-called 'noisy neighbours' that can starve others from resources.

CGroups
^^^^^^
Docker uses cgroups to enforce resource isolation. Cgroups are a part of the
linux kernel, and can be used to restrict the resources available to groups of
processes. In our setup, each Docker container that is launched (and any child
processes forked inside the container) are contained in a given cgroup.

Memory
""""""

When a container is launched, it is done so with the 'mem' option given to the
docker daemon with the value equal to that set in a services definition.
This tells the kernel to limit the memory available to processes in the cgroup
to that shown in the config.

However, one caveat with only setting *this* value is that swap is not
accounted for. As a result, once a container reaches it's memory limit, it may
start swapping, rather than being killed. Without particular kernel cmdline
options, swapping is *not* accounted for the cgroup.

Once we instruct the kernel to start accounting for swap, then there is also a
configuration value ``memsw.limit_in_bytes`` associated with the cgroup. This defines a maximum
value for the sum of memory and swap usage processes in the cgroup can use.

At Yelp, we used the '--memory-swap' parameter to tell Docker to set this value
to the *same value as the memory parameter*. This prevents a container from swapping at all.

You can see these values by looking at:

* `cat /sys/fs/cgroup/docker/<container-id>/memory.limit_in_bytes`
* `cat /sys/fs/cgroup/docker/<container-id>/memory.memsw.limit_in_bytes`

In Yelp's setup, these values should be the same.

If the processes in the cgroup reaches the ``memsw.limit_in_bytes`` value ,
then the kernel will invoke the OOM killer, which in turn will kill off one of
the processes in the cgroup (often, but not always, this is the biggest
contributor to the memory usage). If this is the only process running in the
Docker container, then the container will die. The mesos framework which
launched the task may or may not decide to try and start the same task
elsewhere.

CPUs
""""

CPU enforcement is implemented slightly differently. Many people expect this to
map to a number of cores that are reserved for a task. However, isolating CPU
time like this can be particularly wasteful; unless a task spends 100% of it's
time on CPU (and thus has *no* I/O), then there is no need to prevent other
tasks from running on the spare CPU time available.

Instead, the CPU value is used as a weighting to help the Linux Scheduler
decide the order in which to run waiting threads. If there is no contention
between processes, that is, there is only one thread in the run queue for a CPU
core, then the CPU will run any tasks waiting, irrespective of their weighting
or utilization.
However, in the case where there is contention for CPU resource, then the
weighting of the task to be run has an impact on how the scheduler decides
which task should run next.

As a result, if your service is seeing bad performance, then bumping the value
of the ``cpus`` field won't automatically improve things. Particularly, there
are only a few tasks running on your host, and the length of the run queue is
small, then it is doubtful that it will have much impact at all.

It is also important to note that when deciding the ordering in which tasks
should be scheduled, threads are grouped by the cgroup that they are in. That
is, the scheduler takes both the weight and the utilization of  *all* threads in
a cgroup into account, rather than individual threads. As a result, it may be
prudent to scale horizontally, rather than vertically to improve performance.

Disk
"""""

Unfortunately, the isolator provided by Mesos does not support isolating disk
space used by Docker containers; that is, we have no way of limiting the amount
of disk space used by a task. Our best effort is to ensure that the disk space
is part of the offer given by a given Mesos agent to frameworks, and ensure
that any services we know to use high disk usage (such as search indexes) have
the ``disk`` field set appropriately in their configuration.
