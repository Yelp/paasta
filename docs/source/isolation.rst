==============================================
Resource Isolation in PaaSTA, Mesos and Docker
==============================================

PaaSTA instance definitions include fields that specify the required resources
for your service. The reason for this is two-fold: firstly, so that whichever
Mesos framework can evaluate which Mesos agent making
offers have enough capacity to run the task (and pick one of the agents
accordingly); secondly, so that tasks can be protected from especially noisy
neighbours on a box. That is, if a task under-specifies the resources it
requires to run, or in another case, has a bug that means that it consumes far
more resources than it *should* require, then the offending tasks can be
isolated effectively, preventing them from having a negative impact on its
neighbours.

This document is designed to give a more detailed review of how Mesos
Frameworks such as Marathon use these requirements to run tasks on
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

Once a master node receives offers from an agent, it forwards it to
a framework. Resource offers are split between frameworks according to
the master's configuration - there may be particular priority given
to some frameworks.

At Yelp, we treat the frameworks we run (at the time of writing, Marathon and
Tron) equally. That means that frameworks *should* have offers distributed
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

By default, PaaSTA uses the 'Docker' executor everywhere. This means that *all*
tasks launched by Marathon and Tron are done so with a Docker container.

How Tasks are isolated from each other.
---------------------------------------

Given that a slave may run multiple tasks, we need to ensure that tasks cannot
'interfere' with one another. We do this on a file system level using Docker -
processes launched in Docker containers are protected from each other and the
host by using kernel namespaces. Note that the use of kernel namespaces is a
feature of Docker - PaaSTA doesn't do anything 'extra' to enable this. It's
also worth noting that there are other 'container' technologies that could
provide this - the native Mesos 'containerizer' included.

However, these tasks are still running and consuming resources on the same
host. The next section aims to explain how PaaSTA services are protected from
so-called 'noisy neighbours' that can starve others from resources.

CGroups
^^^^^^^
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
accounted for. As a result, once a container reaches its memory limit, it may
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

CPU enforcement is implemented slightly differently. Many people expect the
value defined in the ``cpus`` field in a service's soa-configs to map to a
number of cores that are reserved for a task. However, isolating CPU time like
this can be particularly wasteful; unless a task spends 100% of its time on
CPU (and thus has *no* I/O), then there is no need to prevent other tasks from
running on the spare CPU time available.

Instead, the CPU value is used to give tasks a relative priority. This priority
is used by the Linux Scheduler decide the order in which to run waiting
threads.

Some notes on this:

  - As mentioned, these values are relative. We enforce no scale on these
    numbers. So if two tasks are competing for resources, one has set ``cpus``
    to 0.5 and the other to 4.5, then it the first task will receive 10% of time
    and the second 90%. If, however, the second task was replaced with another
    with a requirement of 0.5, leaving two tasks with each ``cpu`` set to 0.5,
    then each task will have 50% of time.
  - The amount of time tasks get is proportional to the number of tasks on the
    host. If there are 3 tasks on the host, with ``cpu`` values 10,5,5 then the
    time will be split 50%, 25%, 25%. However, if a fourth task is run, with
    ``cpu`` set to 10, then that time becomes 33%, 16.5%, 16.5%, 33%.
  - Any 'spare' cpu cycles are redistributed by the CPU, so if a task does
    *not* use its 'share', then other cgroups will be allocated this spare CPU
    time.
  - All threads inside a cgroup are considered when the scheduler decides the
    fair share of time. That is, if your container launches multiple tasks,
    then the share is split across all these tasks. If the tasks were to run in
    their own cgroup, then the time spent on CPU by one task would not count
    against the share available for another. The result of this may be that
    a higher number of 'skinny' containers may be preferable to 'fat' containers.

This is different from how Mesos and Marathon use the CPU value when evaluating
whether a task 'fits' on a host. Yelp configures agents to advertise the number
of cores on the box, and Marathon will only schedule containers on agents where
there is enough 'room' on the host, when in reality, there is no such limit.

Disk
"""""

Unfortunately, the isolator provided by Mesos does not support isolating disk
space used by Docker containers; that is, we have no way of limiting the amount
of disk space used by a task. Our best effort is to ensure that the disk space
is part of the offer given by a given Mesos agent to frameworks, and ensure
that any services we know to use high disk usage (such as search indexes) have
the ``disk`` field set appropriately in their configuration.
