==============================================
Resource Isolation in PaaSTA, Kubernetes and Docker
==============================================

PaaSTA instance definitions include fields that specify the required resources
for your service. The reason for this is two-fold: firstly, so that the Kubernetes scheduler
can evaluate which Kubernetes nodes have enough capacity to schedule the Kubernetes Pods (representing PaaSTA instances) on, in the cluster specified;
secondly, so that the Pods can be protected from especially noisy
neighbours on a box. That is, if a Pod under-specifies the resources it
requires to run, or in another case, has a bug that means that it consumes far
more resources than it *should* require, then the offending Pods can be
isolated effectively, preventing them from having a negative impact on its
neighbours.

This document is designed to give a more detailed review of how Kubernetes
use these requirements to run Pods on different Kubernetes nodes, and how these isolation mechanisms are implemented.

Note: Knowing the details of these systems isn't a requirement of using PaaSTA;
most service authors may never need to know the details of such things. In
fact, one of PaaSTA's primary design goals is to *hide* the minutiae of
schedulers and resource isolation. However, this may benefit administrators
of PaaSTA (and, more generally, Kubernetes clusters), and the simply curious.

Final note: The details herein may, nay, will contain (unintended) inaccuracies.
If you notice such a thing, we'd be super grateful if you could open a pull
request to correct the document!

How Tasks are Scheduled on Hosts
--------------------------------

To first understand how these resources are used, one must understand how
a Pod is run on a Kubernetes cluster.

Kubernetes has two types of nodes: Master and worker nodes. The master nodes are
responsible for managing the cluster.

The master node contains the following components:

  * API Server: Exposes the Kubernetes API. It is the front-end for the Kubernetes control plane.
  * Scheduler: Responsible for distributing workloads across multiple nodes.
  * Controller Manager: Responsible for regulating the state of the cluster.

Worker nodes are the machines that run the workload. Each worker node runs the following components
to manage the execution and networking of containers:

  * Kubelet: An agent that runs on each node in the cluster. It makes sure that containers are running in a Pod.
  * Kube-proxy: Maintains network rules on nodes. These network rules allow network communication to Pods from network sessions inside or outside of the cluster.
  * Container runtime: The software that is responsible for running containers. Kubernetes supports several container runtimes: Docker, containerd, CRI-O, and any implementation of the Kubernetes CRI (Container Runtime Interface).


When a new Pod (representing a PaaSTA instance) is created, the Kubernetes scheduler (kube-scheduler) will assign it to the best node for it to run on.
The scheduler will take into account the resources required by the Pod, the resources available on the nodes, and any constraints that are specified. It takes the following
criteria into account when selecting a node to have the Pod run on:

  * Resource requirements: Checks if nodes have enough CPU, memory, and other resources requested by the Pod.
  * Node affinity: Checks if the Pod should be scheduled on a node that has a specific label.
  * Inter-Pod affinity/anti-affinity: checks if the Pod should be scheduled near or far from another Pod.
  * Taints and tolerations: Checks if the Pod should be scheduled on a node that has a specific taint.
  * Node selectors: Checks if the Pod should be scheduled on a node that has a specific label.
  * Custom Policies: any custom scheduling policies or priorities such as the Pod Topology Spread Constraints set by the key "topology_spread_constraint".

The scheduler will then score each node that can host the Pod, based on the criteria above and any custom policies and then select the node
with the highest score to run the Pod on. If multiple nodes have the same highest score then one of them is chosen randomly. Once a node is selected, the scheduler assigns
the Pod to the node and the decision is then communicated back to the API server, which in turn notifies the Kubelet on the chosen node to start the Pod.
For more information on how the scheduler works, see the [Kubernetes documentation](https://kubernetes.io/docs/concepts/scheduling/scheduling-framework/).

How PaaSTA services are isolated from each other
------------------------------------------------

Given that a node may run multiple Pods for PaaSTA services, we need to ensure that Pods cannot
'interfere' with one another. We do this on a file system level using Docker -
processes launched in Docker containers are protected from each other and the
host by using kernel namespaces. Note that the use of kernel namespaces is a
feature of Docker - PaaSTA doesn't do anything 'extra' to enable this.

However, these Pods are still running and consuming resources on the same
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
Docker container, then the container will die. Kubernetes will restart the container
as the RestartPolicy for the container is set to "Always".

CPUs
""""

CPU enforcement is implemented slightly differently. Many people expect the
value defined in the ``cpus`` field in a service's soa-configs to map to a
number of cores that are reserved for a Pod. However, isolating CPU time like
this can be particularly wasteful; unless a task spends 100% of its time on
CPU (and thus has *no* I/O), then there is no need to prevent other Pods from
running on the spare CPU time available.

Instead, the CPU value is used to give Pods a relative priority. This priority
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

Disk
"""""

Kubernetes supports disk resource isolation through the use of storage quotas. Disk resource is isolated through the use of
namespaces - PaaSTA by default apply storage resource limit for the namespace if none is specified (Note: those limits can be overridden in soaconfigs).
