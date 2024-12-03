Glossary
========

**Docker**
~~~~~~~~~~

Container `technology <https://www.docker.com/whatisdocker/>`_ that
PaaSTA uses.

**Kubernetes**
~~~~~~~~~~~~~~

`Kubernetes <https://kubernetes.io/>`_ (a.k.a. k8s) is the open-source system on which Yelp runs many compute workloads.
In Kubernetes, tasks are distributed to and run by servers called Kubelets (but a.k.a. kube nodes or Kubernetes agents) from the Kubernetes control plane.

**Kubernetes Deployment**
~~~~~~~~~~~~~~~~~~~~~~~~~

A Kubernetes resource that represents a collection of pods running the same application. A Deployment is responsible for creating and updating instances of your application.

**Kubernetes Node**
~~~~~~~~~~~~~~~~~~~

A node is a worker machine in a Kubernetes cluster that runs Pods.
In our case, it's usually a virtual machine provisioned via AWS EC2 Fleets or AutoScalingGroups

**Kubernetes Horizontal Pod Autoscaler (HPA)**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Kubernetes feature that automatically scales the number of pods in a deployment based on observed CPU utilization (or, with custom metrics support, on some other application-provided metrics).

**clustername**
~~~~~~~~~~~~~~~

A shortname used to describe a PaaSTA cluster. Use \`paasta
list-clusters\` to see them all.

**Kubernetes Pod**
~~~~~~~~~~~~~~~~~~~

Atomic deployment unit for PaaSTA workloads at Yelp and all Kubernetes clusters. Can be thought of as a collection of 1 or more related containers.
Pods can be seen as one or more containers that share a network namespace, at Yelp these are individual instances of one of our services, many can run on each server.

**Kubernetes Namespace**
~~~~~~~~~~~~~~~~~~~~~~~~

It provides a mechanism for isolating groups of resources within a single cluster. Each K8s Namespace can contain resources like
Pods and Deployments, and it allows for management and access controls to be applied at the Namespace level.

**instancename**
~~~~~~~~~~~~~~~~

Logical collection of Kubernetes pods that comprise an application (a Kubernetes Deployment) deployed on Kubernetes. service
name + instancename = Kubernetes Deployment. Examples: main, canary. Each instance represents a running
version of a service with its own configuration and resources.

**namespace**
~~~~~~~~~~~~~

An haproxy/SmartStack concept grouping backends that listen on a
particular port. A namespace may route to many healthy PaaSTA
instances. By default, the namespace in which a PaaSTA instance appears is
its instancename.

**Nerve**
~~~~~~~~~

A service announcement `daemon <https://github.com/airbnb/nerve>`_
that registers services in zookeeper to be discovered.

**PaaSTA**
~~~~~~~~~~

The name of the Platform as a Service that powers a
`Yellow pages knockoff <http://yelp.com/>`_.

**service\_configuration\_lib**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A python library for interacting with soa-configs.

**SmartStack**
~~~~~~~~~~~~~~

The brand name for Airbnb’s Nerve + Synapse service discovery solution.

**Synapse**
~~~~~~~~~~~

A local haproxy daemon that runs on yocalhost

**soa-configs**
~~~~~~~~~~~~~~~

A git repo for centralized service configs.

**yocalhost**
~~~~~~~~~~~~~

An ip that both on-metal and containers can use to connect to other
services.

**Zookeeper**
~~~~~~~~~~~~~

A distributed key/value store used by PaaSTA for coordination and
persistence.
