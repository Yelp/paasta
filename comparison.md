# PaaSTA Compared to Other Software

> I wonder how many organizations that say they're "doing DevOps" are actually
> building a bespoke PaaS. And how many of those realize it.
- [Mark Imbriaco](https://twitter.com/markimbriaco/status/516444148048887808)

In general PaaSTA is difficult to compare directly to individual products because
PaaSTA itself is an integration of many existing tools.

The problem-space of software deployment is very vast, covering lots problems that
must be solved in one way or another:

* Code packaging and distribution
* Resource Scheduling
* Log aggregation / distribution
* Service Discovery
* Monitoring / Alerting
* Workflow / Orchestration

Some tools try to solve some subset of the above topics. This document is not
meant to be a comparison between every tool out there, but instead is just
designed to give a general overview about what make PaaSTA different, compared
to some of the more popular tools in the same space.

## Hashicorp Nomad + Consul

[Hashicorp Nomad](https://www.nomadproject.io/) is a resource manager and
cluster manager. It integrates well with Hashicorp's other products like Consul
for service discovery.

PaaSTA uses Mesos for resource management, Marathon for task scheduling, and
Smartstack for service discovery. PaaSTA itself just glues the pieces together,
and in theory could use Nomad for launching jobs as well, but it currently does
not.

Hashicorp has its own page comparing the different scheduler idiosyncrasies of
[Mesos versus Nomad](https://www.nomadproject.io/intro/vs/mesos.html).

## Amazon ECS (Elastic Container Service)

Amazon ECS is an Amazon-specific method of launching docker containers on EC2
instances.

PaaSTA is designed to be infrastructure-agnostic, and can run in EC2 just as easily
as it can run on bare metal.

PaaSTA is also much more opinionated about *how* to deploy docker containers in a way
that meets a specified job definition. ECS is an API and is designed to used by
higher-level tools, like [Empire](https://github.com/remind101/empire)

## Kubernetes

Kubernetes is a cluster manager and resource manager. It also does load balancing and
service discovery. It also can do secret distribution and monitoring.

Kubernetes can be closely compared to PaaSTA because they do most of the same
functions.  The main difference is that PaaSTA uses existing open source
technologies to do them, instead of of a single go binary. The consolidated
approach has the benefit of making it easier to setup and deploy, but long term
using external components allows for flexibility in designing a PaaS.

## Heroku

Heroku is a full PaaS, and has a feature set more comparable to PaaSTA. PaaSTA uses
docker containers, which leaves it up to the service author to but their own
software stack, compared to Heroku's provided stacks and buildpacks.

One of the main philosophical differences between Heroku and PaaSTA is that Heroku
is "imperative", in the sense that in order to make changes in Heroku, one must
invoke an action, either using their dashboard or CLI. PaaSTA is designed to be
"declarative", and uses config files and docker tags to be the "source of truth"
for "what is supposed to be running where and how".

Another major difference between the architecture of Heroku and PaaSTA is the
cluster design. In PaaSTA there are many different clusters that code can be
deployed to. In Heroku there is only the main Heroku platform, and users are
expected to run different apps. (test-appname, staging-appname, etc)
