# PaaSTA Compared to Other Software

> I wonder how many organizations that say they're "doing DevOps" are actually
> building a bespoke PaaS. And how many of those realize it.
- [Mark Imbriaco](https://twitter.com/markimbriaco/status/516444148048887808)

In general PaaSTA is difficult to compare directly to individual products because
PaaSTA itself is an integration of many existing tools.

The problem space of software deployment is vast, covering lots of problems that
must be solved in one way or another:

* Code packaging and distribution
* Resource scheduling
* Service Discovery
* Monitoring / alerting
* Workflow / orchestration

Some tools try to solve some subset of the above topics. This document is not
meant to be a comparison between every tool out there, but instead is just
designed to give a general overview about what makes PaaSTA different, compared
to some of the more popular tools in the same space.

As a baseline for comparison, here are the pieces that PaaSTA uses to solve
the paritcular problems associated with running a production PaaS:

| Problem             | PaaSTA Solution              |
|---------------------|------------------------------|
| Code containerizer  | Docker                       |
| Scheduling          | Mesos + Marathon             |
| Service Discovery   | SmartSTack                   |
| Monitoring          | Sensu                        |
| Workflow            | Jenkins or CLI + soa-configs |

Hopefully by looking at these particular sub-problems we can better compare the
different technologies out there.

## Hashicorp Nomad

| Problem             | Nomad/Hashicorp Solution |
|---------------------|--------------------------|
| Code containerizer  | Docker                   |
| Scheduling          | Nomad                    |
| Service Discovery   | Consul                   |
| Monitoring          | Atlas?                   |
| Workflow            | CLI                      |

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

| Problem             | ECS/Amazon Solution |
|---------------------|---------------------|
| Code containerizer  | Docker              |
| Scheduling          | ECS                 |
| Service Discovery   | ELBs + DNS          |
| Monitoring          | CloudWatch          |
| Workflow            | Console or CLI      |

Amazon ECS is an Amazon-specific method of launching Docker containers on EC2
instances.

PaaSTA is designed to be infrastructure-agnostic, and can run in EC2 just as easily
as it can run on bare metal.

PaaSTA is also much more opinionated about *how* to deploy Docker containers in a way
that meets a specified job definition. ECS is an API and is designed to be used by
higher-level tools, like [Empire](https://github.com/remind101/empire)

## Kubernetes

| Problem             | Kubernetes Solution                |
|---------------------|------------------------------------|
| Code containerizer  | Docker                             |
| Scheduling          | Kubernetes core                    |
| Service Discovery   | Env Vars or Kubernetes DNS Service |
| Monitoring          | Kubernetes + Webhooks              |
| Workflow            | CLI / API                          |

[Kubernetes](http://kubernetes.io/) is a cluster manager and resource manager.
It does load balancing and service discovery. It also can do secret
distribution and monitoring.

Kubernetes can be closely compared to PaaSTA because they do most of the same
functions.  The main difference is that PaaSTA uses existing open source
technologies, instead of of a single Go binary. The consolidated
approach has the benefit of making it easier to setup and deploy, at the expense
of being able to swap out components.

PaaSTA trades the convenience of a unified binary for the ability to use proven
existing technologies. The downside to this approach is the cost of integration
and deployment.

## Heroku

| Problem             | Heroku Solution     |
|---------------------|---------------------|
| Code containerizer  | Dynos / cgroups     |
| Scheduling          | Heroku              |
| Service Discovery   | DNS + Heroku Router |
| Monitoring          | via Addons          |
| Workflow            | CLI or Dashboard    |

[Heroku](https://www.heroku.com) is a full PaaS, and has a feature set more
comparable to PaaSTA. PaaSTA uses Docker containers, which leaves it up to the
service author to build their own software stack, compared to Heroku's provided
stacks and buildpacks.

One of the main philosophical differences between Heroku and PaaSTA is that Heroku
is "imperative", in the sense that in order to make changes in Heroku, one must
invoke an action, either using their dashboard or CLI. PaaSTA is designed to be
"declarative", and uses config files and Docker tags to be the source of truth
for "what is supposed to be running where and how".

Another major difference between the architecture of Heroku and PaaSTA is the
cluster design. In PaaSTA there are many different clusters that code can be
deployed to. In Heroku there is only the main Heroku platform, and users are
expected to run different apps. (test-appname, staging-appname, etc)

## Flynn

| Problem             | Flynn Solution     |
|---------------------|--------------------|
| Code containerizer  | Docker             |
| Scheduling          | Flynn              |
| Service Discovery   | DNS + Flynn Router |
| Monitoring          | N/A?               |
| Workflow            | CLI                |

Flynn is a Heroku-inspired PaaS. Flynn is unique in this comparison because it
has first-class support for its embedded Postgress appliance, analogous to
Heroku's Postgres Addon or Amazon's RDS. This reduces the number of components
required to run a fully working setup, assuming Postgres meets the developers'
needs. Most other PaaS's view this problem as "out of scope", including PaaSTA.

Depending on your opinons on the [Twelve-Factor App manifesto](http://12factor.net/),
Flynn, Heroku, or [Empire](http://empire.readthedocs.org/en/latest/) may be good
solutions for environments that have apps that already conform to the twelve-factor
specification.
