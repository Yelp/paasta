[![Build Status](https://github.com/Yelp/paasta/actions/workflows/ci.yml/badge.svg?query=branch%3Amaster)](https://github.com/Yelp/paasta/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/paasta/badge/?version=latest)](https://paasta.readthedocs.io/en/latest/?badge=latest)

# PaaSTA - Build, Deploy, Connect, and Monitor Services
![PaaSTA Logo](http://engineeringblog.yelp.com/images/previews/paasta_preview.png)

PaaSTA is a highly-available, distributed system for building, deploying, and
running services using containers and Kubernetes.

PaaSTA has been running production services at Yelp since 2016. It was
originally designed to run on top of Apache Mesos but has subsequently been
updated to use Kubernetes. Over time the features and functionality that
PaaSTA provides have increased but the principal design remains the same.

PaaSTA aims to take a declarative description of the services that teams need
to run and then ensures that those services are deployed safely, efficiently,
and in a manner that is easy for the teams to maintain. Rather than managing
Kubernetes YAML files, PaaSTA provides a simplified schema to describe your service
and in addition to configuring Kubernetes it can also configure other infrastructure
tools to provide monitoring, logging, cost management etc.

Want to know more about the opinions behind what makes PaaSTA special? Check
out the [PaaSTA Principles](http://paasta.readthedocs.io/en/latest/about/paasta_principles.html).

## Components

*Note*: PaaSTA is an opinionated platform that uses a few un-opinionated
tools. It requires a non-trivial amount of infrastructure to be in place
before it works completely:

 * [Docker](http://www.docker.com/) for code delivery and containment
 * [Kubernetes](https://kubernetes.io/) for code execution and scheduling (runs Docker containers)
 * [Tron](https://tron.readthedocs.io/en/latest/) for running things on a timer (nightly batches)
 * [SmartStack](http://nerds.airbnb.com/smartstack-service-discovery-cloud/) and [Envoy](https://www.envoyproxy.io/) for service registration and discovery
 * [Sensu](https://sensu.io/) for monitoring/alerting
 * [Jenkins](https://jenkins-ci.org/) (optionally) for continuous deployment
 * [Prometheus](https://prometheus.io/) and [HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) for autoscaling services

One advantage to having a PaaS composed of components like these is you
get to reuse them for other purposes. For example, at Yelp Sensu is not just for
PaaSTA, it can be used to monitor all sorts of things. We also use Kubernetes to run
other more complex workloads like [Jolt](https://dcos.io/events/2017/jolt-distributed-fault-tolerant-tests-at-scale-on-mesos/) and [Cassandra](https://engineeringblog.yelp.com/2020/11/orchestrating-cassandra-on-kubernetes-with-operators.html). Our service mesh, which
is a heavily customised version of SmartStack and Envoy, allows many systems at Yelp
to communicate with PaaSTA services and each other.

On the other hand, requiring lots of components, means lots of infrastructure to
setup before PaaSTA can work effectively! Realistacally, running PaaSTA outside of Yelp
would not be sensible, because in addition to the integrations mentioned above we also
have strong opinions encoded in other tooling that you would need to replicate. Nevertheless,
we code PaaSTA in the open because we think it is useful to share our approach and hope that
the code can at least help others understand or solve similar problems.

## Integrations and Features

In addition to the direct integrations above PaaSTA also relies on other components
to provide PaaSTA users with other features and to manage compute capacity at Yelp.

* We use [Karpenter](https://karpenter.sh/) to autoscale pools of EC2 instances to run PaaSTA. Formerly we used our own autoscaler [Clusterman](https://engineeringblog.yelp.com/2019/11/open-source-clusterman.html)
* We bake AMIs using [Packer](https://www.packer.io/)
* We collect logs from services and send them via [Monk](https://engineeringblog.yelp.com/2020/01/streams-and-monk-how-yelp-approaches-kafka-in-2020.html) to [Kafka](https://kafka.apache.org/)
* We use [StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) to run a few stateful PaaSTA services
* We autotune the resources needed by each service by monitoring usage (similar to [VPA](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler))

## Design Goals

 * Declarative, rather than imperative, control
 * Fault tolerance
 * Service isolation
 * Efficient use of resources
 * No single points of failure
 * Pleasant interface

## Getting Started

See the [getting started](http://paasta.readthedocs.io/en/latest/installation/getting_started.html)
documentation for how to deploy PaaSTA. This reference is intended to help understand how PaaSTA
works but we don't advise that you use PaaSTA in production.

## Debugging PaaSTA (in VS Code)

To debug PaaSTA in VS Code, please refer to the internal PaaSTA wiki page "[Debugging PaaSTA (in VS Code)](https://y.yelpcorp.com/paasta-vscode)".

## Documentation

Read the documentation at [Read the Docs](http://paasta.readthedocs.io/en/latest/).

## Yelp-internal Documentation/Links
* [HPA](http://y/hpa)
* [Service Deployment](http://y/service-deploys)

## Videos / Talks About PaaSTA

* [EvanKrall](https://github.com/EvanKrall) speaks at [QCon NYC 2015](http://www.infoq.com/presentations/paasta-yelp) (Oct 2015)
* [EvanKrall](https://github.com/EvanKrall), [solarkennedy](https://github.com/solarkennedy), and [jnb](https://github.com/jnb) give a [behind the scenes tour of PaaSTA at Yelp](https://vimeo.com/141231345) (Oct 2015)
* [Rob-Johnson](https://github.com/Rob-Johnson) talks about PaaSTA at [MesosCon 2015](https://www.youtube.com/watch?v=fxYfmzWctRc) (Nov 2015)
* [solarkennedy](https://github.com/solarkennedy) presents at Box to give a [Theory of PaaSes](https://youtu.be/YFDwdRVTg4g?t=33m11s) (Jan 2016)
* [nhandler](https://github.com/nhandler) speaks at OSCON about Running Applications at Yelp ([Slides](http://www.slideshare.net/NathanHandler/paasta-running-applications-at-yelp) / [Video](https://youtu.be/vISUXKeoqXM)) (May 2016)

## License

PaaSTA is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Contributing

Everyone is encouraged to contribute to PaaSTA by forking the
[Github repository](http://github.com/Yelp/PaaSTA) and making a pull request or
opening an issue.
