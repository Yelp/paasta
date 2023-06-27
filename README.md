[![Build Status](https://github.com/Yelp/paasta/actions/workflows/ci.yml/badge.svg?query=branch%3Amaster)](https://github.com/Yelp/paasta/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/paasta/badge/?version=latest)](https://paasta.readthedocs.io/en/latest/?badge=latest)

# PaaSTA - Build, Deploy, Connect, and Monitor Services
![PaaSTA Logo](http://engineeringblog.yelp.com/images/previews/paasta_preview.png)

PaaSTA is a highly-available, distributed system for building, deploying, and
running services using containers and Apache Mesos!

Want to know more about the opinions behind what makes PaaSTA special? Check
out the [PaaSTA Principles](http://paasta.readthedocs.io/en/latest/about/paasta_principles.html).

*Note*: PaaSTA has been running in production at Yelp for years,
and has a number of "Yelpisms" still lingering in the codebase. We have made
efforts to excise them, but there are bound to be lingering issues. Please help us
by opening an [issue](https://github.com/Yelp/paasta/issues/new) or
better yet a [pull request](https://github.com/Yelp/paasta/pulls).

## Components

*Note*: PaaSTA is an opinionated platform that uses a few un-opinionated
tools. It requires a non-trivial amount of infrastructure to be in place
before it works completely:

 * [Docker](http://www.docker.com/) for code delivery and containment
 * [Mesos](http://mesos.apache.org/) / [Kubernetes](https://kubernetes.io/) for code execution and scheduling (runs Docker containers)
 * [Marathon](https://mesosphere.github.io/marathon/) for managing long-running services
 * [Tron](https://tron.readthedocs.io/en/latest/) for running things on a timer (nightly batches)
 * [SmartStack](http://nerds.airbnb.com/smartstack-service-discovery-cloud/) / [Envoy](https://www.envoyproxy.io/) for service registration and discovery
 * [Sensu](https://sensuapp.org/) for monitoring/alerting
 * [Jenkins](https://jenkins-ci.org/) (optionally) for continuous deployment

The main advantage to having a PaaS composed of components like these is you
get to reuse them for other purposes. For example at Yelp Sensu is not just for
PaaSTA, it can be used to monitor all sorts of things. Also Mesos can be
re-used for things like custom frameworks. For example at Yelp we use the Mesos
infrastructure to run our large-scale testing framework:
[Jolt](https://dcos.io/events/2017/jolt-distributed-fault-tolerant-tests-at-scale-on-mesos/).
SmartStack is used at Yelp for service discovery for Non-PaaSTA things as well,
like databases, legacy apps, and Puppet-defined apps. Most PaaS's do not
allow for this type of component re-use.

On the other hand, requiring lots of components means lots of infrastructure to
setup before PaaSTA is fully baked. If you are looking for a project that
doesn't require external components, we encourage you to look at the doc
[comparing PaaSTA to other tools](https://github.com/Yelp/paasta/blob/master/comparison.md).

## Design Goals

 * Declarative, rather than imperative, control
 * Fault tolerance
 * Service isolation
 * Efficient use of resources
 * No single points of failure
 * Pleasant interface

PaaSTA is an opinionated platform, and it is not designed to interoperate with
every possible backend service out there.

Think of it as an example of how we have integrated these technologies together
to build a cohesive PaaS. It is not a turn-key PaaS solution.

## Getting Started

See the [getting started](http://paasta.readthedocs.io/en/latest/installation/getting_started.html)
documentation for how to deploy PaaSTA.

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
