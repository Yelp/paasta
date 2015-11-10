[![Build Status](https://travis-ci.org/Yelp/paasta.svg?branch=master)](https://travis-ci.org/Yelp/paasta)
[![Coverage Status](https://coveralls.io/repos/Yelp/paasta/badge.svg)](https://coveralls.io/r/Yelp/paasta)

# PaaSTA - Build, Deploy, Connect, and Monitor Services

PaaSTA is a highly-available, distributed system for building, deploying, and
running services using containers and Apache Mesos!

*Note*: PaaSTA has been running in production at Yelp for more than a year,
and has a number of "Yelpisms" still lingering in the codebase. We have made
efforts to excise them, but there are bound to be lingering issues. Please help us
by opening an [issue](https://github.com/Yelp/paasta/issues/new) or
better yet a [pull request](https://github.com/Yelp/paasta/pulls).

## Components

*Note*: PaaSTA is an opinionated platform that uses a few un-opinionated
tools. It requires a non-trivial amount of infrastructure to be in place
before it works completely:

 * [Docker](http://www.docker.com/) for code delivery and containment
 * [Mesos](http://mesos.apache.org/) for code execution and scheduling (runs Docker containers)
 * [Marathon](https://mesosphere.github.io/marathon/) for managing long-running services
 * [Chronos](https://mesos.github.io/chronos/) for running things on a timer (nightly batches)
 * [SmartStack](http://nerds.airbnb.com/smartstack-service-discovery-cloud/) for service registration and discovery
 * [Sensu](https://sensuapp.org/) for monitoring/alerting
 * [Jenkins](https://jenkins-ci.org/) (optionally) for continuous deployment

If you are looking for a project that doesn't require external components, we encourage you
to look at the doc [comparing PaaSTA to other tools](https://github.com/Yelp/paasta/blob/master/comparison.md).

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

See the [getting started](http://paasta.readthedocs.org/en/latest/installation/getting_started.html)
documentation for how to deploy PaaSTA.

## Documentation

Read the documentation at [Read the Docs](http://paasta.readthedocs.org/en/latest/).

## License

PaaSTA is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Contributing

Everyone is encouraged to contribute to PaaSTA by forking the
[Github repository](http://github.com/Yelp/PaaSTA) and making a pull request or
opening an issue.

