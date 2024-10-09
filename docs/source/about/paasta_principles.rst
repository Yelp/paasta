PaaSTA Principles
=================

These are a list of the principles that we think make PaaSTA special, and also
opinionated.  If you don't share these opinions, then PaaSTA is probably not
for you.

This document is similar, but not exactly the same as the
`12 factor <http://12factor.net/>`_ site for Heroku. The principles behind the
infrastructure *do* influence how the apps are deployed. The technical document
for the exact contract an app must meet to run on PaaSTA is documented in the
`PaaSTA Contract <contract.html>`_.

Principles
----------

0. A Bespoke PaaS is not Necessarily a Bad Thing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Building your own PaaS is a non-trivial undertaking. The decision to build your
own, even if it makes heavy use of existing components (like PaaSTA), should
not be taken lightly.

On the plus side, building your own PaaS can give your organization more value
than adopting an existing commercial or third-party PaaS. The value comes from
the fact that you can build your PaaS to meet your specific needs.
Additionally, much of the existing engineering effort towards your current
infrastructure can often be re-used. For example if you have an existing
service-discovery mechanism, you can build a PaaS that integrates tightly
with it, as opposed to replacing it with a different service-discovery mechanism
from a third-party.

There are of course downsides to building your own PaaS. There is certainly
engineering effort involved, but that must be weighed against the
engineering effort involved in *not* building your own PaaS (integration,
training, and migration costs). You also lose the support of the community,
or in the case of a commercial PaaS, the support of a vendor. Problems with
your own PaaS are not "googleable". No new employees will know how your PaaS
works, you cannot hire existing experts on it.

PaaSTA was built because we believed we could provide business value by
building our own PaaS to seamlessly meld with Yelp's existing infrastructure. At
the same time PaaSTA strives to avoid the downsides of building your own PaaS
by heavily reusing existing open source components.

1. **Declarative** is better than **imperative**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a subtle difference between these two approaches to configuring
a particular app in a theoretical PaaS:

+---------------------------------------------+-------------------------------------+
| Declarative                                 | Imperative                          |
+=============================================+=====================================+
| ::                                          | ::                                  |
|                                             |                                     |
|   $ cat >kubernetes-cluster.yaml <<EOF        |                                     |
|   web:                                      |                                     |
|     env:                                    |                                     |
|       PRODUCTION: true                      |   $ paas config:set PRODUCTION=true |
|     instances: 5                            |   $ paas ps:scale web=5             |
|   EOF                                       |                                     |
|                                             |                                     |
|   $ git commit -m "Set myapp to production" |                                     |
|   $ git push origin HEAD                    |                                     |
+---------------------------------------------+-------------------------------------+

To frame it in a different light, look at the difference between these approaches to
installing a package on a server:

+---------------------------------------------+-------------------------------------+
| Declarative                                 | Imperative                          |
+=============================================+=====================================+
| ::                                          | ::                                  |
|                                             |                                     |
|   $ vim puppet.pp                           |                                     |
|   package { 'apache':                       |                                     |
|     ensure => '2.4.17',                     |                                     |
|   }                                         |   $ apt-get install apache=2.4.17   |
|   $ git commit -m "upgrade apache"          |                                     |
|   $ git push origin HEAD                    |                                     |
+---------------------------------------------+-------------------------------------+

At first glance, the imperative approach looks "simpler" and maybe "easier".
For PaaSTA, we think that having config files that *declare* how apps are
deployed is a superior way to define your infrastructure, in the same way that
configuration management is superior to running commands on a server.
Specifically:

* It allows config files to be viewed and edited en masse.
* Storing the config files in a SCM tool allows for easy change tracking, rollbacks,
  and auditing.
* Declaring intent in config files allows the infrastructure to correct itself when
  things get out of sync, as it can reference what things "should" be.
* Config files allow humans and other tools easy access to see the global state of
  things, without querying a database.

2. Git is a pretty good control plane
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PaaSTA uses Git as the source of truth about which versions of a service should be
deployed for which clusters. This has some nice benifits:

* No need for a database to store the desired state of the services
* Git allows for decent auditing of what was deployed, when, and by whom
* Advanced git hosting (Gitolite, Gitlab, Github) makes it easy to use similar ACLs
  that are used for both commiting and deploying code

One downside to using git as a control plane is that it means PaaSTA components
might need to access this metadata from remote environments. PaaSTA solves this
by generating a ``deployments.json`` for each service and using `soa-configs <../soa_configs.html>`_ to
distribute it. This allows PaaSTA to extract all the metadata out of a git repo
once, and distribute it globally to each PaaSTA cluster. It also helps isolate
PaaSTA from git failures or outages.

3. Services should be **owned** and monitored
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most of the PaaSTA code base assumes that team data is available for a service.
That is because at Yelp, all services are owned by some team.

This principle manifests in PaaSTA through the `monitoring.yaml <../yelpsoa_configs.html#monitoring-yaml>`_ file. The
minimal amount of data required in that file is the ``team``. Additionally
we encourage services to have at least a ``description`` and ``external_link``
in `soa-configs <../soa_configs.html>`_.

This helps emphasize that PaaSTA is built for the long-haul, and designed to
run services in a production setting. Things are monitored by default, and alerts
are sent to the team that owns the service. PaaSTA isn't optimized for quickly
spinning up, say, "a Redis container in prod". It is optimized for services that
exist for months or years.

4. Services (and hence Docker images) should be reproducible
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The PaaSTA architecture is built with the assumption that the ultimate source
of truth about a service is its source code repository. In theory, the state of
a production PaaSTA cluster could be reproduced from just the `soa-configs <../soa_configs.html>`_
repo and the source code repo of the services deployed there.

This is different than other Docker-based platforms, where the fundamental
building block is the *Docker image*. With PaaSTA the Docker image is treated
as only an intermediate artifact. In fact, with PaaSTA the user never actually
specifies the exact Docker image to use. The image name is programmatically
generated from the service name and Git sha that generated it.

Enforcing this principle at the infrastructure level ensures that every
service can be traced to its source Git repo. This principle is analogous
to requiring that system images for things like Amazon AMIs, Vagrant
boxes, or VMware images are reproducible from scratch.

The downside to this principle is that it discourages users from pulling images
directly off a remote image repository, like the Docker Hub, and using the
image as-is. Such images may or may not be reproducible. The upside is that
(hopefully) more thought is put into the sustainability of such a practice,
especially in a production environment.
