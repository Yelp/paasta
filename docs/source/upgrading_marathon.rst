Upgrading Marathon
==================

The Marathon App itself is deployed via our `continuous
integration package workflow <https://jenkins.yelpcorp.com/view/packages-marathon/>`_.
Currently, it is using the official packages from the Mesosphere repository.

Releases for Marathon are `here <https://github.com/mesosphere/marathon/releases>`_.
Any API-breaking changes are usually listed in the changelog.

We use the Marathon Python client
`here <https://github.com/thefactory/marathon-python/blob/master/CHANGELOG.md>`_.


Orchestrating Marathon Upgrades
-------------------------------

In general, the Marathon Python client is not forward compatible with API
changes. This usually drives the orchestration sequence as follows:

#. Upgrade to the latest version of the Python client.

   #. Bump the version we want in paasta_tool's setup.py and requirements.txt.

#. Run the integration tests to ensure the new client works with the existing
   marathon container.

   #. Verify the marathon container version in the docker-compose.yml used by the integration tests.
   #. Run the tests (``tox -e paasta_itests``).

#. If passing, deploy the new version of paasta_tools with the new client library.
   (follow the `standard release cycle stuff <contributing.html#making-new-versions>`_)

#. Once deployed everywhere, pull in the new version of the Marathon package.

   #. Clone the repo where it is built. (``<git@git.yelpcorp.com:packages/marathon>``)
   #. Bump the version numbers in the Makefile.
   #. Push and let Jenkins build the new package.
   #. Update Puppet to use the new version of Marathon. This is done using Hiera.
         (see the `puppet code <https://opengrok.yelpcorp.com/xref/sysgit/puppet/modules/profile_paasta/manifests/marathon.pp>`_)
   #. Using ``./orchestration_tools/upgrade_marathon.sh``, perform the Marathon upgrade in each cluster.

If you find that there is incompatibility anywhere along the way, use judgement
to decide what steps need to be re-ordered preserve Marathon availability.
