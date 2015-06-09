Upgrading Marathon
==================

The Marathon App itself is deployed via Docker using our `continuous
integration workflow <https://jenkins.yelpcorp.com/view/docker-images-marathon/>`_.

Releases for Marathon are `here <https://github.com/mesosphere/marathon/releases>`_.
Any API-breaking changes are usually listed in the changelog.

We use the Marathon Python client
`here <https://github.com/thefactory/marathon-python/blob/master/CHANGELOG.md>`_.


Orchestrating Marathon Upgrades
-------------------------------

In general, the Marathon Python client is not forward compatible with API
changes. This usually drives the orchestration sequence as follows:

#. Upgrade to the latest version of the Python client.

   #. Add the new package to our `pypi <https://trac.yelpcorp.com/wiki/InternalPyPI#AddinganewopensourcepackagetoourInternalPyPi>`_.
   #. Hint: ``$ fetch_python_package marathon==0.6.15``
   #. Bump the version we want in paasta_tool's setup.py and requirements.txt.

#. Run the integration tests to ensure the new client works with the existing
   marathon container.

   #. Verify the marathon container version in the fig.yml used by the integration tests.
   #. Run the tests (``tox -e marathon_integration``).

#. If passing, deploy the new version of paasta_tools with the new client library.
   (follow the `standard release cycle stuff <contributing.html#making-new-versions>_`)

#. Once deployed everywhere, pull in the new version of the Marathon container.

   #. Clone the repo where it is built. (``<git@git.yelpcorp.com:docker-images/marathon>``)
   #. Bump as necessary; let jenkins push out the new container.
   #. Ask puppet to run the new version of the container in a safe area (e.g. mesosstage).
         if that works, ask puppet to run the new version everywhere.
         (see the `puppet code <https://opengrok.yelpcorp.com/xref/sysgit/puppet/modules/profile_paasta/manifests/marathon.pp>`_)


If you find that there is incompatibility anywhere along the way, use judgement
to decide what steps need to be re-ordered preserve Marathon availability.
