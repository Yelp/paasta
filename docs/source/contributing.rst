Contributing
============

Running The Tests
-----------------

Unit Tests
^^^^^^^^^^

Python 3.6 and virtualenv are required for running the unit tests. You can simply run
``make test`` to execute them.

This will build a virtualenv with the required python packages, then run the tests
written in the ``tests`` directory.

Integration Tests
^^^^^^^^^^^^^^^^^

Python 3.6, virtualenv, and Docker are required to run the integration test suite.
You can run ``make itest`` to execute them.

Example Cluster
^^^^^^^^^^^^^^^^^
There is a docker compose configuration based on our itest containers that you
can use to run the paasta code against a semi-realistic cluster whilst you are
developing. More instructions `here <./installation/example_cluster.html>`_

System Package Building / itests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PaaSTA is distributed as a debian package. This package can be built and tested
with ``make itest_xenial``. These tests make assertions about the
packaging implementation.


Making new versions
-------------------
* Make a branch. WRITE TESTS FIRST (TDD)! Add features.

* Submit your branch for review. Include the "paasta" group. Communicate with
  the team to select a single designated Primary Reviewer.

* After ShipIts, merge your branch to master.

  * This version will become live *automatically* if the test suite passes.

  * If you *do not want this*, go to Puppet and pin the ``paasta_tools``
    package to the current (without your changes) version. The ``mesosstage``
    cluster will still pick up your changes (due to that cluster's explicit
    hiera override of version to ``latest``) so you can test them there.

      * If you do pin a specific version, email paasta@yelp.com to let the rest of the team know.

* Edit ``yelp_package/Makefile`` and bump the version in ``RELEASE``.

* ``make release`` and follow the instructions.

* If you pinned a specific version earlier, remove the pin and let your version go out everywhere when you're done.


Testing command-line tab completion
-----------------------------------
We use `argcomplete <https://github.com/kislyuk/argcomplete>`_ to provide tab completion on the command-line. Testing
it is a little tricky.

* There's some guidance in `argcomplete's Debugging section <https://github.com/kislyuk/argcomplete#debugging>`_.

* You can load the appropriate rules into your shell. Note that it is sensitive
  to the exact path you use to invoke the command getting autocomplete hints:

  * ``eval "$(.tox/py27/bin/register-python-argcomplete ./tox/py27/bin/paasta)"``

* There is a simple integration test. See the itest/ folder.

Upgrading Components
--------------------

As things progress, there will come a time that you will have to upgrade
PaaSTA components to new versions.

* See `Upgrading Mesos <upgrading_mesos.html>`_ for how to upgrade Mesos safely.
* See `Upgrading Marathon <upgrading_marathon.html>`_ for how to upgrade Marathon safely.
