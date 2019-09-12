Testing
=======

We write tests for PaaSTA in a few ways. This document focusses on the developer experience for writing tests.
We use `tox <https://tox.readthedocs.io/en/latest/>`_ for orchestrating the creation of virtualenvs and execution of tests. We wrap tox invocations with
make targets to ease their usage.

Unit Tests
==========

We write tests for paasta-tools using `pytest <https://docs.pytest.org/en/latest/>`_.

To run all tests:

``make test``

To run a tests for a specific module:

Ensure the virtualenv created by tox is activated

``source .tox/py36/bin/activate``

And execute pytest

``py.test tests/test_async_utils.py``

More advanced usage of pytest can be found in the `docs <https://docs.pytest.org/en/latest/example/pythoncollection.html#changing-standard-python-test-discovery>`

Integration Tests
=================

We also write integration tests, to have confidence that the system as a whole behaves as one would expect.
Given the number of moving parts in a fully deployed PaaSTA setup, there is a lot of infrastructure required to run behavior tests.

To do so, we use `Docker Compose <https://docs.docker.com/compose/>`_ to launch a number of containers, including one to install paasta-tools into and run tests from.

To write integration tests, we use `behave <https://behave.readthedocs.io/en/latest/>`_.

To run the full suite of integration tests, you can run

``make itest``

Warning: This might take some time.

Other Testing
=============

If you want to iterate against a semi realistic cluster, then you can use the example cluster.
See the `docs <intallation/example_cluster.html>`_ for instructions using it.
