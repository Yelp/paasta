Contributing
============

Making new versions
-------------------
* Make a branch. WRITE TESTS FIRST (TDD)! Add features.

* Submit your branch for review. Include the "paasta" group. Communicate with
  the team to select a single designated Primary Reviewer.

* After ShipIts, merge your branch to master.

* Edit ``yelp_package/Makefile`` and bump the version in ``RELEASE``.

* ``make release`` and follow the instructions.

* This version will become live *automatically* if the test suite passes.

  * If you *do not want this*, go to Puppet and pin the ``paasta_tools``
    package to the current (without your changes) version. The ``mesosstage``
    cluster will still pick up your changes (due to that cluster's explicit
    hiera override of version to ``latest``) so you can test them there.

  * When you're done, remove the pin and let your version go out everywhere.


Testing command-line tab completion
-----------------------------------
We use `argcomplete <https://github.com/kislyuk/argcomplete>`_ to provide tab completion on the command-line. Testing it is a little tricky.

* There's some guidance in `argcomplete's Debugging section <https://github.com/kislyuk/argcomplete#debugging>`_.

* You can load the appropriate rules into your shell. Note that it is sensitive
  to the exact path you use to invoke the command getting autocomplete hints:

  * ``eval "$(.tox/py/bin/register-python-argcomplete ./paasta_tools/paasta_cli/paasta_cli.py)"``

* There is a simple integration test. See the itest/ folder.


Upgrading Components
--------------------

As things progress, there will come a time that you will have to upgrade
PaaSTA components to new versions.

* See `Upgrading Marathon <upgrading_marathon.html>`_ for how to upgrade Marathon safely.
