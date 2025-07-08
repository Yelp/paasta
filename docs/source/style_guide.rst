=======================
Development Style Guide
=======================

This style guide documents the conventions used by the PaaSTA tools. They
are written down here for reference so future development will retain a
consistent "look and feel". A consistent style and user interface makes the
PaaSTA tools easier to learn and use, more predictable, and hopefully more fun.

General Guidelines
==================

Logging
-------

There are 3 possible channels of logging to consider. Here are the general
guidelines:

* Interactive tools SHOULD send their output to stdout.
* Use Scribe ``event`` level messages when they are service-specific

Examples
^^^^^^^^

Good:
 * paasta mark-for-deployment => Goes to stdout. (But also scribe)
Bad:
 * paasta mark-for-deployment => Going to scribe only, which is surprising.

Good:
 * paasta itest => Sends event-level detail to stdout

Example::

  itest for example_service completed successfully
  And sends the debug level to stderr
  make itest
  ....
  leaving directory []...
  etc"

Bad:
 * paasta itest => Sends all output to scribe, no output to stdout. Jenkins console output is empty and surprises users.

 * Scribe: Tools that contribute to the overall flow of a service pipeline should log to scribe with their given component. Only log lines that are specific to a service should be sent here. Logging to scribe should be selective and precise as to not overwhelm the event stream.

 * Anything going to scribe should ALSO go to stdout.

Good:
 * setup_kubernetes_job => general output to stdout, app-specific output to scribe
Bad:
 * setup_kubernetes_job | stdint2scribe (no selective filtering, raw stdout dump)

Good:
 * paasta itest => Sends summary of pass or fail to scribe event log. Sends full output of the run to the scribe debug log
Bad:
 * paasta itest => Sends every line of the ``make itest`` output to ``event`` level, drowning out other key event lines.

 * Syslog: Non-interactive system processes that do not send data to developers can use syslog, but via stdout => logger. Do not send to syslog directly.

 * If there messages that are relevant to a PaaSTA consumer, that should go to Scribe so it can be read via the normal channels.

Good:
 * sync_jobs_cron | logger -t paasta_sync_jobs
Bad:
 * sync_jobs_cron (no output, silently goes to syslog)


Scribe Logging Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~~

Scribe logs should be separated into ``event`` level detail and ``debug``
level detail.

Event Level
~~~~~~~~~~~

Event Level General Guidelines:

* All event-level scribe logs should be as terse as possible while still providing a high level summary of the events occurring in the infrastructure.
* All state changing events MUST have at least one event-level scribe log line emitted.
* It is not necessary to repeat redundant information, like service name, as all PaaSTA log invocations already are service-specific anyway.
* All event level logs SHOULD use active verbs to indicate the action that took place.
* Log lines SHOULD NOT contain the log level that they are using *in* the log line. Don't try to emulate syslog.
* If an external URL with more context is available, the log line SHOULD reference it, but only if an error or warning is detected.
* All event-level logs should also go to stdout.

Good examples of things that would be in the ``event`` level log stream:

* ``40e74f marked for deployment in cluster.main``
* ``upthendown bounce initiated on instance main``
* ``itest Passed for 9e2990.``
* ``itest Failed for 9e2990. More info: http://....``

Bad Examples of things for the ``event`` log:

* ``Service: example_service Cluster: cluster Instance: main is deployed``
* ``executed command: git push -f cluster.main``
* ``example_service.main is healthy``
* ``ERROR: itest failed for 9e2990``

Debug Level
~~~~~~~~~~~

Debug Level General Guidelines:

* Viewing Debug level logs SHOULD NOT be necessary under normal PaaSTA operation.
* Debug logs are for providing additional context when things go wrong.
* Debug logs should still use active verbs and not repeat redundant information if possible.
* All debug-level logs should also go to stderr.

Good examples of things that would be in the ``debug`` level log stream:

* ``"git push -f cluster.main" returned code 0``
* Output of make itest, one log-line per line of output

Example::

  make itest
  running /itest/ubuntu.sh
  exit code 0

* ``Scaling main to 5 instance for crossover bounce``
* ``Cleaning up old app id "example_service.main.git2345" for upthendown bounce``

Components
~~~~~~~~~~
TBD


Interactive Command Line Tools
==============================

Interactive command line tools are commands that are expected to be run by a
human. They MUST be subcommands of the ``paasta`` super command. (like git)

Tab Completion
--------------

``paasta`` subcommands SHOULD add tab_completion completers when possible.
Tab completion MUST be fast and take under 500 milliseconds to be pleasant.

Tab completion MUST be a superset of the possible values for a command line
argument. It MUST NOT be a subset, because that might autocomplete something
undesirable. For example: If you want to type in ``example_baz`` and the
tab completer has completions for ``example_foo`` and ``bar``, the tab completer
would fill in ``example_foo`` and make you backspace. This should not happen.

Colors
------

Because these are interactive tools, color SHOULD be used to enhance the
readability of the output.

The following colors should be used for different cases:
* links: Cyan
* Healthy things: Green or Bold
* Warning: Yellow
* Failed: Red


General Python
==============

In general, in the `paasta_tools` repo we use `flake8` to enforce Python style stuff.

Positional Args Versus Keyword Args
-----------------------------------

When in doubt, use `keyword arguments <https://docs.python.org/3/faq/programming.html#faq-argument-vs-parameter>`_
to increase readability of the arguments to your function call, at the expense of verboseness.

Doing so allows arguments to function calls to be "order independent" and can
eliminate bugs caused by passing in arguments in the wrong order.
