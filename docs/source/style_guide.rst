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

* Stdout: Standard stuff, mostly for interactive tools where data
  should be sent by default

  Good: `paasta mark-for-deployment => Goes to stdout. (and optionally scribe too)`
  Bad: `paasta mark-for-deployment => Going to scribe only, which is surprising`

* Syslog: Non-interactive system processes that do not send data to developers
  can use syslog, but via stdout => logger.

  Good: `sync_jobs_cron | logger -t paasta_sync_jobs`
  Bad: `sync_jobs_cron (no output, silently goes to syslog)`

* Scribe: Tools that contribute to the overall flow of a service pipeline
  should log to scribe with their given component. Only log lines that are
  specific to a service should be sent here. Logging to scribe should be
  selective and precise as to not overwhelm the event stream.

  Good: `setup_marathon_job => general output to stdout, app-specific output to scribe`
  Bad: `setup_marathon_job | stdint2scribe (no selective filtering, raw stdout dump)`


Interactive Command Line Tools
==============================

Interactive command line tools are commands that are expected to be run by a
human. They MUST be subcommands of the `paasta` super command. (like git)

Tab Completion
--------------

`paasta` subcommands SHOULD add tab_completion completers when possible.
Tab completion MUST be fast and take under 500 milliseconds to be pleasant.

Tab completion MUST be a superset of the possible values for a command line
argument. It MUST NOT be a subset, because that might autocomplete something
undesirable. For example: If you want to type in `example_baz` and the
tab completer has completions for `example_foo` and `bar`, the tab completer
would fill in `example_foo` and make you backspace. This should not happen.

Colors
------

Because these are interactive tools, color SHOULD be used to enhance the
readability of the output.

The following colors should be used for different cases:
* links: Cyan
* Healthy things: Green or Bold
* Warning: Yellow
* Failed: Red

