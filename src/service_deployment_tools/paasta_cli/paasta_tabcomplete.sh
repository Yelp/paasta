#!/bin/bash
# This magic eval enables tab-completion for the "paasta" command
# http://argcomplete.readthedocs.org/en/latest/index.html#synopsis
# This comes from the service-deployment-tools system package
eval "$(/usr/share/python/service-deployment-tools/bin/register-python-argcomplete paasta)"
