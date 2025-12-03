#!/usr/bin/env python
# flake8: noqa: E402
""" Delayed imports for paasta_tools.docker_wrapper, meant to improve the performance
of docker_wrapper for execution paths when we do not use add_firewall. It turns out
that the imports needed add a fair amount of overhead to "docker inspect" command
which does not use add_firewall, and Mesos executes this command quite a lot. We can
make it faster by moving all the paasta_tools imports to separate file, i.e. here
"""
from paasta_tools.firewall import DEFAULT_SYNAPSE_SERVICE_DIR
from paasta_tools.firewall import firewall_flock
from paasta_tools.firewall import prepare_new_container
from paasta_tools.mac_address import reserve_unique_mac_address
from paasta_tools.utils import DEFAULT_SOA_DIR
