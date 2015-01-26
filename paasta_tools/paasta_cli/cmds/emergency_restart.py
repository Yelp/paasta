#!/usr/bin/env python
from argcomplete.completers import ChoicesCompleter

from paasta_tools.paasta_cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import list_instances
from paasta_tools.marathon_tools import list_clusters


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'emergency-restart',
        description="Restarts a PaaSTA service in an emergency",
        help="Restarts a PaaSTA service by asking Marathon to suspend/resume.")
    status_parser.add_argument(
        '-s', '--service',
        help='Service that you want to restart. Like example_service.',
        required=True,
    ).completer = ChoicesCompleter(list_services())
    status_parser.add_argument(
        '-i', '--instance',
        help='Instance of the service that you want to restart. Like "main" or "canary".',
        required=True,
    ).completer = ChoicesCompleter(list_instances())
    status_parser.add_argument(
        '-c', '--cluster',
        help='The PaaSTA cluster that has the service you want to restart. Like norcal-prod',
        required=True,
    ).completer = ChoicesCompleter(list_clusters())
    status_parser.set_defaults(command=paasta_emergency_restart)


def paasta_emergency_restart(args):
    """Performs an emergency restart on a given service.instance on a given cluster"""
    print "Performing an emergency restart on %s.%s..." % (args.service, args.instance)
    execute_paasta_serviceinit_on_remote_master('stop', args.cluster, args.service, args.instance)
    print "Warning: this tool just asks Marathon suspend, and then resume normal operation"
    print "It does not (currently) do a fancy bounce. This tool is only designed to be used"
    print "in an emergency."
    print ""
    print "Run this to see the status:"
    print "paasta status --service %s --clusters %s" % (args.service, args.cluster)
