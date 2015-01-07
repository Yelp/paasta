#!/usr/bin/env python
from argcomplete.completers import ChoicesCompleter

from paasta_tools.paasta_cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import list_instances
from paasta_tools.marathon_tools import list_clusters


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'emergency-start',
        description="Starts a stopped PaaSTA service",
        help="Starts a PaaSTA service back up by asking Marathon to have a normal instance count.")
    status_parser.add_argument(
        '-s', '--service',
        help='Service that you want to start. Like example_service.'
    ).completer = ChoicesCompleter(list_services())
    status_parser.add_argument(
        '-i', '--instance',
        help='Instance of the service that you want to start. Like "main" or "canary".'
    ).completer = ChoicesCompleter(list_instances())
    status_parser.add_argument(
        '-c', '--cluster',
        help='The PaaSTA cluster that has the service you want to start. Like norcal-prod'
    ).completer = ChoicesCompleter(list_clusters())
    status_parser.set_defaults(command=paasta_emergency_start)


def paasta_emergency_start(args):
    """Performs an emergency start on a given service.instance on a given cluster"""
    print "Performing an emergency start on %s.%s..." % (args.service, args.instance)
    execute_paasta_serviceinit_on_remote_master('start', args.cluster, args.service, args.instance)
    print "Warning: this tool just asks Marathon to resume normal operation"
    print "and run the 'normal' number of instances of this %s.%s" % (args.service, args.instance)
    print "It is not magic and cannot actually get a service to start if it"
    print "couldn't run before."
    print ""
    print "Run this to see the status:"
    print "paasta status --service %s --clusters %s" % (args.service, args.instance, args.cluster)
