#!/usr/bin/env python
from paasta_tools.paasta_cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import lazy_choices_completer
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import list_instances
from paasta_tools.utils import list_clusters


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'emergency-start',
        description="Starts a stopped PaaSTA service",
        help="Starts a PaaSTA service back up by asking Marathon to have a normal instance count.")
    status_parser.add_argument(
        '-s', '--service',
        help='Service that you want to start. Like example_service.',
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-i', '--instance',
        help='Instance of the service that you want to start. Like "main" or "canary".',
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    status_parser.add_argument(
        '-c', '--cluster',
        help='The PaaSTA cluster that has the service you want to start. Like norcal-prod',
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.set_defaults(command=paasta_emergency_start)


def paasta_emergency_start(args):
    """Performs an emergency start on a given service.instance on a given cluster"""
    service = figure_out_service_name(args)
    print "Performing an emergency start on %s.%s..." % (service, args.instance)
    execute_paasta_serviceinit_on_remote_master('start', args.cluster, service, args.instance)
    print "Warning: this tool just asks Marathon to resume normal operation"
    print "and run the 'normal' number of instances of this %s.%s" % (service, args.instance)
    print "It is not magic and cannot actually get a service to start if it"
    print "couldn't run before."
    print ""
    print "Run this to see the status:"
    print "paasta status --service %s --clusters %s" % (service, args.cluster)
