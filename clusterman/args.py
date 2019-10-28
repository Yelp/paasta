import argparse
import os
import sys

import colorlog

from clusterman import __version__

logger = colorlog.getLogger(__name__)


def subparser(command, help, entrypoint):  # pragma: no cover
    """ Function decorator to simplify adding arguments to subcommands

    :param command: name of the subcommand to add
    :param help: help string for the subcommand
    :param entrypoint: the 'main' function for the subcommand to execute
    """
    def decorator(add_args):
        def wrapper(subparser):
            subparser = subparser.add_parser(command, formatter_class=help_formatter, add_help=False)
            required_named_args = subparser.add_argument_group('required arguments')
            optional_named_args = subparser.add_argument_group('optional arguments')
            add_args(subparser, required_named_args, optional_named_args)
            optional_named_args.add_argument('-h', '--help', action='help', help='show this message and exit')
            subparser.set_defaults(entrypoint=entrypoint)
        return wrapper
    return decorator


def deprecate_argument(depr_arg, alt):
    class DeprecationAction(argparse.Action):
        # Like _StoreAction
        STATIC_COMPLETION_NARGS_OPTIONAL = False
        STATIC_COMPLETION_NARGS = 1

        def __call__(self, parser, namespace, values, option_string=None):
            if option_string == depr_arg:
                logger.warn(f' {depr_arg} is deprecated and will be removed in the future; please use {alt} instead')
            setattr(namespace, self.dest, values)
    return DeprecationAction


def add_branch_or_tag_arg(parser):  # pragma: no cover
    parser.add_argument(
        '-S', '--signals-branch-or-tag',
        default=None,
        help='Branch or tag to use for the clusterman_signals repository'
    )


def add_start_end_args(parser, start_help, end_help):  # pragma: no cover
    """ Add --start-time and --end-time args to a parser

    :param start_help: help string for --start-time
    :param end_help: help string for --end-time
    """
    parser.add_argument(
        '--start-time',
        metavar='timestamp',
        default='-1h',
        help=f'{start_help} (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )
    parser.add_argument(
        '--end-time',
        metavar='timestamp',
        default='now',
        help=f'{end_help} (try "yesterday", "-5m", "3 months ago"; use quotes)',
    )


def add_region_arg(parser, required=False):  # pragma: no cover
    """ Add an --aws-region argument to a parser """
    parser.add_argument(
        '--aws-region',
        required=required,
        help='AWS region to operate in',
    )


def add_cluster_arg(parser, required=False):  # pragma: no cover
    """ Add --cluster argument to a parser """
    parser.add_argument(
        '--cluster',
        default=os.environ.get('CMAN_CLUSTER', None),
        help='Name of Mesos cluster to operate on',
    )


def add_pool_arg(parser):  # pragma: no cover
    """ Add --pool argument to a parser """
    parser.add_argument(
        '--pool', '--role',
        dest='pool',
        default=os.environ.get('CMAN_POOL', 'default'),
        action=deprecate_argument(depr_arg='--role', alt='--pool'),
        help='Identifier for a pool of machines to operate on',
    )


def add_scheduler_arg(parser):  # pragma: no cover
    """ Add --scheduler argument to a parser """
    parser.add_argument(
        '--scheduler',
        default=os.environ.get('CMAN_SCHEDULER', 'mesos'),
        choices=['mesos', 'kubernetes'],
        help='Type of scheduler used to manage the chosen pool',
    )


def add_env_config_path_arg(parser):  # pragma: no cover
    """ Add a --env-config-path argument to a parser """
    parser.add_argument(
        '--env-config-path',
        default='/nail/srv/configs/clusterman.yaml',
        help='Path to clusterman configuration file',
    )


def add_cluster_config_directory_arg(parser):  # pragma: no cover
    parser.add_argument(
        '--cluster-config-directory',
        metavar='directory',
        help='specify role configuration directory for simulation',
    )


def add_disable_sensu_arg(parser):  # pragma: no cover
    """ Add a --disable-sensu argument to a parser """
    parser.add_argument(
        '--disable-sensu',
        action='store_true',
        help='Disable sensu checkins',
    )


def help_formatter(prog):  # pragma: no cover
    """Formatter for the argument parser help strings"""
    return argparse.ArgumentDefaultsHelpFormatter(prog, max_help_position=35, width=100)


def _get_validated_args(parser):
    args = parser.parse_args()

    if args.subcommand is None:
        logger.error('missing subcommand')
        parser.print_help()
        sys.exit(1)

    if hasattr(args, 'cluster') and not args.cluster:
        logger.critical('cluster name must be specified')
        sys.exit(1)

    # Every subcommand must specify an entry point, accessed here by args.entrypoint
    # (protip) use the subparser decorator to set this up for you
    if not hasattr(args, 'entrypoint'):
        logger.critical(f'error: missing entrypoint for {args.subcommand}')
        sys.exit(1)

    return args


def get_parser(description=''):  # pragma: no cover
    from clusterman.cli.generate_data import add_generate_data_parser
    from clusterman.cli.info import add_mesos_list_clusters_parser
    from clusterman.cli.info import add_mesos_list_pools_parser
    from clusterman.cli.manage import add_mesos_manager_parser
    from clusterman.cli.status import add_mesos_status_parser
    from clusterman.cli.simulate import add_simulate_parser
    from clusterman.cli.toggle import add_cluster_disable_parser
    from clusterman.cli.toggle import add_cluster_enable_parser
    from clusterman.draining.queue import add_queue_parser

    root_parser = argparse.ArgumentParser(prog='clusterman', description=description, formatter_class=help_formatter)
    add_env_config_path_arg(root_parser)
    root_parser.add_argument(
        '--log-level',
        default='warning',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
    )
    root_parser.add_argument(
        '-v', '--version',
        action='version',
        version='clusterman ' + __version__
    )

    subparser = root_parser.add_subparsers(help='accepted commands')
    subparser.dest = 'subcommand'

    add_cluster_disable_parser(subparser)
    add_cluster_enable_parser(subparser)
    add_generate_data_parser(subparser)
    add_mesos_list_clusters_parser(subparser)
    add_mesos_list_pools_parser(subparser)
    add_mesos_status_parser(subparser)
    add_mesos_manager_parser(subparser)
    add_simulate_parser(subparser)
    add_queue_parser(subparser)

    return root_parser


def parse_args(description):  # pragma: no cover
    """Parse arguments for the CLI too and any subcommands

    :param description: a string descripting the tool
    :returns: a namedtuple of the parsed command-line options with their values
    """
    root_parser = get_parser(description)

    args = _get_validated_args(root_parser)
    return args
