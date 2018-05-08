"""
Usage: ./setup_tron_namespace.py <service>

Deploy a namespace to the local Tron master from a service configuration file.
Reads from the soa_dir /nail/etc/services by default.

TODO
"""
import argparse
import logging
import sys

from paasta_tools import tron_tools
from paasta_tools.tron.client import TronRequestError
from paasta_tools.tron_tools import InvalidTronConfig
from paasta_tools.tron_tools import TronNotConfigured
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaNotConfiguredError


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Update the Tron namespace configuration for a service.')
    parser.add_argument(
        'service',
        help='The service to update.',
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=tron_tools.DEFAULT_SOA_DIR,
        help="Use a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        default=False,
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    try:
        new_config = tron_tools.create_complete_config(
            service=args.service,
            soa_dir=args.soa_dir,
        )
        client = tron_tools.get_tron_client()
        client.update_namespace(args.service, new_config)
    except (
        InvalidTronConfig,
        NoConfigurationForServiceError,
        TronNotConfigured,
        PaastaNotConfiguredError,
        TronRequestError,
    ) as e:
        log.error('Update for {namespace} failed: {error}'.format(namespace=args.service, error=str(e)))
        sys.exit(1)


if __name__ == "__main__":
    main()
