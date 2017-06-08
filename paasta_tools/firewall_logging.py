# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import os
import signal
import socket
import sys

import syslogmp
from six.moves import socketserver

from paasta_tools.firewall import services_running_here
from paasta_tools.utils import _log
from paasta_tools.utils import configure_log
from paasta_tools.utils import load_system_paasta_config

DEFAULT_NUM_WORKERS = 5

log = logging.getLogger(__name__)


class SyslogUDPHandler(socketserver.BaseRequestHandler):
    def setup(self):
        configure_log()
        self.cluster = load_system_paasta_config().get_cluster()

    def handle(self):
        data, socket = self.request
        syslog_to_paasta_log(data, self.cluster)


def syslog_to_paasta_log(data, cluster):
    iptables_log = parse_syslog(data)
    if iptables_log is None:
        return

    service, instance = lookup_service_instance_by_ip(iptables_log['SRC'])
    if service is None or instance is None:
        return

    # prepend hostname
    log_line = iptables_log['hostname'] + ': ' + iptables_log['message']

    _log(
        service=service,
        component='security',
        level='debug',
        cluster=cluster,
        instance=instance,
        line=log_line,
    )


def parse_syslog(data):
    parsed_data = syslogmp.parse(data)
    try:
        full_message = parsed_data.message.decode()
    except UnicodeDecodeError:
        return None

    if not full_message.startswith('kernel: ['):
        # Not a kernel message
        return None

    close_bracket = full_message.find(']')
    if close_bracket == -1:
        return None

    iptables_message = full_message[close_bracket + 1:].strip()
    parts = iptables_message.split(' ')

    # parts[0] is the log-prefix
    # parts[1..] is either KEY=VALUE or just KEY
    if not parts[1].startswith('IN='):
        # not an iptables message
        return None

    fields = {k: v for k, _, v in (field.partition('=') for field in parts[1:])}

    fields['hostname'] = parsed_data.hostname
    fields['message'] = iptables_message
    return fields


def lookup_service_instance_by_ip(ip_lookup):
    for service, instance, mac, ip in services_running_here():
        if ip == ip_lookup:
            return (service, instance)
    log.info('Unable to find container for ip {}'.format(ip_lookup))
    return (None, None)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description='Adapts iptables syslog messages into scribe')
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    parser.add_argument('-l', '--listen-host', help='Default %(default)s', default='127.0.0.1')
    parser.add_argument('-p', '--listen-port', type=int, help='Default %(default)s', default=1516)
    parser.add_argument('-w', '--num-workers', type=int, help='Default %(default)s', default=DEFAULT_NUM_WORKERS)
    args = parser.parse_args(argv)
    return args


def setup_logging(verbose):
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)


class MultiUDPServer(socketserver.UDPServer):
    # UDPServer with SO_REUSEPORT enabled so that incoming packets are
    # load-balanced across listeners
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # UDPServer is old-style class so can't use super
        socketserver.UDPServer.server_bind(self)


def run_server(listen_host, listen_port):
    server = MultiUDPServer((listen_host, listen_port), SyslogUDPHandler)
    server.serve_forever()


def main(argv=None):
    args = parse_args(argv)
    setup_logging(args.verbose)

    assert args.num_workers > 0

    # start n-1 separate processes, then run_server() on this one
    num_forks = args.num_workers - 1
    for x in range(num_forks):
        if os.fork() == 0:
            run_server(args.listen_host, args.listen_port)

    # propagate SIGTERM to all my children then exit
    signal.signal(signal.SIGTERM, lambda signum, _: os.killpg(os.getpid(), signum) or sys.exit(1))

    run_server(args.listen_host, args.listen_port)
