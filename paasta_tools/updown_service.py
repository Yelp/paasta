# Utility to change local SmartStack service state
import argparse
import csv
import os
import socket
import subprocess
import sys
import time
import urllib2

import requests
from requests.exceptions import RequestException
from service_configuration_lib import read_service_configuration

from paasta_tools.marathon_tools import load_service_namespace_config


# Maximum amount of time to run before returning
DEFAULT_TIMEOUT_S = 300

# Even though a service has entered the expected state in our local HAProxy
# instance, there may still be a short delay before all remote HAProxy instances
# also pick up this change.  So we add an additional delay before returning.
# This also allows service instances to finish serving any existing requests
# before we shut them down.
DEFAULT_WAIT_TIME_S = 5

HAPROXY_STATUS_URL = 'http://169.254.255.254:3212/;csv'
HAPROXY_QUERY_TIMEOUT_S = 1
HAPROXY_POLL_INTERVAL_S = 1


def service_name(service):
    if not len(service.split('.')) == 2:
        msg = "Namespace missing from service name"
        raise argparse.ArgumentTypeError(msg)
    return service


def get_args():
    description = "Control SmartStack service state in load balancers"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-t", "--timeout", type=int,
                        help="Maximum time to wait for <state> \
                        (default: updown_timeout_s if set, otherwise {0})".format(DEFAULT_TIMEOUT_S))
    parser.add_argument("-w", "--wait-time", default=DEFAULT_WAIT_TIME_S, type=int,
                        help="Additional number of seconds to wait for convergence (default: %(default)s)")
    parser.add_argument("-x", "--wait-only", action="store_true",
                        help="Wait for the specified state without reconfiguring hacheck")
    parser.add_argument("service", type=service_name,
                        help="Service name, including namespace e.g. 'service_one.main'")
    parser.add_argument("state", choices=['up', 'down'], help="desired state")
    args = parser.parse_args()
    return args


def reconfigure_hacheck(service, state):
    if state == 'down':
        hacheck_command = '/usr/bin/hadown'
    else:
        hacheck_command = '/usr/bin/haup'

    try:
        subprocess.check_call([hacheck_command, service])
    except:
        print >> sys.stderr, "Error running %s" % hacheck_command


def get_my_ip_address():
    return socket.gethostbyname(socket.getfqdn())


def check_haproxy_state(service, expected_state):
    """If the expected_state is 'up', then return 'true' iff the local service
    instance is 'up' in HAProxy.

    If the expected_state is 'down', then return 'true' iff the local service
    instance is NOT 'up' in HAProxy (could be 'down', 'maint' or missing).

    Note that this requires synapse to be running on localhost.
    """

    try:
        fd = urllib2.urlopen(HAPROXY_STATUS_URL, timeout=HAPROXY_QUERY_TIMEOUT_S)
    except:
        # Allow for transient errors when querying HAProxy
        return False

    host = '%s:' % get_my_ip_address()
    reader = csv.DictReader(fd, delimiter=',')

    entries = list(reader)
    entries = [entry for entry in entries if service == entry['# pxname']]

    if len(entries) == 0:
        msg = 'No backends present in Smartstack, have you added any?'
        print >>sys.stderr, msg
        sys.exit(1)

    entries = [entry for entry in entries if host in entry['svname']]

    if len(entries) == 0:
        # We did not find our host
        return expected_state == 'down'

    # We found our service/host.  Let's check whether it has the correct state
    actual_state = entries[0]['status'].lower()
    expected_up = expected_state == 'up'
    actual_up = actual_state.startswith('up')

    return expected_up == actual_up


def check_local_healthcheck(service_name):
    """Makes a local HTTP healthcheck call to the service and returns True if
    it gets a 2XX response, else returns False.

    :param service_name: a string like 'service_one.main'
    :return: Whether healthcheck call was successful for a http service.
    Returns false for a tcp service.
    :rtype: boolean
    """
    srv_name, namespace = service_name.split('.')
    srv_config = read_service_configuration(srv_name)
    smartstack_config = srv_config.get('smartstack', {})
    namespace_config = smartstack_config.get(namespace, {})

    healthcheck_uri = namespace_config.get('healthcheck_uri', '/status')
    healthcheck_port = namespace_config.get('healthcheck_port',
                                            srv_config.get('port'))
    healthcheck_mode = namespace_config.get('mode', 'http')

    # TODO: Add support for TCP healthcheck using hacheck - Ref. RB: 109478
    if healthcheck_mode == 'http' and healthcheck_port:
        try:
            url = "http://{host}:{port}{uri}".format(
                host="127.0.0.1", port=healthcheck_port, uri=healthcheck_uri)
            requests.get(url).raise_for_status()
            return True
        except RequestException as e:
            print >>sys.stderr, "Calling {0}, got - {1}".format(url, str(e))

    return False


def wait_for_haproxy_state(service, expected_state, timeout, wait_time):
    """Wait for the specified service to enter the given state in HAProxy."""

    # This isn't precise, but it's easy to test :)
    iterations = timeout / HAPROXY_POLL_INTERVAL_S
    n = 0

    for n in xrange(iterations):
        # If we are asking to up a service on a machine that has the "all"
        # service downed, return a success providing that the service itself
        # is healthy
        if expected_state == 'up':
            try:
                with open(os.devnull, 'w') as devnull:
                    subprocess.check_call(
                        ['/usr/bin/hastatus', 'all'], stdout=devnull
                    )
            except:
                if check_local_healthcheck(service):
                    return 0

        if check_haproxy_state(service, expected_state):
            print '{0}Service entered state \'{1}\''.format(
                '\n' if n > 0 else '', expected_state)
            print 'Sleeping for an additional {0}s'.format(wait_time)
            time.sleep(wait_time)
            return 0

        sys.stdout.write('.')
        sys.stdout.flush()

        time.sleep(HAPROXY_POLL_INTERVAL_S)
    else:
        print '{0}Service failed to enter state \'{1}\''.format(
            '\n' if n > 0 else '', expected_state)
        if expected_state == 'up':
            print '*** Please manually check your service\'s healthcheck endpoint. ***'
            print '*** If your service is healthy, then please talk to #paasta. ***'
        return 1


def _should_manage_service(service_name):
    srv_name, namespace = service_name.split('.')
    marathon_config = load_service_namespace_config(srv_name, namespace)
    classic_config = read_service_configuration(srv_name)

    should_manage = marathon_config.get('proxy_port') is not None
    blacklisted = classic_config.get('no_updown_service')

    return (should_manage and not blacklisted)


def _get_timeout_s(service_name, timeout):
    if timeout is not None:
        return timeout

    srv_name, namespace = service_name.split('.')
    namespace_configuration = load_service_namespace_config(srv_name, namespace)
    timeout_s = namespace_configuration.get('updown_timeout_s', DEFAULT_TIMEOUT_S)
    return timeout_s


def main():
    args = get_args()
    should_check = _should_manage_service(args.service)
    timeout_s = _get_timeout_s(args.service, args.timeout)

    if not should_check:
        print '{0} is not available in synapse, doing nothing'.format(
            args.service
        )
        sys.exit(0)

    if not args.wait_only:
        reconfigure_hacheck(args.service, args.state)

    result = wait_for_haproxy_state(
        args.service, args.state, timeout_s, args.wait_time)
    sys.exit(result)


if __name__ == '__main__':
    main()
