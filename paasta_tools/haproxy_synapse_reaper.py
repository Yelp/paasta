#!/usr/bin/env python
"""When haproxy is soft restarted, the previous 'main' haproxy instance sticks
around as an 'alumnus' until all connections have drained:

   main --[soft restart]--> alumnus --[connections drained]--> <dead>

If the alumus is handling long-lived connections (e.g. scribe), it could take
a long time to exit.  This script bounds the length of time that a haproxy
instance can spend in the alumnus state by killing such processes after a
specified period of time.

How do we know how long a process has spent in the alumnus state?  The first
time we see a non-main haproxy instance, we create an entry for it in the
specified state directory.  Once an entry reaches the specified 'reap age',
the associated haproxy instance is killed.

See SRV-1404 for more background info.
"""
import argparse
import errno
import logging
import operator
import os
import time

import psutil


DEFAULT_USERNAME = 'nobody'

DEFAULT_STATE_DIR = '/var/run/synapse_alumni'

DEFAULT_REAP_AGE_S = 60 * 60

DEFAULT_MAX_PROCS = 10

HAPROXY_SYNAPSE_PIDFILE = '/var/run/synapse/haproxy.pid'

LOG_FORMAT = '%(levelname)s %(message)s'

log = logging.getLogger()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--state-dir', default=DEFAULT_STATE_DIR,
                        help='State directory (default: %(default)s).')
    parser.add_argument('-r', '--reap-age', type=int, default=DEFAULT_REAP_AGE_S,
                        help='Reap age (default: %(default)s).')
    parser.add_argument('-p', '--max-procs', type=int, default=DEFAULT_MAX_PROCS,
                        help='Maximum processes (default: %(default)s).')
    parser.add_argument('-u', '--username', default=DEFAULT_USERNAME,
                        help='Username that haproxy-synapse runs under (default: %(default)s).')
    return parser.parse_args()


def get_main_pid():
    with open(HAPROXY_SYNAPSE_PIDFILE) as fh:
        return int(fh.readline().strip())


def get_alumni(username):
    main_pid = get_main_pid()

    for proc in psutil.process_iter():
        if proc.name() != 'haproxy-synapse':
            continue

        if proc.username() != username:
            continue

        if proc.pid == main_pid:
            continue

        yield proc


def kill_alumni(alumni, state_dir, reap_age, max_procs):
    reap_count = 0

    # Sort by oldest process creation time (= youngest) first
    alumni = sorted(
        alumni,
        key=operator.methodcaller('create_time'),
        reverse=True)

    for index, proc in enumerate(alumni):
        pidfile = os.path.join(state_dir, str(proc.pid))

        # Create pidfile if necessary
        if not os.path.exists(pidfile):
            log.info('Creating pidfile for new alumnus: %d', proc.pid)
            open(pidfile, 'w').close()

        age = time.time() - os.path.getctime(pidfile)
        if age < reap_age and index < max_procs:
            continue

        # Teletubby bye bye
        log.info('Reaping process %d with age %ds and index %d' %
                 (proc.pid, age, index))
        try:
            proc.kill()
            reap_count += 1
        except psutil.NoSuchProcess:
            log.warn('Process %d has disappeared' % proc.pid)

    return reap_count


def remove_stale_alumni_pidfiles(alumni, state_dir):
    alumni_pids = [proc.pid for proc in alumni]

    for pidfile in os.listdir(state_dir):
        try:
            pid = int(pidfile)
        except ValueError:
            log.warn('Ignoring invalid filename: %s' % pidfile)
            continue

        if pid in alumni_pids:
            continue

        log.info('Removing stale pidfile for %d', pid)
        os.remove(os.path.join(state_dir, pidfile))


def ensure_path_exists(path):
    try:
        os.mkdir(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    args = parse_args()
    ensure_path_exists(args.state_dir)
    alumni = list(get_alumni(args.username))
    reap_count = kill_alumni(
        alumni, args.state_dir, args.reap_age, args.max_procs)
    remove_stale_alumni_pidfiles(alumni, args.state_dir)

    log.info('Reaped %d processes' % reap_count)


if __name__ == '__main__':
    main()
