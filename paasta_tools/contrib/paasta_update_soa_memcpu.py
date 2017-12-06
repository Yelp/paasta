#!/usr/bin/env python3
import argparse
import contextlib
import os
import subprocess
import sys
import tempfile

import ruamel.yaml as yaml


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument(
        '-s', '--service',
        help="Service to edit. Like 'example_service'.",
        dest="service",
        required=True,
    )
    parser.add_argument(
        '-i', '--instance',
        help="Instance of the service to edit. Like 'main' or 'canary'.",
        dest="instance",
        required=True,
    )
    parser.add_argument(
        '-c', '--cluster',
        help="The PaaSTA cluster that has the service instance to edit. Like 'norcal-prod'.",
        dest="cluster",
        required=True,
    )
    parser.add_argument(
        '-m', '--mem',
        default='',
        dest="mem",
        help='New value for mem of service',
    )
    parser.add_argument(
        '-cp', '--cpu',
        default='',
        dest="cpu",
        help='New value for cpus of service',
    )
    parser.add_argument(
        '-t', '--ticket',
        dest="ticket",
        help='JIRA ticket tracking changes',
    )
    return parser.parse_args(argv)


def tempdir():
    return tempfile.TemporaryDirectory(
        prefix='repo',
        dir='/nail/tmp',
    )


@contextlib.contextmanager
def cwd(path):
    pwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(pwd)


@contextlib.contextmanager
def in_tempdir():
    with tempdir() as tmp:
        with cwd(tmp):
            yield


def clone(branch_name):
    print('cloning')
    remote = 'git@sysgit.yelpcorp.com:yelpsoa-configs'
    subprocess.check_call(('git', 'clone', remote, '.'))
    subprocess.check_call(('git', 'checkout', 'origin/master', '-b', branch_name))


def commit(filename, memcpu):
    message = 'Updating {} for under/overprovisioned {}'.format(filename, memcpu)
    subprocess.check_call(('git', 'add', filename))
    subprocess.check_call(('git', 'commit', '-m', message))


def get_reviewers(filename):
    authors = subprocess.check_output((
        'git', 'log', '--format=%ae', '--', filename,
    )).decode('UTF-8').splitlines()

    authors = list(set(authors))
    authors = [x.split('@')[0] for x in authors]
    return authors[:3]


def review(filename):
    reviewers = ' '.join(get_reviewers(filename))
    description = (
        'This change was made automatically by the perf_update_soaconfigs script. If not reviewed'
        ' in a week, it will be merged by the perf or paasta teams. For more context on why these'
        ' changes are being made see: PERF-2439'
    )
    subprocess.check_call((
        'review-branch',
        '--force',
        '--summary=automatically updating {} for under/overprovisioned mem/cpu'.format(filename),
        '--description="{}"'.format(description),
        '--reviewers', reviewers,
        '--target-groups', 'operations',
    ))


def edit_soa_configs(filename, instance, mem, cpu):
    with open(filename, 'r') as fi:
        yams = fi.read()
        yams = yams.replace('cpus: .', 'cpus: 0.')
        data = yaml.round_trip_load(yams, preserve_quotes=True)

    instdict = data[instance]
    if mem:
        instdict['mem'] = mem
    else:
        instdict['cpus'] = cpu
    out = yaml.round_trip_dump(data, width=10000)

    with open(filename, 'w') as fi:
        fi.write(out)


def main(argv=None):
    args = parse_args(argv)
    mem = args.mem
    cpu = args.cpu
    instance = args.instance
    service = args.service
    cluster = args.cluster
    ticket = args.ticket

    filename = '{}/marathon-{}.yaml'.format(service, cluster)
    memcpu = ''
    if mem:
        memcpu = 'mem'
        mem = int(mem)
    elif cpu:
        memcpu = 'cpu'
        cpu = int(cpu)
    else:
        print('please specify either mem or cpu')
        sys.exit(2)

    with in_tempdir():
        clone(ticket)
        edit_soa_configs(filename, instance, mem, cpu)
        commit(filename, memcpu)
        review(filename)


if __name__ == '__main__':
    main()
