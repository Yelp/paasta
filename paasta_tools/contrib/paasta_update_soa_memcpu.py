#!/usr/bin/env python3
import argparse
import contextlib
import json
import os
import subprocess
import tempfile
import time

import requests
import ruamel.yaml as yaml


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument(
        '-s', '--splunk_creds',
        help="Creds for Splunk API, user:pass",
        dest="splunk_creds",
        required=True,
    )
    # parser.add_argument(
    #     '-j', '--jira_creds',
    #     help="Creds for JIRA API, user:pass",
    #     dest="jira_creds",
    #     required=True,
    # )
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


def get_perf_data(creds):
    url = 'https://splunk-api.yelpcorp.com/servicesNS/nobody/yelp_performance/search/jobs/export'
    search = (
        '| inputlookup paasta_overprovision_alerts_fired_new.csv |'
        ' eval _time = search_time | where _time > relative_time(now(),\"-7d\")'
    )
    data = {
        'output_mode': 'json',
        'search': search,
    }
    creds = creds.split(':')
    resp = requests.post(url, data=data, auth=(creds[0], creds[1]))
    resp_text = resp.text.split('\n')
    resp_text = [x for x in resp_text if x]
    resp_text = [json.loads(x) for x in resp_text]

    services_to_update = []
    print(services_to_update)
    for d in resp_text:
        criteria = d['result']['criteria'].split()
        serv = {}
        serv['service'] = criteria[0]
        serv['cluster'] = criteria[1]
        serv['instance'] = criteria[2]
        serv['cpus'] = d['result']['suggested_cpus']
        serv['memcpu'] = 'cpu'
        serv['project'] = d['result']['project']
        services_to_update.append(serv)

    return services_to_update


def clone(branch_name):
    print('cloning')
    remote = 'git@sysgit.yelpcorp.com:yelpsoa-configs'
    subprocess.check_call(('git', 'clone', remote, '.'))
    subprocess.check_call(('git', 'checkout', '-b', branch_name))


def commit(filename, memcpu):
    message = 'Updating {} for under/overprovisioned {}'.format(filename, memcpu)
    subprocess.check_call(('git', 'add', filename))
    subprocess.check_call(('git', 'commit', '-n', '-m', message))


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
        'git',
        'push',
        '--force',
        'origin',
        'HEAD',
    ))
    subprocess.check_call((
        'review-branch',
        '--summary=automatically updating {} for under/overprovisioned mem/cpu'.format(filename),
        '--description="{}"'.format(description),
        '--reviewers', reviewers,
        '--parent', 'master',
        '--target-groups', 'operations', 'perf',
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


def create_jira_ticket(serv):
    # creds = creds.split(':')
    # options = {'server': 'https://jira.yelpcorp.com'}
    # jira_cli = JIRA(options=options, basic_auth=(creds[0], creds[1])
    # description = ('Perf suspects that {s}, {i}, {c} may be over/underprovisioned.
    # jira_ticket = {
    #     'project': { 'key': serv['project'] },
    #     'issuetype': { 'name': 'Improvement'},
    #     'priority': { 'id': '2' }
    #     'summary': "{s}, {i}, {c}, may be over/underprovisioned".format(s=serv['service'],
    #                                                   i=serv['instance'],
    #                                                   c=serv['cluster'])

    return 'cpu-{}'.format(str(time.time()))


def main(argv=None):
    args = parse_args(argv)
    services_to_update = get_perf_data(args.splunk_creds)

    for serv in services_to_update:
        filename = '{}/{}.yaml'.format(serv['service'], serv['cluster'])
        memcpu = serv['memcpu']
        if serv.get('mem', ''):
            memcpu = 'mem'
            mem = float(serv['mem'])
            cpu = ''
        elif serv.get('cpus', ''):
            memcpu = 'cpu'
            cpu = float(serv['cpus'])
            mem = ''

        ticket = create_jira_ticket(serv)
        with in_tempdir():
            clone(ticket)
            edit_soa_configs(filename, serv['instance'], mem, cpu)
            commit(filename, memcpu)
            review(filename)


if __name__ == '__main__':
    main()
