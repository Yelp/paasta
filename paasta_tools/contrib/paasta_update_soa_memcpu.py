#!/usr/bin/env python3
import argparse
import contextlib
import json
import os
import subprocess
import tempfile

import requests
import ruamel.yaml as yaml
from jira.client import JIRA


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
    parser.add_argument(
        '-j', '--jira_creds',
        help="Creds for JIRA API, user:pass",
        dest="jira_creds",
        required=True,
    )
    parser.add_argument(
        '-f', '--file-splunk',
        help='Splunk csv from which to pull data. Defaults to paasta_overprovision_alerts_fired.csv',
        dest="file_splunk",
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


def get_perf_data(creds, filename):
    url = 'https://splunk-api.yelpcorp.com/servicesNS/nobody/yelp_performance/search/jobs/export'
    search = (
        '| inputlookup {} |'
        ' eval _time = search_time | where _time > relative_time(now(),\"-7d\")'
    ).format(filename)
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
    for d in resp_text:
        criteria = d['result']['criteria'].split()
        serv = {}
        serv['service'] = criteria[0]
        serv['cluster'] = criteria[1]
        serv['instance'] = criteria[2]
        serv['cpus'] = d['result']['suggested_cpus']
        serv['owner'] = d['result']['service_owner']
        serv['money'] = d['result']['estimated_monthly_savings']
        serv['date'] = d['result']['_time'].split(" ")[0]
        serv['old_cpus'] = d['result']['current_cpus']
        serv['project'] = d['result']['project']
        services_to_update.append(serv)

    return services_to_update


def clone(branch_name):
    print('cloning')
    remote = 'git@sysgit.yelpcorp.com:yelpsoa-configs'
    subprocess.check_call(('git', 'clone', remote, '.'))
    subprocess.check_call(('git', 'checkout', '-b', branch_name))


def commit(filename, over):
    message = 'Updating {} for {}provisioned cpu'.format(filename, over)
    subprocess.check_call(('git', 'add', filename))
    subprocess.check_call(('git', 'commit', '-n', '-m', message))


def get_reviewers(filename):
    authors = subprocess.check_output((
        'git', 'log', '--format=%ae', '--', filename,
    )).decode('UTF-8').splitlines()

    authors = list(set(authors))
    authors = [x.split('@')[0] for x in authors]
    return authors[:3]


def review(filename, description, over):
    reviewers = ' '.join(get_reviewers(filename))
    subprocess.check_call((
        'git',
        'push',
        '--force',
        'origin',
        'HEAD',
    ))
    subprocess.check_call((
        'review-branch',
        '--summary=automatically updating {} for {}provisioned cpu'.format(filename, over),
        '--description="{}"'.format(description),
        '--reviewers', reviewers,
        '--server', 'https://reviewboard.yelpcorp.com',
        '--target-groups', 'operations perf',
    ))


def edit_soa_configs(filename, instance, cpu):
    with open(filename, 'r') as fi:
        yams = fi.read()
        yams = yams.replace('cpus: .', 'cpus: 0.')
        data = yaml.round_trip_load(yams, preserve_quotes=True)

    instdict = data[instance]
    instdict['cpus'] = cpu
    out = yaml.round_trip_dump(data, width=10000)

    with open(filename, 'w') as fi:
        fi.write(out)


def create_jira_ticket(serv, creds, description):
    creds = creds.split(':')
    options = {'server': 'https://jira.yelpcorp.com'}
    jira_cli = JIRA(options=options, basic_auth=(creds[0], creds[1]))
    jira_ticket = {}
    # Sometimes the project name doesn't match a JIRA project
    try:
        jira_ticket = {
            'project': {'key': serv['project']},
            'issuetype': {'name': 'Improvement'},
            'description': description,
            'labels': ['perf-watching', 'paasta-rightsizer'],
            'summary': "{s} {c} {i} may be {o}provisioned".format(
                s=serv['service'],
                i=serv['instance'],
                c=serv['cluster'],
                o=serv['over'],
            ),
        }
        tick = jira_cli.create_issue(fields=jira_ticket)
    except Exception:
        jira_ticket = {
            'project': {'key': 'PERF'},
            'issuetype': {'name': 'Improvement'},
            'description': description,
            'summary': "{s} {c} {i} may be {o}provisioned".format(
                s=serv['service'],
                i=serv['instance'],
                c=serv['cluster'],
                o=serv['over'],
            ),
            'labels': ['perf-watching', 'paasta-rightsizer'],
        }
        tick = jira_cli.create_issue(fields=jira_ticket)
    return tick.key


def main(argv=None):
    args = parse_args(argv)
    services_to_update = get_perf_data(args.splunk_creds, args.file_splunk)

    for serv in services_to_update:
        filename = '{}/{}.yaml'.format(serv['service'], serv['cluster'])
        cpus = float(serv['cpus'])
        over = 'over'
        if cpus > float(serv['old_cpus']):
            over = 'under'

        serv['over'] = over
        ticket_desc = (
            "We suspect that {s} {c} {i} may be {o}-provisioned"
            " as of {d}.\n- Dashboard: y/{o}provisioned\n- Service"
            " owner: {n}\n- Estimated monthly excess cost: {m}"
            "\n- Runbook: y/rb-provisioning-alert"
            "\n- Alert owner: team-perf@yelp.com"
        ).format(
            s=serv['service'],
            c=serv['cluster'],
            i=serv['instance'],
            o=over,
            d=serv['date'],
            n=serv['owner'],
            m=serv['money'],
        )
        ticket = create_jira_ticket(serv, args.jira_creds, ticket_desc)
        with in_tempdir():
            clone(ticket)
            edit_soa_configs(filename, serv['instance'], cpus)
            commit(filename, over)
            review(filename, ticket_desc, over)


if __name__ == '__main__':
    main()
