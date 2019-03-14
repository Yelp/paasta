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
        '-n', '--no-tick',
        help='Do not create a JIRA ticket',
        action='store_true',
        dest='no_tick',
        default=False,
    )
    parser.add_argument(
        '-m', '--manual',
        help='Do not automatically publish the review',
        action='store_true',
        dest='manual_rb',
        default=False,
    )

    parser.add_argument(
        '-f', '--file-splunk',
        help='Splunk csv from which to pull data. Defaults to paasta_overprovision_alerts_fired.csv',
        dest="file_splunk",
        default='paasta_overprovision_alerts_fired.csv',
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
    remote = 'git@sysgit.yelpcorp.com:yelpsoa-configs'
    subprocess.check_call(('git', 'clone', remote, '.'))
    subprocess.check_call(('git', 'checkout', '-b', branch_name))


def commit(filename, serv):
    message = 'Updating {} for {}provisioned cpu from {} to {} cpus'.format(
        filename,
        serv['state'],
        serv['old_cpus'],
        serv['cpus'],
    )
    subprocess.check_call(('git', 'add', filename))
    subprocess.check_call(('git', 'commit', '-n', '-m', message))


def get_reviewers_in_group(group_name):
    """Using rbt's target-groups argument overrides our configured default review groups.
    So we'll expand the group into usernames and pass those users in the group individually.
    """
    rightsizer_reviewers = json.loads(
        subprocess.check_output(
            (
                'rbt',
                'api-get',
                '--server', 'https://reviewboard.yelpcorp.com',
                f'groups/{group_name}/users/',
            ),
        ).decode('UTF-8'),
    )
    return [user.get('username', '') for user in rightsizer_reviewers.get('users', {})]


def get_reviewers(filename):
    recent_authors = set()
    authors = subprocess.check_output((
        'git', 'log', '--format=%ae', '--', filename,
    )).decode('UTF-8').splitlines()

    authors = [x.split('@')[0] for x in authors]
    for author in authors:
        recent_authors.add(author)
        if len(recent_authors) >= 3:
            break
    return recent_authors


def review(filename, description, provisioned_state, manual_rb):
    all_reviewers = get_reviewers(filename).union(get_reviewers_in_group('right-sizer'))
    reviewers_arg = ' '.join(all_reviewers)
    if manual_rb:
        subprocess.check_call((
            'review-branch',
            f'--summary=automatically updating {filename} for {provisioned_state}provisioned cpu',
            f'--description="{description}"',
            '--reviewers', reviewers_arg,
            '--server', 'https://reviewboard.yelpcorp.com',
        ))
    else:
        subprocess.check_call((
            'review-branch',
            f'--summary=automatically updating {filename} for {provisioned_state}provisioned cpu',
            f'--description="{description}"',
            '-p',
            '--reviewers', reviewers_arg,
            '--server', 'https://reviewboard.yelpcorp.com',
        ))


def edit_soa_configs(filename, instance, cpu):
    with open(filename, 'r') as fi:
        yams = fi.read()
        yams = yams.replace('cpus: .', 'cpus: 0.')
        data = yaml.round_trip_load(yams, preserve_quotes=True)

    instdict = data[instance]
    instdict['cpus'] = cpu
    out = yaml.round_trip_dump(data, width=120)

    with open(filename, 'w') as fi:
        fi.write(out)


def create_jira_ticket(serv, creds, description):
    creds = creds.split(':')
    options = {'server': 'https://jira.yelpcorp.com'}
    jira_cli = JIRA(options=options, basic_auth=(creds[0], creds[1]))
    jira_ticket = {}
    # Sometimes a project has required fields we can't predict
    try:
        jira_ticket = {
            'project': {'key': serv['project']},
            'description': description,
            'issuetype': {'name': 'Improvement'},
            'labels': ['perf-watching', 'paasta-rightsizer'],
            'summary': "{s}.{i} in {c} may be {o}provisioned".format(
                s=serv['service'],
                i=serv['instance'],
                c=serv['cluster'],
                o=serv['state'],
            ),
        }
        tick = jira_cli.create_issue(fields=jira_ticket)
    except Exception:
        jira_ticket['project'] = {'key': 'PERF'}
        jira_ticket['labels'].append(serv['service'])
        tick = jira_cli.create_issue(fields=jira_ticket)
    return tick.key


def _get_dashboard_qs_param(param, value):
    # Some dashboards may ask for query string params like param=value, but not this provider.
    return f'variables%5B%5D={param}%3D{param}:{value}'


def main(argv=None):
    args = parse_args(argv)
    services_to_update = get_perf_data(args.splunk_creds, args.file_splunk)

    for serv in services_to_update:
        filename = '{}/{}.yaml'.format(serv['service'], serv['cluster'])
        cpus = float(serv['cpus'])
        provisioned_state = 'over'
        if cpus > float(serv['old_cpus']):
            provisioned_state = 'under'

        serv['state'] = provisioned_state
        ticket_desc = (
            "This ticket and CR have been auto-generated to help keep PaaSTA right-sized."
            "\nPERF will review this CR and give a shipit. Then an ops deputy from your team can merge"
            " if these values look good for your service after review."
            "\nOpen an issue with any concerns and someone from PERF will respond."
            "\nWe suspect that {s}.{i} in {c} may have been {o}-provisioned"
            " during the 1 week prior to {d}. It initially had {x} cpus, but based on the below dashboard,"
            " we recommend {y} cpus."
            "\n- Dashboard: https://y.yelpcorp.com/{o}provisioned?{cluster_param}&{service_param}&{instance_param}"
            "\n- Service owner: {n}"
            "\n- Estimated monthly excess cost: ${m}"
            "\n\nFor more information and sizing examples for larger services:"
            "\n- Runbook: https://y.yelpcorp.com/rb-provisioning-alert"
            "\n- Alert owner: team-perf@yelp.com"
        ).format(
            s=serv['service'],
            c=serv['cluster'],
            i=serv['instance'],
            o=provisioned_state,
            d=serv['date'],
            n=serv['owner'],
            m=serv['money'],
            x=serv['old_cpus'],
            y=serv['cpus'],
            cluster_param=_get_dashboard_qs_param('paasta_cluster', serv['cluster'].replace('marathon-', '')),
            service_param=_get_dashboard_qs_param('paasta_service', serv['service']),
            instance_param=_get_dashboard_qs_param('paasta_instance', serv['instance']),
        )
        branch = ''
        if args.no_tick:
            branch = 'rightsize-{}'.format(int(time.time()))
        else:
            branch = create_jira_ticket(serv, args.jira_creds, ticket_desc)

        with in_tempdir():
            clone(branch)
            edit_soa_configs(filename, serv['instance'], cpus)
            try:
                commit(filename, serv)
                review(filename, ticket_desc, provisioned_state, args.manual_rb)
            except Exception:
                print((
                    "\nUnable to push changes to {f}. Check if {f} conforms to"
                    "yelpsoa-configs yaml rules. No review created. To see the"
                    "cpu suggestion for this service check {t}."
                ).format(f=filename, t=branch))
                continue


if __name__ == '__main__':
    main()
