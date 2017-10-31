#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import sys
import time

from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import load_system_paasta_config


OUTPUT_FORMAT = "{:<30}  {:<8}  {:<20}  {:<27}  {}"


def get_mesos_state():
    state = get_mesos_master().state
    return state


def marathon_tasks(state):
    for framework in state.get('frameworks', []):
        if framework['name'].lower().startswith('marathon'):
            for task in framework.get('tasks', []):
                yield task


def create_slave_id_to_hostname_dict(state):
    res = {}
    for slave in state['slaves']:
        res[slave['id']] = slave['hostname']
    return res


def group_running_tasks_by_job_id_and_gitsha(state):
    res = {}
    for t in marathon_tasks(state):
        if t['state'] == 'TASK_RUNNING':
            job_id = t['name'][:t['name'].find('.', t['name'].find('.') + 1)]
            gitsha = t['name'][len(job_id) + 1:t['name'].find('.', len(job_id) + 1)]
            res.setdefault(job_id, {}).setdefault(gitsha, []).append(t)
    return res


def detect_outdated_gitshas(versions, max_bounce_time=4 * 3600):
    """Find versions that should have drained more than bounce_time ago (in seconds)"""
    deploy_time = {}
    res = []
    if len(versions) > 1:
        latest_deploy = 0
        for version, tasks in versions.items():
            deploy_time[version] = sum(t['statuses'][0]['timestamp'] for t in tasks) / len(tasks)
            if deploy_time[version] > latest_deploy and time.time() - deploy_time[version] > max_bounce_time:
                latest_deploy = deploy_time[version]
        for version, dtime in deploy_time.items():
            if dtime + max_bounce_time < latest_deploy:
                res.append(version)
    return res


def report_outdated_instances(job_id, gitsha, tasks, slave_id2hostname):
    output = []
    for t in tasks:
        deploy_time = datetime.datetime.fromtimestamp(int(t['statuses'][0]['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
        container_name = "{}.{}".format(
            t['slave_id'],
            t['statuses'][0]['container_status']['container_id']['value'],
        )
        hostname = slave_id2hostname[t['slave_id']]
        hostname = hostname[:hostname.find('.')]
        output.append(
            OUTPUT_FORMAT.format(
                job_id.replace('--', '_')[:30],
                gitsha[3:],
                deploy_time,
                hostname,
                container_name,
            ),
        )
    return output


def check_mesos_tasks():
    output = []
    state = get_mesos_state()
    aggregated_tasks = group_running_tasks_by_job_id_and_gitsha(state)
    slave_id2hostname = create_slave_id_to_hostname_dict(state)
    for job_id, versions in aggregated_tasks.items():
        for gitsha in detect_outdated_gitshas(versions):
            output.extend(report_outdated_instances(
                job_id, gitsha, versions[gitsha],
                slave_id2hostname,
            ))
    return output


def main():
    cluster = load_system_paasta_config().get_cluster()
    output = check_mesos_tasks()
    if output:
        print("CRITICAL - There are {} outdated instances running in {}"
              .format(len(output), cluster))
        print(OUTPUT_FORMAT.format('SERVICE.INSTANCE', 'COMMIT', 'CREATED', 'HOSTNAME', 'CONTAINER'))
        print('\n'.join(output))
        return 1
    else:
        print("OK - There are no outdated instances in {}".format(cluster))
        return 0


if __name__ == "__main__":
    sys.exit(main())
