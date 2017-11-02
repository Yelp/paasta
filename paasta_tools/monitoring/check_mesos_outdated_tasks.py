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
import argparse
import datetime
import sys
import time

from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import load_system_paasta_config


OUTPUT_FORMAT = "{:<30}  {:<8}  {:<20}  {:<27}  {}"
FRAMEWORK_NAME = "marathon"
MAX_BOUNCE_TIME_IN_HOURS = 4


def parse_args():
    parser = argparse.ArgumentParser(
        description='Find all containers serving previous push versions.',
    )
    parser.add_argument(
        '--bounce-time', dest="bounce_time", type=int,
        default=MAX_BOUNCE_TIME_IN_HOURS,
        help=(
            "Ignore versions that were launched in the last BOUNCE_TIME hours "
            "because they probably are still bouncing."
        ),
    )
    return parser.parse_args()


def get_mesos_state():
    state = get_mesos_master().state
    return state


def marathon_tasks(state):
    for framework in state.get('frameworks', []):
        if framework['name'].lower().startswith(FRAMEWORK_NAME):
            for task in framework.get('tasks', []):
                yield task


def create_slave_id_to_hostname_dict(state):
    res = {}
    for slave in state['slaves']:
        res[slave['id']] = slave['hostname']
    return res


def group_running_tasks_by_id_and_gitsha(state):
    res = {}
    for t in marathon_tasks(state):
        if t['state'] == 'TASK_RUNNING':
            task_id = t['name'][:t['name'].find('.', t['name'].find('.') + 1)]
            gitsha = t['name'][len(task_id) + 1:t['name'].find('.', len(task_id) + 1)]
            res.setdefault(task_id, {}).setdefault(gitsha, []).append(t)
    return res


def detect_outdated_gitshas(versions, max_bounce_time_in_hours):
    """Find versions that should have drained more than 'max_bounce_time_in_hours' ago"""
    if len(versions) < 2:
        return []
    deploy_time = {}
    latest_deploy = 0
    for version, tasks in versions.items():
        deploy_time[version] = sum(t['statuses'][0]['timestamp'] for t in tasks) / len(tasks)
        if (
            deploy_time[version] > latest_deploy and
            time.time() - deploy_time[version] > max_bounce_time_in_hours * 3600
        ):
            latest_deploy = deploy_time[version]
    return [version for version, dtime in deploy_time.items() if dtime < latest_deploy]


def report_outdated_instances(task_id, gitsha, tasks, slave_id2hostname):
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
                task_id.replace('--', '_')[:30],
                gitsha[3:],
                deploy_time,
                hostname,
                container_name,
            ),
        )
    return output


def check_mesos_tasks(max_bounce_time_in_hours=MAX_BOUNCE_TIME_IN_HOURS):
    output = []
    state = get_mesos_state()
    aggregated_tasks = group_running_tasks_by_id_and_gitsha(state)
    slave_id2hostname = create_slave_id_to_hostname_dict(state)
    for task_id, versions in aggregated_tasks.items():
        for gitsha in detect_outdated_gitshas(versions, max_bounce_time_in_hours):
            output.extend(report_outdated_instances(
                task_id, gitsha, versions[gitsha],
                slave_id2hostname,
            ))
    return output


def main():
    args = parse_args()
    cluster = load_system_paasta_config().get_cluster()
    output = check_mesos_tasks(args.bounce_time)
    if output:
        print("CRITICAL - There are {} tasks running in {} that are more than {}h older than their"
              " last bounce.".format(len(output), cluster, args.bounce_time))
        print(OUTPUT_FORMAT.format('SERVICE.INSTANCE', 'COMMIT', 'CREATED', 'HOSTNAME', 'CONTAINER'))
        print('\n'.join(output))
        return 1
    else:
        print("OK - There are no outdated tasks in {}".format(cluster))
        return 0


if __name__ == "__main__":
    sys.exit(main())
