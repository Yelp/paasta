#!/usr/bin/env python
import argparse
import datetime
import logging
from collections import defaultdict

import pysensu_yelp
import requests

from paasta_tools import mesos_tools
from paasta_tools.monitoring_tools import send_event
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_services


logger = logging.getLogger(__name__)


JUPYTER_PREFIX = 'jupyterhub_'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Reports long-running Spark frameworks.',
    )
    parser.add_argument(
        '--min-hours',
        type=float,
        help='Report frameworks that have been registered for more than this duration',
        default=0,
    )
    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='Skip notifying the teams that own each framework',
    )
    return parser.parse_args()


def get_time_running(framework):
    registered_time = datetime.datetime.fromtimestamp(framework['registered_time'])
    return datetime.datetime.now() - registered_time


def get_spark_properties(framework):
    webui_url = framework.get('webui_url')
    if not webui_url:
        return None

    env_endpoint = f'{webui_url}/api/v1/applications/{framework.id}/environment'
    try:
        response = requests.get(env_endpoint, timeout=5)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        logger.warning(f'Unable to connect to {env_endpoint}: {e!r}')
        return None

    if response.status_code != 200:
        logger.warning(f'Bad response from {env_endpoint}: {response.status_code}')
        return None

    try:
        return response.json()['sparkProperties']
    except (ValueError, KeyError):
        logger.warning(f'Unable to get sparkProperties for {framework.id}: got response {response.text}')
        return None


def guess_service(properties):
    if not properties:
        return None
    for key, value in properties:
        if key == 'spark.executorEnv.PAASTA_SERVICE':
            service = value
            break
    else:
        return None
    if service.startswith(JUPYTER_PREFIX):
        return service[len(JUPYTER_PREFIX):]
    else:
        return service


def get_matching_framework_info(min_hours):
    frameworks = mesos_tools.get_all_frameworks(active_only=True)
    matching_info = []
    min_timedelta = datetime.timedelta(hours=min_hours)
    for framework in frameworks:
        if not framework.active:
            continue
        if framework.get('principal') != 'spark':
            continue
        time_running = get_time_running(framework)
        if time_running >= min_timedelta:
            info = {
                'id': framework.id,
                'name': framework.name,
                'webui_url': framework.get('webui_url'),
                'service': guess_service(get_spark_properties(framework)),
                'user': framework.user,
                'time_running': str(time_running),
            }
            matching_info.append(info)

    return matching_info


def format_framework(info):
    result = [f'{info["name"]} (running for {info["time_running"]})']
    result.append(f'  user: {info["user"]}')
    result.append(f'  job UI: {info["webui_url"]}')
    return '\n'.join(result)


def format_message_for_service(service, frameworks):
    output = f'Found the following long-running Spark frameworks associated with service {service}.\n'
    output += f'Please check why they are still running and terminate if appropriate.\n\n'
    output += '\n'.join(format_framework(f) for f in frameworks)
    return output


def get_messages_by_service(results):
    results_by_service = defaultdict(list)
    for result in results:
        service = result['service']
        results_by_service[service].append(result)

    return {
        service: format_message_for_service(service, results)
        for service, results in results_by_service.items()
    }


def update_check_status(service, output, status):
    overrides = {
        'page': False,
        'alert_after': 0,
        'tip': 'Ask the user to check the job UI and terminate the job if appropriate.',
        'runbook': 'http://y/spark-debug',
        'ticket': True,
    }
    send_event(
        service=service,
        check_name=f'long_running_spark_jobs.{service}',
        overrides=overrides,
        status=status,
        output=output,
        soa_dir=DEFAULT_SOA_DIR,
    )


def report_spark_jobs(min_hours, no_notify):
    results = get_matching_framework_info(
        min_hours=min_hours,
    )
    messages_by_service = get_messages_by_service(results)
    valid_services = set(list_services())

    messages_for_unknown_services = []
    for service, message in messages_by_service.items():
        if service in valid_services:
            print(f'{message}\n')
        else:
            messages_for_unknown_services.append(message)
    if messages_for_unknown_services:
        print('\nINVALID SERVICES')
        print('----------------')
        print('The following frameworks are associated with services that are not configured in PaaSTA.\n')
        print('\n\n'.join(messages_for_unknown_services))

    if not no_notify:
        for service in valid_services:
            if service in messages_by_service:
                update_check_status(service, message, pysensu_yelp.Status.WARNING)
            else:
                update_check_status(service, 'No long running spark jobs', pysensu_yelp.Status.OK)


def main():
    args = parse_args()
    logging.basicConfig()
    report_spark_jobs(args.min_hours, args.no_notify)


if __name__ == '__main__':
    main()
