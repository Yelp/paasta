#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
"""
PaaSTA service instance status/start/stop etc.
"""
import traceback

from pyramid.view import view_config

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.paasta_serviceinit import get_deployment_version
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import validate_service_instance


def chronos_instance_status(instance_status, service, instance, verbose):
    cstatus = {}
    return cstatus


def marathon_job_status(mstatus, client, job_config):
    try:
        app_id = job_config.format_marathon_app_dict()['id']
    except NoDockerImageError:
        error_msg = "Docker image is not in deployments.json."
        mstatus['error_message'] = error_msg
        return

    mstatus['app_id'] = app_id
    mstatus['slaves'] = list({task.slave['hostname'] for task in get_running_tasks_from_active_frameworks(app_id)})
    mstatus['expected_instance_count'] = job_config.get_instances()

    deploy_status = marathon_tools.get_marathon_app_deploy_status(client, app_id)
    mstatus['deploy_status'] = marathon_tools.MarathonDeployStatus.tostring(deploy_status)

    # by comparing running count with expected count, callers can figure
    # out if the instance is in Healthy, Warning or Critical state.
    if deploy_status == marathon_tools.MarathonDeployStatus.NotRunning:
        mstatus['running_instance_count'] = 0
    else:
        mstatus['running_instance_count'] = client.get_app(app_id).tasks_running

    if deploy_status == marathon_tools.MarathonDeployStatus.Delayed:
        _, backoff_seconds = marathon_tools.get_app_queue_status(client, app_id)
        mstatus['backoff_seconds'] = backoff_seconds


def marathon_instance_status(instance_status, service, instance, verbose):
    mstatus = {}
    apps = marathon_tools.get_matching_appids(service, instance, settings.marathon_client)
    job_config = marathon_tools.load_marathon_service_config(
        service, instance, settings.cluster, soa_dir=settings.soa_dir)

    # bouncing status can be inferred from app_count, ref get_bouncing_status
    mstatus['app_count'] = len(apps)
    mstatus['desired_state'] = job_config.get_desired_state()
    mstatus['bounce_method'] = job_config.get_bounce_method()
    marathon_job_status(mstatus, settings.marathon_client, job_config)
    return mstatus


@view_config(route_name='service.instance.status', request_method='GET', renderer='json')
def instance_status(request):
    service = request.swagger_data.get('service')
    instance = request.swagger_data.get('instance')
    verbose = request.matchdict.get('verbose', False)

    instance_status = {}
    instance_status['service'] = service
    instance_status['instance'] = instance

    try:
        actual_deployments = get_actual_deployments(service, settings.soa_dir)
    except:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    version = get_deployment_version(actual_deployments, settings.cluster, instance)
    # exit if the deployment key is not found
    if not version:
        error_message = 'deployment key %s not found' % '.'.join([settings.cluster, instance])
        raise ApiFailure(error_message, 404)

    instance_status['git_sha'] = version

    try:
        instance_type = validate_service_instance(service, instance, settings.cluster, settings.soa_dir)
        if instance_type == 'marathon':
            instance_status['marathon'] = marathon_instance_status(instance_status, service, instance, verbose)
        elif instance_type == 'chronos':
            instance_status['chronos'] = chronos_instance_status(instance_status, service, instance, verbose)
        else:
            error_message = 'Unknown instance_type %s of %s.%s' % (instance_type, service, instance)
            raise ApiFailure(error_message, 404)
    except:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    return instance_status
