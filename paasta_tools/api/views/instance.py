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
import logging
import traceback

from pyramid.response import Response
from pyramid.view import view_config

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.marathon_tools import get_marathon_app_deploy_status
from paasta_tools.marathon_tools import MarathonDeployStatus
from paasta_tools.paasta_serviceinit import get_deployment_version
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import validate_service_instance


log = logging.getLogger(__name__)


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

    if marathon_tools.is_app_id_running(app_id, client):
        app = client.get_app(app_id)
        deploy_status, _ = get_marathon_app_deploy_status(app, app_id, client)
        mstatus['deploy_status'] = MarathonDeployStatus.tostring(deploy_status)

        # by comparing running count with expected count, callers can figure
        # out if the instance is in Healthy, Warning or Critical state.
        mstatus['running_instance_count'] = app.tasks_running
        mstatus['expected_instance_count'] = job_config.get_instances()
    else:
        mstatus['deploy_status'] = 'Not Running'


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


class InstanceFailure(Exception):
    def __init__(self, msg, err):
        self.msg = msg
        self.err = err


@view_config(context=InstanceFailure)
def instance_failure_response(exc, request):
    """Construct an HTTP response with an error status code. This happens when
    the API service has to stop on a 'hard' error. In contrast, the API service
    continues to produce results on a 'soft' error. It will place a 'message'
    field in the output. Multiple 'soft' errors are concatenated in the same
    'message' field when errors happen in the same hierarchy.
    """
    log.error(exc.msg)

    response = Response('ERROR: %s' % exc.msg)
    response.status_int = exc.err
    return response


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
        raise InstanceFailure(error_message, 500)

    version = get_deployment_version(actual_deployments, settings.cluster, instance)
    # exit if the deployment key is not found
    if not version:
        error_message = 'deployment key %s not found' % '.'.join([settings.cluster, instance])
        raise InstanceFailure(error_message, 404)

    instance_status['git_sha'] = version

    try:
        instance_type = validate_service_instance(service, instance, settings.cluster, settings.soa_dir)
        if instance_type == 'marathon':
            instance_status['marathon'] = marathon_instance_status(instance_status, service, instance, verbose)
        elif instance_type == 'chronos':
            instance_status['chronos'] = chronos_instance_status(instance_status, service, instance, verbose)
        else:
            error_message = 'Unknown instance_type %s of %s.%s' % (instance_type, service, instance)
            raise InstanceFailure(error_message, 404)
    except:
        error_message = traceback.format_exc()
        raise InstanceFailure(error_message, 500)

    return instance_status
