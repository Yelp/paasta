import sys
import time

import mock

sys.path.append('../')
import service_deployment_tools
from service_deployment_tools import paasta_serviceinit

@given(u'a currently running job - test-service.main')
def step_impl(context):
    trivial_app_config = {
        'id': 'test-service.main',
        'cmd': '/bin/sleep 1m',
    }
    with mock.patch('service_deployment_tools.bounce_lib.create_app_lock'):
        service_deployment_tools.bounce_lib.create_marathon_app('test-service.main', trivial_app_config, context.client)

@when(u'we wait a bit for it to be deployed')
def step_impl(context):
    print "Sleeping 10 seconds to wait for test-service to be deployed."
    time.sleep(10)

@then(u'paasta_serviceinit status should try to exit 0')
def step_impl(context):
    normal_instance_count = 1
    client = context.client
    app_id = 'test-service.main'
    service = 'test-service'
    instance = 'main'

    with mock.patch('sys.exit') as sys_exit_patch:
        paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
        sys_exit_patch.assert_called_once_with(0)

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
