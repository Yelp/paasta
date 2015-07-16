import contextlib
import sys

from behave import when, then
import mock

sys.path.append('../')
from paasta_tools import setup_marathon_job
from paasta_tools import marathon_tools

fake_service_name = 'fake_complete_service'
fake_instance_name = 'fake_instance'
fake_appid = 'fake--complete--service.gitdeadbeef.configdeadbeef2'
fake_service_marathon_config = marathon_tools.MarathonServiceConfig(
    fake_service_name,
    fake_instance_name,
    {},
    {'docker_image': 'test-image'},
)
fake_service_config = {
    'id': fake_appid,
    'container': {
        'docker': {
            'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}],
            'image': 'localhost/fake_docker_url',
            'network': 'BRIDGE'
            },
        'type': 'DOCKER',
        'volumes': [
            {'hostPath': '/nail/etc/habitat', 'containerPath': '/nail/etc/habitat', 'mode': 'RO'},
            {'hostPath': '/nail/etc/datacenter', 'containerPath': '/nail/etc/datacenter', 'mode': 'RO'},
            {'hostPath': '/nail/etc/ecosystem', 'containerPath': '/nail/etc/ecosystem', 'mode': 'RO'},
            {'hostPath': '/nail/etc/rntimeenv', 'containerPath': '/nail/etc/rntimeenv', 'mode': 'RO'},
            {'hostPath': '/nail/etc/region', 'containerPath': '/nail/etc/region', 'mode': 'RO'},
            {'hostPath': '/nail/etc/sperregion', 'containerPath': '/nail/etc/sperregion', 'mode': 'RO'},
            {'hostPath': '/nail/etc/topology_env', 'containerPath': '/nail/etc/topology_env', 'mode': 'RO'},
            {'hostPath': '/nail/srv', 'containerPath': '/nail/srv', 'mode': 'RO'},
            {'hostPath': '/etc/boto_cfg', 'containerPath': '/etc/boto_cfg', 'mode': 'RO'}]
        },
    'instances': 1,
    'mem': 300,
    'args': [],
    'backoff_factor': 2,
    'cpus': 0.25,
    'uris': ['file:///root/.dockercfg'],
    'backoff_seconds': 1,
    'constraints': None
}

@when(u'we create a complete app')
def create_complete_app(context):
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.create_complete_config'),
        mock.patch('paasta_tools.marathon_tools.load_marathon_config', return_value=context.marathon_config),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=context.system_paasta_config),
        mock.patch('paasta_tools.bounce_lib.load_system_paasta_config', return_value=context.system_paasta_config),
    ) as (
        mock_create_complete_config,
        _,
        _,
        _,
    ):
        mock_create_complete_config.return_value = fake_service_config
        print marathon_tools.load_marathon_config()
        return_tuple = setup_marathon_job.setup_service(
            fake_service_name,
            fake_instance_name,
            context.client,
            context.marathon_config,
            fake_service_marathon_config,
        )
        assert return_tuple[0] == 0
        assert 'deployed' in return_tuple[1]


@then(u'we should see it in the list of apps')
def see_it_in_list(context):
    assert fake_appid in marathon_tools.list_all_marathon_app_ids(context.client)


@then(u'we can run get_app on it')
def can_run_get_app(context):
    assert context.client.get_app(fake_appid)

