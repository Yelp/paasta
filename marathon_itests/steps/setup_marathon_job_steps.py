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
fake_service_config = {'container': {'docker': {'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}], 'image': u'localhost/fake_docker_url', 'network': 'BRIDGE'}, 'type': 'DOCKER', 'volumes': [{u'hostPath': u'/nail/etc/habitat', u'containerPath': u'/nail/etc/habitat', u'mode': u'RO'}, {u'hostPath': u'/nail/etc/datacenter', u'containerPath': u'/nail/etc/datacenter', u'mode': u'RO'}, {u'hostPath': u'/nail/etc/ecosystem', u'containerPath': u'/nail/etc/ecosystem', u'mode': u'RO'}, {u'hostPath': u'/nail/etc/runtimeenv', u'containerPath': u'/nail/etc/runtimeenv', u'mode': u'RO'}, {u'hostPath': u'/nail/etc/region', u'containerPath': u'/nail/etc/region', u'mode': u'RO'}, {u'hostPath': u'/nail/etc/superregion', u'containerPath': u'/nail/etc/superregion', u'mode': u'RO'}, {u'hostPath': u'/nail/etc/topology_env', u'containerPath': u'/nail/etc/topology_env', u'mode': u'RO'}, {u'hostPath': u'/nail/srv', u'containerPath': u'/nail/srv', u'mode': u'RO'}, {u'hostPath': u'/etc/boto_cfg', u'containerPath': u'/etc/boto_cfg', u'mode': u'RO'}]}, 'instances': 1, 'mem': 300, 'args': [], 'backoff_factor': 2, 'cpus': 0.25, 'uris': ['file:///root/.dockercfg'], 'backoff_seconds': 1, 'id': fake_appid, 'constraints': None}

@when(u'we create a complete app')
def create_complete_app(context):
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.create_complete_config'),
        mock.patch('paasta_tools.marathon_tools.load_marathon_config', return_value=context.marathon_config),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=context.system_paasta_config),
    ) as (
        mock_create_complete_config,
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

