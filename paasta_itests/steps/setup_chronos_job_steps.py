
import contextlib
import sys

from behave import when, then
import mock

sys.path.append('../')
from paasta_tools import setup_chronos_job
from paasta_tools import chronos_tools

fake_service_name = 'fake_complete_service'
fake_instance_name = 'fake_instance'
fake_appid = 'fake_app_id'
fake_service_job_config = chronos_tools.ChronosJobConfig(
    fake_service_name,
    fake_instance_name,
    {},
    {'docker_image': 'test-image', 'desired_state': 'start'},
)

fake_service_config = {
    "retries": 1,
    "container": {
        "image": "localhost/fake_docker_url",
        "type": "DOCKER",
        "network": "BRIDGE",
        'volumes': [
            {'hostPath': u'/nail/etc/habitat', 'containerPath': '/nail/etc/habitat', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/datacenter', 'containerPath': '/nail/etc/datacenter', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/ecosystem', 'containerPath': '/nail/etc/ecosystem', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/rntimeenv', 'containerPath': '/nail/etc/rntimeenv', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/region', 'containerPath': '/nail/etc/region', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/sperregion', 'containerPath': '/nail/etc/sperregion', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/topology_env', 'containerPath': '/nail/etc/topology_env', 'mode': 'RO'},
            {'hostPath': u'/nail/srv', 'containerPath': '/nail/srv', 'mode': 'RO'},
            {'hostPath': u'/etc/boto_cfg', 'containerPath': '/etc/boto_cfg', 'mode': 'RO'}]
    },
    "name": fake_appid,
    "schedule": "R//PT10M",
    "mem": 128,
    "epsilon": "PT30S",
    "cpus": 0.1,
    "disabled": False,
    "command": "fake command",
    "owner": "fake_team",
    "async": True,
    "disk": 256
}


@when(u'we create a complete chronos job')
def create_complete_job(context):
    with contextlib.nested(
        mock.patch('paasta_tools.chronos_tools.create_complete_config'),
    ) as (
        mock_create_complete_config,
    ):
        mock_create_complete_config.return_value = fake_service_config
        return_tuple = setup_chronos_job.setup_job(
            fake_service_name,
            fake_instance_name,
            fake_service_job_config,
            context.chronos_client,
            None,
        )
        print return_tuple
        assert return_tuple[0] == 0
        assert 'Deployed job' in return_tuple[1]


@then(u'we should see it in the list of jobs')
def see_it_in_list_of_jobs(context):
    job_names = [job['name'] for job in context.chronos_client.list()]
    assert fake_appid in job_names
