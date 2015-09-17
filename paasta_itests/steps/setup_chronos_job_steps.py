import sys

from behave import when, then

sys.path.append('../')
from paasta_tools import setup_chronos_job
from paasta_tools import chronos_tools

fake_service_name = 'fake_complete_service'
fake_instance_name = 'fake_instance'
fake_job_id = 'fake_job_id'
fake_service_job_config = chronos_tools.ChronosJobConfig(
    fake_service_name,
    fake_instance_name,
    {},
    {'docker_image': 'test-image', 'desired_state': 'start'},
)

# TODO DRY out in PAASTA-1174
fake_service_config = {
    "retries": 1,
    "container": {
        "image": "localhost/fake_docker_url",
        "type": "DOCKER",
        "network": "BRIDGE",
        "volumes": [
            {'hostPath': u'/nail/etc/habitat', 'containerPath': '/nail/etc/habitat', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/datacenter', 'containerPath': '/nail/etc/datacenter', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/ecosystem', 'containerPath': '/nail/etc/ecosystem', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/rntimeenv', 'containerPath': '/nail/etc/rntimeenv', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/region', 'containerPath': '/nail/etc/region', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/sperregion', 'containerPath': '/nail/etc/sperregion', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/topology_env', 'containerPath': '/nail/etc/topology_env', 'mode': 'RO'},
            {'hostPath': u'/nail/srv', 'containerPath': '/nail/srv', 'mode': 'RO'},
            {'hostPath': u'/etc/boto_cfg', 'containerPath': '/etc/boto_cfg', 'mode': 'RO'},
        ],
    },
    "name": fake_job_id,
    "schedule": "R//PT10M",
    "mem": 128,
    "epsilon": "PT30S",
    "cpus": 0.1,
    "disabled": False,
    "command": "fake command",
    "owner": "fake_team",
    "async": True,
    "disk": 256,
}


# TODO DRY out in PAASTA-1174 and rename so it doesn't sound like the funcs in chronos_steps
@when(u'we create a complete chronos job')
def create_complete_job(context):
    return_tuple = setup_chronos_job.setup_job(
        fake_service_name,
        fake_instance_name,
        fake_service_job_config,
        fake_service_config,
        context.chronos_client,
        "fake_cluster",
    )
    assert return_tuple[0] == 0
    assert 'Deployed job' in return_tuple[1]


# TODO DRY out in PAASTA-1174
@then(u'we should see it in the list of jobs')
def see_it_in_list_of_jobs(context):
    job_names = [job['name'] for job in context.chronos_client.list()]
    assert fake_job_id in job_names
