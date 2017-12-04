import pytest

from paasta_tools.utils import SystemPaastaConfig


@pytest.fixture
def system_paasta_config():
    return SystemPaastaConfig(
        {
            'cluster': 'fake_cluster',
            'api_endpoints': {'fake_cluster': "http://fake_cluster:5054"},
            'docker_registry': 'fake_registry',
            'volumes': [{
                "hostPath": "/hostPath",
                "containerPath": "/containerPath",
                "mode": "RO",
            }],
        },
        '/fake_dir/',
    )
