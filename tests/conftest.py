import time

import mock
import pytest

from paasta_tools.utils import compose_job_id
from paasta_tools.utils import SystemPaastaConfig


def time_to_feel_bad(*args, **kwarg):
    raise Exception(
        "This test called time.sleep() which is bad and slows down our test suite"
    )


time.true_slow_sleep = time.sleep
time.sleep = time_to_feel_bad


@pytest.fixture
def system_paasta_config():
    return SystemPaastaConfig(
        {
            "cluster": "fake_cluster",
            "api_endpoints": {"fake_cluster": "http://fake_cluster:5054"},
            "docker_registry": "fake_registry",
            "volumes": [
                {
                    "hostPath": "/hostPath",
                    "containerPath": "/containerPath",
                    "mode": "RO",
                }
            ],
        },
        "/fake_dir/",
    )


@pytest.fixture
def flink_instance_config():
    service = "fake_service"
    instance = "fake_instance"
    job_id = compose_job_id(service, instance)
    mock_instance_config = mock.Mock(
        service=service,
        instance=instance,
        cluster="fake_cluster",
        soa_dir="fake_soa_dir",
        job_id=job_id,
        config_dict={},
    )
    mock_instance_config.get_replication_crit_percentage.return_value = 100
    mock_instance_config.get_registrations.return_value = [job_id]
    return mock_instance_config
