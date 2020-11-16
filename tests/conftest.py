import time

import pytest

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
            "service_discovery_providers": {"smartstack": {}, "envoy": {}},
        },
        "/fake_dir/",
    )
