import asyncio
import os
import sys
import time

import mock
import pytest

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import SystemPaastaConfig


def time_to_feel_bad(*args, **kwarg):
    # VSCode (debugpy) and PyCharm (pydevd) debugger call time.sleep()
    if "debugpy" not in sys.modules or "_pydevd_bundle" not in sys.modules:
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
            "kube_clusters": {
                "pnw-prod": {"aws_account": "prod"},
                "pnw-devc": {"aws_account": "dev"},
            },
        },
        "/fake_dir/",
    )


@pytest.fixture(scope="function", autouse=True)
def remove_pod_identity_env_vars():
    with mock.patch.dict(
        os.environ,
        {
            k: v
            for k, v in os.environ.items()
            if k not in ["AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE"]
        },
        clear=True,
    ):
        yield


@pytest.fixture(autouse=True)
def mock_read_soa_metadata():
    with mock.patch(
        "service_configuration_lib.read_soa_metadata",
        autospec=True,
    ) as m:
        m.return_value = {"git_sha": "fake_soa_git_sha"}
        yield m


@pytest.fixture(autouse=True)
def mock_ktools_read_soa_metadata(mock_read_soa_metadata):
    with mock.patch(
        "paasta_tools.kubernetes_tools.read_soa_metadata",
        mock_read_soa_metadata,
        autospec=None,
    ):
        yield mock_read_soa_metadata


@pytest.fixture(autouse=True)
def cache_clear_KubeClient():
    KubeClient.__new__.cache_clear()
    KubeClient.__init__.cache_clear()


class Struct:
    """
    convert a dictionary to an object
    """

    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __iter__(self):
        return iter(self.__dict__)

    def __getitem__(self, property_name):
        """Get a property value by name.
        :type property_name: str
        """
        return self.__dict__[property_name]

    def __setitem__(self, property_name, val):
        """Set a property value by name.
        :type property_name: str
        """
        self.__dict__[property_name] = val

    def to_dict(self):
        return self.__dict__


def wrap_value_in_task(value):
    async def returner():
        return value

    return asyncio.create_task(returner())
