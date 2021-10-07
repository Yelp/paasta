import asyncio
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
