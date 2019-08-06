import asyncio
import concurrent
import time
import unittest
from queue import Empty

import mock
import pytest

from paasta_tools.deployd.common import BaseServiceInstance
from paasta_tools.deployd.common import DelayDeadlineQueue
from paasta_tools.deployd.common import exponential_back_off
from paasta_tools.deployd.common import get_marathon_clients_from_config
from paasta_tools.deployd.common import get_service_instances_needing_update
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError


class TestPaastaThread(unittest.TestCase):
    def setUp(self):
        self.thread = PaastaThread()

    def test_log(self):
        self.thread.log.info("HAAAALP ME")


class TestPaastaQueue(unittest.TestCase):
    def setUp(self):
        self.queue = PaastaQueue("AtThePostOffice")

    def test_log(self):
        self.queue.log.info("HAAAALP ME")

    def test_put(self):
        with mock.patch(
            "paasta_tools.deployd.common.Queue.put", autospec=True
        ) as mock_q_put:
            self.queue.put("human")
            mock_q_put.assert_called_with(self.queue, "human")


class TestDelayDeadlineQueue:
    @pytest.fixture
    def queue(self):
        yield DelayDeadlineQueue()

    def test_log(self, queue):
        queue.log.info("HAAAALP ME")

    def test_put(self):
        # We have to mock time.time and asyncio.sleep before the DelayDeadlineQueue, so that they are mocked when the
        # background thread is launched.
        with mock.patch("time.time", autospec=True) as mock_time, mock.patch(
            "asyncio.sleep", autospec=True
        ) as mock_asyncio_sleep:
            queue = DelayDeadlineQueue()
            sleep_future = concurrent.futures.Future()
            mock_asyncio_sleep.return_value = asyncio.wrap_future(
                sleep_future, loop=queue.loop
            )

            with mock.patch.object(
                queue.available_service_instances,
                "put",
                wraps=queue.available_service_instances.put,
            ) as mock_available_service_instances_put:
                mock_time.return_value = 5

                si1 = mock.Mock(wait_until=3, bounce_by=41)
                si2 = mock.Mock(wait_until=6, bounce_by=42)
                si1_future = queue.put(si1)
                si2_future = queue.put(si2)
                # Since si1.wait_until < time.time(), the background thread should immediately make si1 available.
                si1_future.result(
                    timeout=1.0
                )  # wait until si1 is put in the available_service_instances queue
                assert mock_available_service_instances_put.call_count == 1
                mock_available_service_instances_put.assert_called_with((41, si1))

                mock_time.return_value = 7
                sleep_future.set_result(None)  # make all asyncio.sleep() calls finish
                si2_future.result(
                    timeout=2.0
                )  # wait until si2 is put in the available_service_instances queue
                assert mock_available_service_instances_put.call_count == 2

                mock_available_service_instances_put.assert_called_with((42, si2))

    def test_get(self, queue):
        with mock.patch.object(
            queue.available_service_instances, "get", autospec=True
        ) as mock_available_service_instances_get:
            mock_available_service_instances_get.side_effect = [(2, "human"), Empty]
            assert queue.get() == "human"
            with pytest.raises(Empty):
                queue.get(block=False)

    def test_dont_block_indefinitely_when_wait_until_is_in_future(self, queue):
        """Regression test for a specific bug in the first implementation of DelayDeadlineQueue"""
        queue.put(
            mock.Mock(wait_until=time.time() + 0.001, bounce_by=time.time() + 0.001)
        )
        queue.get(timeout=1.0)


class TestServiceInstance(unittest.TestCase):
    def setUp(self):
        self.service_instance = ServiceInstance(  # type: ignore
            service="universe",
            instance="c137",
            watcher="mywatcher",
            cluster="westeros-prod",
            bounce_by=0,
            wait_until=0,
        )

    def test___new__(self):
        expected = BaseServiceInstance(
            service="universe",
            instance="c137",
            watcher="mywatcher",
            bounce_by=0,
            wait_until=0,
            failures=0,
            bounce_timers=None,
            processed_count=0,
        )
        assert self.service_instance == expected

        expected = BaseServiceInstance(
            service="universe",
            instance="c137",
            watcher="mywatcher",
            bounce_by=0,
            wait_until=0,
            failures=0,
            bounce_timers=None,
            processed_count=0,
        )
        # https://github.com/python/mypy/issues/2852
        assert (
            ServiceInstance(  # type: ignore
                service="universe",
                instance="c137",
                watcher="mywatcher",
                cluster="westeros-prod",
                bounce_by=0,
                wait_until=0,
            )
            == expected
        )


def test_exponential_back_off():
    assert exponential_back_off(0, 60, 2, 6000) == 60
    assert exponential_back_off(2, 60, 2, 6000) == 240
    assert exponential_back_off(99, 60, 2, 6000) == 6000


def test_get_service_instances_needing_update():
    with mock.patch(
        "paasta_tools.deployd.common.get_all_marathon_apps", autospec=True
    ) as mock_get_marathon_apps, mock.patch(
        "paasta_tools.deployd.common.load_marathon_service_config_no_cache",
        autospec=True,
    ) as mock_load_marathon_service_config:
        mock_marathon_apps = [
            mock.Mock(id="/universe.c137.c1.g1", instances=2),
            mock.Mock(id="/universe.c138.c1.g1", instances=2),
        ]
        mock_get_marathon_apps.return_value = mock_marathon_apps
        mock_service_instances = [("universe", "c137"), ("universe", "c138")]
        mock_configs = [
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c137.c1.g1", "instances": 2}
                )
            ),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert mock_get_marathon_apps.called
        calls = [
            mock.call(
                service="universe",
                instance="c137",
                cluster="westeros-prod",
                soa_dir=DEFAULT_SOA_DIR,
            ),
            mock.call(
                service="universe",
                instance="c138",
                cluster="westeros-prod",
                soa_dir=DEFAULT_SOA_DIR,
            ),
        ]
        mock_load_marathon_service_config.assert_has_calls(calls)
        assert ret == [("universe", "c138")]

        mock_configs = [
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c137.c1.g1", "instances": 3}
                )
            ),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert ret == [("universe", "c137"), ("universe", "c138")]

        mock_configs = [
            mock.Mock(
                format_marathon_app_dict=mock.Mock(side_effect=NoDockerImageError)
            ),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert ret == [("universe", "c138")]

        mock_configs = [
            mock.Mock(
                format_marathon_app_dict=mock.Mock(side_effect=NoSlavesAvailableError)
            ),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert ret == [("universe", "c138")]

        mock_configs = [
            mock.Mock(
                format_marathon_app_dict=mock.Mock(side_effect=InvalidJobNameError)
            ),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert ret == [("universe", "c138")]

        mock_configs = [
            mock.Mock(
                format_marathon_app_dict=mock.Mock(side_effect=NoDeploymentsAvailable)
            ),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert ret == [("universe", "c138")]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=Exception)),
            mock.Mock(
                format_marathon_app_dict=mock.Mock(
                    return_value={"id": "universe.c138.c2.g2", "instances": 2}
                )
            ),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(
            fake_clients, mock_service_instances, "westeros-prod"
        )
        assert ret == [("universe", "c138")]


def test_get_marathon_clients_from_config():
    with mock.patch(
        "paasta_tools.deployd.common.load_system_paasta_config", autospec=True
    ), mock.patch(
        "paasta_tools.deployd.common.get_marathon_servers", autospec=True
    ), mock.patch(
        "paasta_tools.deployd.common.get_marathon_clients", autospec=True
    ) as mock_marathon_clients:
        assert get_marathon_clients_from_config() == mock_marathon_clients.return_value
