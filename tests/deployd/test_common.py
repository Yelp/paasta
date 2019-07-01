import unittest
from queue import Empty

import mock

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
            'paasta_tools.deployd.common.Queue.put', autospec=True,
        ) as mock_q_put:
            self.queue.put("human")
            mock_q_put.assert_called_with(self.queue, "human")


class TestDelayDeadlineQueue(unittest.TestCase):
    def setUp(self):
        self.queue = DelayDeadlineQueue()

    def test_log(self):
        self.queue.log.info("HAAAALP ME")

    def test_put(self):
        with mock.patch.object(
            self.queue.unavailable_service_instances,
            'put',
            wraps=self.queue.unavailable_service_instances.put,
        ) as mock_unavailable_service_instances_put, mock.patch.object(
            self.queue.available_service_instances,
            'put',
            wraps=self.queue.available_service_instances.put,
        ) as mock_available_service_instances_put:
            si1 = mock.Mock(wait_until=6, bounce_by=4)
            self.queue.put(si1, now=5)
            mock_unavailable_service_instances_put.assert_called_with((6, 4, si1))
            assert mock_available_service_instances_put.call_count == 0

            mock_unavailable_service_instances_put.reset_mock()
            si2 = mock.Mock(wait_until=3, bounce_by=4)
            self.queue.put(si2, now=5)
            print(mock_unavailable_service_instances_put.mock_calls)
            mock_unavailable_service_instances_put.assert_any_call((3, 4, si2))
            # this is left over from the previous insertion, and gets re-inserted by
            # process_unavailable_service_instances because 6 > now
            mock_unavailable_service_instances_put.assert_any_call((6, 4, si1))
            mock_available_service_instances_put.assert_called_with((4, si2))

    def test_get(self):
        with mock.patch.object(
            self.queue.unavailable_service_instances,
            'get',
            autospec=True,
        ) as mock_unavailable_service_instances_get:
            mock_unavailable_service_instances_get.side_effect = [
                (3, 2, "human"),
                Empty,
            ]
            assert self.queue.get() == "human"


class TestServiceInstance(unittest.TestCase):
    def setUp(self):
        self.service_instance = ServiceInstance(  # type: ignore
            service='universe',
            instance='c137',
            watcher='mywatcher',
            cluster='westeros-prod',
            bounce_by=0,
            wait_until=0,
        )

    def test___new__(self):
        expected = BaseServiceInstance(
            service='universe',
            instance='c137',
            watcher='mywatcher',
            bounce_by=0,
            wait_until=0,
            failures=0,
            bounce_timers=None,
            processed_count=0,
        )
        assert self.service_instance == expected

        expected = BaseServiceInstance(
            service='universe',
            instance='c137',
            watcher='mywatcher',
            bounce_by=0,
            wait_until=0,
            failures=0,
            bounce_timers=None,
            processed_count=0,
        )
        # https://github.com/python/mypy/issues/2852
        assert ServiceInstance(  # type: ignore
            service='universe',
            instance='c137',
            watcher='mywatcher',
            cluster='westeros-prod',
            bounce_by=0,
            wait_until=0,
        ) == expected


def test_exponential_back_off():
    assert exponential_back_off(0, 60, 2, 6000) == 60
    assert exponential_back_off(2, 60, 2, 6000) == 240
    assert exponential_back_off(99, 60, 2, 6000) == 6000


def test_get_service_instances_needing_update():
    with mock.patch(
        'paasta_tools.deployd.common.get_all_marathon_apps', autospec=True,
    ) as mock_get_marathon_apps, mock.patch(
        'paasta_tools.deployd.common.load_marathon_service_config_no_cache', autospec=True,
    ) as mock_load_marathon_service_config:
        mock_marathon_apps = [
            mock.Mock(id='/universe.c137.c1.g1', instances=2),
            mock.Mock(id='/universe.c138.c1.g1', instances=2),
        ]
        mock_get_marathon_apps.return_value = mock_marathon_apps
        mock_service_instances = [('universe', 'c137'), ('universe', 'c138')]
        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c137.c1.g1',
                'instances': 2,
            })),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert mock_get_marathon_apps.called
        calls = [
            mock.call(
                service='universe',
                instance='c137',
                cluster='westeros-prod',
                soa_dir=DEFAULT_SOA_DIR,
            ),
            mock.call(
                service='universe',
                instance='c138',
                cluster='westeros-prod',
                soa_dir=DEFAULT_SOA_DIR,
            ),
        ]
        mock_load_marathon_service_config.assert_has_calls(calls)
        assert ret == [('universe', 'c138')]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c137.c1.g1',
                'instances': 3,
            })),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c137'), ('universe', 'c138')]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=NoDockerImageError)),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c138')]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=NoSlavesAvailableError)),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c138')]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=InvalidJobNameError)),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c138')]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=NoDeploymentsAvailable)),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c138')]

        mock_configs = [
            mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=Exception)),
            mock.Mock(format_marathon_app_dict=mock.Mock(return_value={
                'id': 'universe.c138.c2.g2',
                'instances': 2,
            })),
        ]
        mock_load_marathon_service_config.side_effect = mock_configs
        mock_client = mock.Mock(servers=["foo"])
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        ret = get_service_instances_needing_update(fake_clients, mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c138')]


def test_get_marathon_clients_from_config():
    with mock.patch(
        'paasta_tools.deployd.common.load_system_paasta_config', autospec=True,
    ), mock.patch(
        'paasta_tools.deployd.common.get_marathon_servers', autospec=True,
    ), mock.patch(
        'paasta_tools.deployd.common.get_marathon_clients', autospec=True,
    ) as mock_marathon_clients:
        assert get_marathon_clients_from_config() == mock_marathon_clients.return_value
