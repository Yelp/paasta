import unittest

import mock

from paasta_tools.deployd.common import BaseServiceInstance
from paasta_tools.deployd.common import exponential_back_off
from paasta_tools.deployd.common import get_marathon_clients_from_config
from paasta_tools.deployd.common import get_priority
from paasta_tools.deployd.common import get_service_instances_needing_update
from paasta_tools.deployd.common import PaastaPriorityQueue
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
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


class TestPaastaPriorityQueue(unittest.TestCase):
    def setUp(self):
        self.queue = PaastaPriorityQueue("AtThePostOffice")

    def test_log(self):
        self.queue.log.info("HAAAALP ME")

    def test_put(self):
        with mock.patch(
            'paasta_tools.deployd.common.PriorityQueue.put', autospec=True,
        ) as mock_q_put:
            self.queue.put(3, "human")
            mock_q_put.assert_called_with(self.queue, (3, 1, "human"))

            self.queue.put(3, "human")
            mock_q_put.assert_called_with(self.queue, (3, 2, "human"))

    def test_get(self):
        with mock.patch(
            'paasta_tools.deployd.common.PriorityQueue.get', autospec=True,
        ) as mock_q_get:
            mock_q_get.return_value = (3, 2, "human")
            assert self.queue.get() == "human"


class TestServiceInstance(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'paasta_tools.deployd.common.get_priority', autospec=True,
        ) as mock_get_priority:
            mock_get_priority.return_value = 1
            # https://github.com/python/mypy/issues/2852
            self.service_instance = ServiceInstance(  # type: ignore
                service='universe',
                instance='c137',
                watcher='mywatcher',
                cluster='westeros-prod',
                bounce_by=0,
            )

    def test___new__(self):
        with mock.patch(
            'paasta_tools.deployd.common.get_priority', autospec=True,
        ):
            expected = BaseServiceInstance(
                service='universe',
                instance='c137',
                watcher='mywatcher',
                bounce_by=0,
                failures=0,
                bounce_timers=None,
                priority=1,
            )
            assert self.service_instance == expected

            expected = BaseServiceInstance(
                service='universe',
                instance='c137',
                watcher='mywatcher',
                bounce_by=0,
                failures=0,
                bounce_timers=None,
                priority=2,
            )
            # https://github.com/python/mypy/issues/2852
            assert ServiceInstance(  # type: ignore
                service='universe',
                instance='c137',
                watcher='mywatcher',
                cluster='westeros-prod',
                bounce_by=0,
                priority=2,
            ) == expected

    def test_get_priority(self):
        with mock.patch(
            'paasta_tools.deployd.common.load_marathon_service_config', autospec=True,
        ) as mock_load_marathon_service_config:
            mock_load_marathon_service_config.return_value = mock.Mock(get_bounce_priority=mock.Mock(return_value=1))
            assert get_priority('universe', 'c137', 'westeros-prod') == 1
            mock_load_marathon_service_config.assert_called_with(
                service='universe',
                instance='c137',
                cluster='westeros-prod',
                soa_dir='/nail/etc/services',
            )

            mock_load_marathon_service_config.side_effect = NoDockerImageError()
            assert get_priority('universe', 'c137', 'westeros-prod') == 0

            mock_load_marathon_service_config.side_effect = InvalidJobNameError()
            assert get_priority('universe', 'c137', 'westeros-prod') == 0

            mock_load_marathon_service_config.side_effect = NoDeploymentsAvailable()
            assert get_priority('universe', 'c137', 'westeros-prod') == 0


def test_rate_limit_instances():
    with mock.patch(
        'paasta_tools.deployd.common.get_priority', autospec=True, return_value=0,
    ), mock.patch(
        'time.time', autospec=True,
    ) as mock_time:
        mock_time.return_value = 1
        mock_si_1 = ('universe', 'c137')
        mock_si_2 = ('universe', 'c138')
        ret = rate_limit_instances([mock_si_1, mock_si_2], "westeros-prod", 2, "Custos")
        expected = [
            BaseServiceInstance(
                service='universe',
                instance='c137',
                watcher='Custos',
                priority=0,
                bounce_by=1,
                bounce_timers=None,
                failures=0,
            ),
            BaseServiceInstance(
                service='universe',
                instance='c138',
                watcher='Custos',
                priority=0,
                bounce_by=31,
                bounce_timers=None,
                failures=0,
            ),
        ]
        assert ret == expected

        ret = rate_limit_instances([mock_si_1, mock_si_2], "westeros-prod", 2, "Custos", priority=99)
        expected = [
            BaseServiceInstance(
                service='universe',
                instance='c137',
                watcher='Custos',
                priority=99,
                bounce_by=1,
                bounce_timers=None,
                failures=0,
            ),
            BaseServiceInstance(
                service='universe',
                instance='c138',
                watcher='Custos',
                priority=99,
                bounce_by=31,
                bounce_timers=None,
                failures=0,
            ),
        ]
        assert ret == expected


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
