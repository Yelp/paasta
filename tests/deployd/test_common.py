from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

import mock

from paasta_tools.deployd.common import exponential_back_off
from paasta_tools.deployd.common import get_marathon_client_from_config
from paasta_tools.deployd.common import get_service_instances_needing_update
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.utils import DEFAULT_SOA_DIR
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
            'paasta_tools.deployd.common.Queue', autospec=True
        ) as mock_q:
            self.queue.put("human")
            mock_q.put.assert_called_with(self.queue, "human")


def test_rate_limit_instances():
    with mock.patch(
        'time.time', autospec=True
    ) as mock_time:
        mock_time.return_value = 1
        mock_si_1 = ('universe', 'c137')
        mock_si_2 = ('universe', 'c138')
        ret = rate_limit_instances([mock_si_1, mock_si_2], 2, "Custos")
        expected = [ServiceInstance(service='universe',
                                    instance='c137',
                                    watcher='Custos',
                                    bounce_by=1,
                                    bounce_timers=None,
                                    failures=0),
                    ServiceInstance(service='universe',
                                    instance='c138',
                                    watcher='Custos',
                                    bounce_by=31,
                                    bounce_timers=None,
                                    failures=0)]
        assert ret == expected


def test_exponential_back_off():
    assert exponential_back_off(0, 60, 2, 6000) == 60
    assert exponential_back_off(2, 60, 2, 6000) == 240
    assert exponential_back_off(99, 60, 2, 6000) == 6000


def test_get_service_instances_needing_update():
    with mock.patch(
        'paasta_tools.deployd.common.get_all_marathon_apps', autospec=True
    ) as mock_get_marathon_apps, mock.patch(
        'paasta_tools.deployd.common.load_marathon_service_config_no_cache', autospec=True
    ) as mock_load_marathon_service_config:
        mock_marathon_apps = [mock.Mock(id='/universe.c137.c1.g1', instances=2),
                              mock.Mock(id='/universe.c138.c1.g1', instances=2)]
        mock_get_marathon_apps.return_value = mock_marathon_apps
        mock_service_instances = [('universe', 'c137'), ('universe', 'c138')]
        mock_configs = [mock.Mock(format_marathon_app_dict=mock.Mock(return_value={'id': 'universe.c137.c1.g1',
                                                                                   'instances': 2})),
                        mock.Mock(format_marathon_app_dict=mock.Mock(return_value={'id': 'universe.c138.c2.g2',
                                                                                   'instances': 2}))]
        mock_load_marathon_service_config.side_effect = mock_configs
        ret = get_service_instances_needing_update(mock.Mock(), mock_service_instances, 'westeros-prod')
        assert mock_get_marathon_apps.called
        calls = [mock.call(service='universe',
                           instance='c137',
                           cluster='westeros-prod',
                           soa_dir=DEFAULT_SOA_DIR),
                 mock.call(service='universe',
                           instance='c138',
                           cluster='westeros-prod',
                           soa_dir=DEFAULT_SOA_DIR)]
        mock_load_marathon_service_config.assert_has_calls(calls)
        assert ret == [('universe', 'c138')]

        mock_configs = [mock.Mock(format_marathon_app_dict=mock.Mock(return_value={'id': 'universe.c137.c1.g1',
                                                                                   'instances': 3})),
                        mock.Mock(format_marathon_app_dict=mock.Mock(return_value={'id': 'universe.c138.c2.g2',
                                                                                   'instances': 2}))]
        mock_load_marathon_service_config.side_effect = mock_configs
        ret = get_service_instances_needing_update(mock.Mock(), mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c137'), ('universe', 'c138')]

        mock_configs = [mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=NoDockerImageError)),
                        mock.Mock(format_marathon_app_dict=mock.Mock(return_value={'id': 'universe.c138.c2.g2',
                                                                                         'instances': 2}))]
        mock_load_marathon_service_config.side_effect = mock_configs
        ret = get_service_instances_needing_update(mock.Mock(), mock_service_instances, 'westeros-prod')
        assert ret == [('universe', 'c137'), ('universe', 'c138')]


def test_get_marathon_client_from_config():
    with mock.patch(
        'paasta_tools.deployd.common.load_marathon_config', autospec=True
    ), mock.patch(
        'paasta_tools.deployd.common.get_marathon_client', autospec=True
    ) as mock_marathon_client:
        assert get_marathon_client_from_config() == mock_marathon_client.return_value
