import sys
import unittest

import mock
from pytest import raises
from requests.exceptions import RequestException

from paasta_tools.deployd.common import BaseServiceInstance


class FakePyinotify(object):  # pragma: no cover
    class ProcessEvent():
        pass

    @property
    def WatchManager():
        pass

    @property
    def EventsCodes():
        pass

    @property
    def Notifier():
        pass


# This module is only available on linux
# and we will be mocking it in the unit tests anyway
# so this just creates it as a dummy module to prevent
# the ImportError
sys.modules['pyinotify'] = FakePyinotify
from paasta_tools.deployd.watchers import PaastaWatcher  # noqa
from paasta_tools.deployd.watchers import SoaFileWatcher  # noqa
from paasta_tools.deployd.watchers import YelpSoaEventHandler  # noqa
from paasta_tools.deployd.watchers import AutoscalerWatcher  # noqa
from paasta_tools.deployd.watchers import PublicConfigFileWatcher  # noqa
from paasta_tools.deployd.watchers import PublicConfigEventHandler  # noqa
from paasta_tools.deployd.watchers import get_service_instances_needing_update  # noqa
from paasta_tools.deployd.watchers import get_marathon_clients_from_config  # noqa
from paasta_tools.deployd.watchers import MaintenanceWatcher  # noqa


class TestPaastaWatcher(unittest.TestCase):
    def test_init(self):
        mock_inbox_q = mock.Mock()
        PaastaWatcher(mock_inbox_q, 'westeros-prod', config=mock.Mock())


class TestAutoscalerWatcher(unittest.TestCase):
    def setUp(self):
        self.mock_zk = mock.Mock()
        self.mock_inbox_q = mock.Mock()
        self.watcher = AutoscalerWatcher(
            self.mock_inbox_q,
            "westeros-prod",
            zookeeper_client=self.mock_zk,
            config=mock.Mock(),
        )

    def test_watch_folder(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.ChildrenWatch', autospec=True,
        ) as mock_children_watch, mock.patch(
            'paasta_tools.deployd.watchers.AutoscalerWatcher.watch_node', autospec=True,
        ) as mock_watch_node:
            self.watcher.watch_folder('/path/autoscaling.lock')
            assert not mock_children_watch.called

            mock_watcher = mock.Mock(_prior_children=[])
            mock_children_watch.return_value = mock_watcher
            self.watcher.watch_folder('/rick/beth')
            mock_children_watch.assert_called_with(
                self.mock_zk,
                '/rick/beth',
                func=self.watcher.process_folder_event,
                send_event=True,
            )
            assert not mock_watch_node.called

            mock_children = mock.PropertyMock(side_effect=[['morty', 'summer'], [], []])
            mock_watcher = mock.Mock()
            type(mock_watcher)._prior_children = mock_children
            mock_children_watch.return_value = mock_watcher
            self.watcher.watch_folder('/rick/beth')
            assert not mock_watch_node.called
            calls = [
                mock.call(
                    self.mock_zk,
                    '/rick/beth',
                    func=self.watcher.process_folder_event,
                    send_event=True,
                ),
                mock.call(
                    self.mock_zk,
                    '/rick/beth/morty',
                    func=self.watcher.process_folder_event,
                    send_event=True,
                ),
                mock.call(
                    self.mock_zk,
                    '/rick/beth/summer',
                    func=self.watcher.process_folder_event,
                    send_event=True,
                ),
            ]
            mock_children_watch.assert_has_calls(calls)

            mock_watcher = mock.Mock(_prior_children=['instances'])
            mock_children_watch.return_value = mock_watcher
            self.watcher.watch_folder('/rick/beth')
            mock_watch_node.assert_called_with(self.watcher, '/rick/beth/instances')

    def test_watch_node(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.DataWatch', autospec=True,
        ) as mock_data_watch:
            self.watcher.watch_node('/some/node')
            mock_data_watch.assert_called_with(
                self.mock_zk,
                '/some/node',
                func=self.watcher.process_node_event,
                send_event=True,
            )

    def test_process_node_event(self):
        with mock.patch(
            'paasta_tools.deployd.common.ServiceInstance.get_priority', autospec=True, return_value=0,
        ), mock.patch(
            'paasta_tools.deployd.watchers.EventType', autospec=True,
        ) as mock_event_type, mock.patch(
            'time.time', autospec=True, return_value=1,
        ):
            mock_event_other = mock_event_type.DELETED
            mock_event = mock.Mock(
                type=mock_event_other,
                path='/autoscaling/service/instance/instances',
            )
            assert not self.mock_inbox_q.put.called

            mock_event_created = mock_event_type.CREATED
            mock_event = mock.Mock(
                type=mock_event_created,
                path='/autoscaling/service/instance/instances',
            )
            self.watcher.process_node_event(mock.Mock(), mock.Mock(), mock_event)
            self.mock_inbox_q.put.assert_called_with(BaseServiceInstance(
                service='service',
                instance='instance',
                bounce_by=1,
                bounce_timers=None,
                watcher=self.watcher.__class__.__name__,
                priority=0,
                failures=0,
            ))

            mock_event_changed = mock_event_type.CHANGED
            mock_event = mock.Mock(
                type=mock_event_changed,
                path='/autoscaling/service/instance/instances',
            )
            self.watcher.process_node_event(mock.Mock(), mock.Mock(), mock_event)
            self.mock_inbox_q.put.assert_called_with(BaseServiceInstance(
                service='service',
                instance='instance',
                bounce_by=1,
                bounce_timers=None,
                watcher=self.watcher.__class__.__name__,
                priority=0,
                failures=0,
            ))

    def test_process_folder_event(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.EventType', autospec=True,
        ) as mock_event_type, mock.patch(
            'paasta_tools.deployd.watchers.AutoscalerWatcher.watch_folder', autospec=True,
        ) as mock_watch_folder:
            mock_event_other = mock_event_type.DELETED
            mock_event = mock.Mock(
                type=mock_event_other,
                path='/autoscaling/service/instance',
            )
            self.watcher.process_folder_event([], mock_event)
            assert not mock_watch_folder.called

            mock_event_child = mock_event_type.CHILD
            mock_event = mock.Mock(
                type=mock_event_child,
                path='/rick/beth',
            )
            self.watcher.process_folder_event(['morty', 'summer'], mock_event)
            calls = [
                mock.call(self.watcher, '/rick/beth/morty'),
                mock.call(self.watcher, '/rick/beth/summer'),
            ]
            mock_watch_folder.assert_has_calls(calls)

    def test_run(self):
        with mock.patch(
            'time.sleep', autospec=True, side_effect=LoopBreak,
        ), mock.patch(
            'paasta_tools.deployd.watchers.AutoscalerWatcher.watch_folder', autospec=True,
        ) as mock_watch_folder:
            assert not self.watcher.is_ready
            with raises(LoopBreak):
                self.watcher.run()
            assert self.watcher.is_ready
            mock_watch_folder.assert_called_with(self.watcher, '/autoscaling')


class LoopBreak(Exception):
    pass


class TestSoaFileWatcher(unittest.TestCase):
    def setUp(self):
        mock_inbox_q = mock.Mock()
        with mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.WatchManager', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.watchers.YelpSoaEventHandler', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.Notifier', autospec=True,
        ) as mock_notifier_class, mock.patch(
            'paasta_tools.deployd.watchers.SoaFileWatcher.mask', autospec=True,
        ):
            self.mock_notifier = mock.Mock()
            mock_notifier_class.return_value = self.mock_notifier
            self.watcher = SoaFileWatcher(mock_inbox_q, 'westeros-prod', config=mock.Mock())
            assert mock_notifier_class.called

    def test_mask(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.EventsCodes', autospec=True,
        ) as mock_event_codes:
            mock_event_codes.OP_FLAGS = {'UNION_JACK': 1, 'STARS_AND_STRIPES': 2, 'IN_OPEN': 4}
            assert self.watcher.mask == 3

    def test_run(self):
        self.watcher.run()
        self.mock_notifier.loop.assert_called_with(callback=self.watcher.startup_checker)

    def test_startup_checker(self):
        assert not self.watcher.is_ready
        self.watcher.startup_checker(mock.Mock())
        assert self.watcher.is_ready


class TestPublicConfigWatcher(unittest.TestCase):
    def setUp(self):
        mock_inbox_q = mock.Mock()
        with mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.WatchManager', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.watchers.PublicConfigEventHandler', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.Notifier', autospec=True,
        ) as mock_notifier_class, mock.patch(
            'paasta_tools.deployd.watchers.PublicConfigFileWatcher.mask', autospec=True,
        ):
            self.mock_notifier = mock.Mock()
            mock_notifier_class.return_value = self.mock_notifier
            self.watcher = PublicConfigFileWatcher(mock_inbox_q, 'westeros-prod', config=mock.Mock())
            assert mock_notifier_class.called

    def test_mask(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.EventsCodes', autospec=True,
        ) as mock_event_codes:
            mock_event_codes.OP_FLAGS = {'UNION_JACK': 1, 'STARS_AND_STRIPES': 2, 'IN_OPEN': 4}
            assert self.watcher.mask == 3

    def test_run(self):
        self.watcher.run()
        self.mock_notifier.loop.assert_called_with(callback=self.watcher.startup_checker)

    def test_startup_checker(self):
        assert not self.watcher.is_ready
        self.watcher.startup_checker(mock.Mock())
        assert self.watcher.is_ready


class TestMaintenanceWatcher(unittest.TestCase):
    def setUp(self):
        self.mock_inbox_q = mock.Mock()
        self.mock_marathon_client = mock.Mock()
        mock_config = mock.Mock(get_deployd_maintenance_polling_frequency=mock.Mock(return_value=20))
        with mock.patch(
            'paasta_tools.deployd.watchers.get_marathon_clients_from_config', autospec=True,
        ):
            self.watcher = MaintenanceWatcher(self.mock_inbox_q, "westeros-prod", config=mock_config)

    def test_get_new_draining_hosts(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.get_draining_hosts', autospec=True,
        ) as mock_get_draining_hosts:

            mock_get_draining_hosts.return_value = ['host1', 'host2']
            assert self.watcher.get_new_draining_hosts() == ['host1', 'host2']
            assert self.watcher.draining == {'host1', 'host2'}

            mock_get_draining_hosts.return_value = ['host1']
            assert self.watcher.get_new_draining_hosts() == []
            assert self.watcher.draining == {'host1'}

            mock_get_draining_hosts.side_effect = RequestException
            assert self.watcher.get_new_draining_hosts() == []
            assert self.watcher.draining == {'host1'}

            mock_get_draining_hosts.side_effect = None
            mock_get_draining_hosts.return_value = ['host3', 'host1']
            assert self.watcher.get_new_draining_hosts() == ['host3']
            assert self.watcher.draining == {'host1', 'host3'}

            mock_get_draining_hosts.return_value = []
            assert self.watcher.get_new_draining_hosts() == []
            assert self.watcher.draining == set()

    def test_run(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.MaintenanceWatcher.get_new_draining_hosts', autospec=True,
        ) as mock_get_new_draining_hosts, mock.patch(
            'paasta_tools.deployd.watchers.MaintenanceWatcher.get_at_risk_service_instances', autospec=True,
        ) as mock_get_at_risk_service_instances, mock.patch(
            'time.sleep', autospec=True, side_effect=LoopBreak,
        ):
            mock_get_new_draining_hosts.return_value = []
            assert not self.watcher.is_ready
            with raises(LoopBreak):
                self.watcher.run()
            assert self.watcher.is_ready
            assert not mock_get_at_risk_service_instances.called

            mock_get_new_draining_hosts.return_value = ['host1', 'host2']
            mock_get_at_risk_service_instances.return_value = ['si1', 'si2']
            with raises(LoopBreak):
                self.watcher.run()
            mock_get_at_risk_service_instances.assert_called_with(self.watcher, ['host1', 'host2'])
            calls = [
                mock.call('si1'),
                mock.call('si2'),
            ]
            self.mock_inbox_q.put.assert_has_calls(calls)

    def test_get_at_risk_service_instances(self):
        with mock.patch(
            'paasta_tools.deployd.common.ServiceInstance.get_priority', autospec=True, return_value=0,
        ), mock.patch(
            'paasta_tools.deployd.watchers.get_marathon_apps_with_clients', autospec=True,
        ) as mock_get_marathon_apps, mock.patch(
            'time.time', autospec=True, return_value=1,
        ):
            mock_marathon_apps = [
                mock.Mock(tasks=[
                    mock.Mock(
                        host='host1',
                        app_id='/universe.c137.configsha.gitsha',
                    ),
                    mock.Mock(
                        host='host2',
                        app_id='/universe.c138.configsha.gitsha',
                    ),
                ]),
                mock.Mock(tasks=[mock.Mock(
                    host='host1',
                    app_id='/universe.c139.configsha.gitsha',
                )]),
                mock.Mock(tasks=[mock.Mock(
                    host='host1',
                    app_id='/universe.c139.configsha.gitsha',
                )]),
            ]
            mock_client = mock.Mock()
            mock_get_marathon_apps.return_value = [(app, mock_client) for app in mock_marathon_apps]
            ret = self.watcher.get_at_risk_service_instances(['host1'])
            expected = [
                BaseServiceInstance(
                    service='universe',
                    instance='c137',
                    bounce_by=1,
                    watcher=self.watcher.__class__.__name__,
                    priority=0,
                    bounce_timers=None,
                    failures=0,
                ),
                BaseServiceInstance(
                    service='universe',
                    instance='c139',
                    bounce_by=1,
                    watcher=self.watcher.__class__.__name__,
                    priority=0,
                    bounce_timers=None,
                    failures=0,
                ),
            ]
            assert ret == expected


class TestPublicConfigEventHandler(unittest.TestCase):
    def setUp(self):
        self.handler = PublicConfigEventHandler()
        self.mock_filewatcher = mock.Mock()
        self.mock_config = mock.Mock(get_cluster=mock.Mock())
        with mock.patch(
            'paasta_tools.deployd.watchers.load_system_paasta_config', autospec=True, return_value=self.mock_config,
        ), mock.patch(
            'paasta_tools.deployd.watchers.get_marathon_clients_from_config', autospec=True,
        ):
            self.handler.my_init(self.mock_filewatcher)

    def test_log(self):
        self.handler.log.info('WHAAAAAT')

    def test_filter_event(self):
        mock_event = mock.Mock()
        name = mock.PropertyMock(return_value='deployd.json')
        type(mock_event).name = name
        assert mock_event == self.handler.filter_event(mock_event)

        mock_event = mock.Mock(maskname='MAJORAS')
        name = mock.PropertyMock(return_value='another.file')
        type(mock_event).name = name
        assert self.handler.filter_event(mock_event) is None

        mock_event = mock.Mock(maskname='IN_CREATE|IN_ISDIR', pathname='/foo/bar')
        name = mock.PropertyMock(return_value='another.file')
        type(mock_event).name = name
        assert mock_event == self.handler.filter_event(mock_event)

    def test_watch_new_folder(self):
        mock_event = mock.Mock(maskname='MAJORAS')
        self.handler.watch_new_folder(mock_event)
        assert not self.mock_filewatcher.wm.add_watch.called
        mock_event = mock.Mock(maskname='IN_CREATE|IN_ISDIR', pathname='/foo/')
        self.handler.watch_new_folder(mock_event)
        assert self.mock_filewatcher.wm.add_watch.called

    def test_process_default(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.PublicConfigEventHandler.filter_event', autospec=True,
        ) as mock_filter_event, mock.patch(
            'paasta_tools.deployd.watchers.PublicConfigEventHandler.watch_new_folder', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.watchers.get_services_for_cluster', autospec=True,
        ) as mock_get_services_for_cluster, mock.patch(
            'paasta_tools.deployd.watchers.load_system_paasta_config', autospec=True,
        ) as mock_load_system_config, mock.patch(
            'paasta_tools.deployd.watchers.get_service_instances_needing_update',
            autospec=True,
        ) as mock_get_service_instances_needing_update, mock.patch(
            'paasta_tools.deployd.watchers.rate_limit_instances', autospec=True,
        ) as mock_rate_limit_instances:
            mock_event = mock.Mock()
            mock_filter_event.return_value = mock_event
            mock_load_system_config.return_value = self.mock_config
            self.handler.process_default(mock_event)
            assert mock_load_system_config.called
            assert not mock_get_services_for_cluster.called
            assert not mock_get_service_instances_needing_update.called
            assert not mock_rate_limit_instances.called
            assert not self.mock_filewatcher.inbox_q.put.called

            mock_load_system_config.return_value = mock.Mock(get_cluster=mock.Mock())
            mock_get_service_instances_needing_update.return_value = []
            self.handler.process_default(mock_event)
            assert mock_load_system_config.called
            assert mock_get_services_for_cluster.called
            assert mock_get_service_instances_needing_update.called
            assert not mock_rate_limit_instances.called
            assert not self.mock_filewatcher.inbox_q.put.called

            mock_load_system_config.return_value = mock.Mock(get_deployd_big_bounce_rate=mock.Mock())
            mock_si = mock.Mock()
            mock_get_service_instances_needing_update.return_value = [mock_si]
            mock_rate_limit_instances.return_value = [mock_si]
            self.handler.process_default(mock_event)
            assert mock_load_system_config.called
            assert mock_get_services_for_cluster.called
            assert mock_get_service_instances_needing_update.called
            assert mock_rate_limit_instances.called
            self.mock_filewatcher.inbox_q.put.assert_called_with(mock_si)


class TestYelpSoaEventHandler(unittest.TestCase):
    def setUp(self):
        self.handler = YelpSoaEventHandler()
        self.mock_filewatcher = mock.Mock()
        with mock.patch(
            'paasta_tools.deployd.watchers.get_marathon_clients_from_config', autospec=True,
        ):
            self.handler.my_init(self.mock_filewatcher)

    def test_log(self):
        self.handler.log.info('WHAAAAAT')

    def test_filter_event(self):
        mock_event = mock.Mock()
        name = mock.PropertyMock(return_value='marathon-cluster.yaml')
        type(mock_event).name = name
        assert mock_event == self.handler.filter_event(mock_event)
        name = mock.PropertyMock(return_value='deployments.json')
        type(mock_event).name = name
        assert mock_event == self.handler.filter_event(mock_event)
        name = mock.PropertyMock(return_value='another.file')
        type(mock_event).name = name
        assert self.handler.filter_event(mock_event) is None

    def test_watch_new_folder(self):
        with mock.patch(
            'os.listdir', autospec=True,
        ) as mock_os_list, mock.patch(
            'paasta_tools.deployd.watchers.YelpSoaEventHandler.bounce_service', autospec=True,
        ) as mock_bounce_service:
            mock_os_list.return_value = ["some.file", "some_other.file"]
            mock_event = mock.Mock(maskname='MAJORAS', pathname='/some/path')
            self.handler.watch_new_folder(mock_event)
            assert not self.mock_filewatcher.wm.add_watch.called

            mock_event = mock.Mock(maskname='IN_CREATE|IN_ISDIR', pathname='/foo')
            name = mock.PropertyMock(return_value='universe')
            type(mock_event).name = name
            self.handler.watch_new_folder(mock_event)
            assert self.mock_filewatcher.wm.add_watch.called
            assert not mock_bounce_service.called

            mock_os_list.return_value = ["some.file", "marathon-cluster.yaml"]
            self.handler.watch_new_folder(mock_event)
            assert self.mock_filewatcher.wm.add_watch.called
            mock_bounce_service.assert_called_with(self.handler, 'universe')

            mock_os_list.side_effect = OSError
            mock_bounce_service.reset_mock()
            self.handler.watch_new_folder(mock_event)
            assert self.mock_filewatcher.wm.add_watch.called
            assert not mock_bounce_service.called

    def test_process_default(self):
        mock_event = mock.Mock(path='/folder/universe')
        with mock.patch(
            'paasta_tools.deployd.watchers.YelpSoaEventHandler.bounce_service', autospec=True,
        ) as mock_bounce_service, mock.patch(
            'paasta_tools.deployd.watchers.YelpSoaEventHandler.watch_new_folder', autospec=True,
        ) as mock_watch_folder, mock.patch(
            'paasta_tools.deployd.watchers.YelpSoaEventHandler.filter_event', autospec=True,
        ) as mock_filter_event:
            mock_filter_event.return_value = mock_event
            self.handler.process_default(mock_event)
            mock_watch_folder.assert_called_with(self.handler, mock_event)
            mock_filter_event.assert_called_with(self.handler, mock_event)
            mock_bounce_service.assert_called_with(self.handler, 'universe')

    def test_bounce_service(self):
        with mock.patch(
            'paasta_tools.deployd.common.ServiceInstance.get_priority', autospec=True, return_value=0,
        ), mock.patch(
            'paasta_tools.deployd.watchers.list_all_instances_for_service', autospec=True,
        ) as mock_list_instances, mock.patch(
            'paasta_tools.deployd.watchers.get_service_instances_needing_update', autospec=True,
        ) as mock_get_service_instances_needing_update, mock.patch(
            'time.time', autospec=True, return_value=1,
        ):
            mock_list_instances.return_value = ['c137', 'c138']
            mock_get_service_instances_needing_update.return_value = [('universe', 'c137')]
            self.handler.bounce_service('universe')
            mock_list_instances.assert_called_with(
                service='universe',
                clusters=[self.handler.filewatcher.cluster],
                instance_type='marathon',
                cache=False,
            )
            mock_get_service_instances_needing_update.assert_called_with(
                self.handler.marathon_clients,
                [
                    ('universe', 'c137'),
                    ('universe', 'c138'),
                ],
                self.handler.filewatcher.cluster,
            )
            expected_si = BaseServiceInstance(
                service='universe',
                instance='c137',
                bounce_by=1,
                watcher='YelpSoaEventHandler',
                bounce_timers=None,
                priority=0,
                failures=0,
            )
            self.mock_filewatcher.inbox_q.put.assert_called_with(expected_si)
            assert self.mock_filewatcher.inbox_q.put.call_count == 1
