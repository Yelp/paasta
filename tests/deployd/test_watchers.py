from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import unittest

import mock
from pytest import raises

from paasta_tools.deployd.common import ServiceInstance


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
from paasta_tools.deployd.watchers import FileWatcher  # noqa
from paasta_tools.deployd.watchers import YelpSoaEventHandler  # noqa
from paasta_tools.deployd.watchers import AutoscalerWatcher  # noqa


class TestPaastaWatcher(unittest.TestCase):
    def test_init(self):
        mock_inbox_q = mock.Mock()
        PaastaWatcher(mock_inbox_q, 'westeros-prod')


class TestAutoscalerWatcher(unittest.TestCase):
    def setUp(self):
        self.mock_zk = mock.Mock()
        self.mock_inbox_q = mock.Mock()
        self.watcher = AutoscalerWatcher(self.mock_inbox_q, "westeros-prod", zookeeper_client=self.mock_zk)

    def test_watch_folder(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.ChildrenWatch', autospec=True
        ) as mock_children_watch, mock.patch(
            'paasta_tools.deployd.watchers.AutoscalerWatcher.watch_node', autospec=True
        ) as mock_watch_node:
            self.watcher.watch_folder('/path/autoscaling.lock')
            assert not mock_children_watch.called

            mock_watcher = mock.Mock(_prior_children=[])
            mock_children_watch.return_value = mock_watcher
            self.watcher.watch_folder('/rick/beth')
            mock_children_watch.assert_called_with(self.mock_zk,
                                                   '/rick/beth',
                                                   func=self.watcher.process_folder_event,
                                                   send_event=True)
            assert not mock_watch_node.called

            mock_children = mock.PropertyMock(side_effect=[['morty', 'summer'], [], []])
            mock_watcher = mock.Mock()
            type(mock_watcher)._prior_children = mock_children
            mock_children_watch.return_value = mock_watcher
            self.watcher.watch_folder('/rick/beth')
            assert not mock_watch_node.called
            calls = [mock.call(self.mock_zk,
                               '/rick/beth',
                               func=self.watcher.process_folder_event,
                               send_event=True),
                     mock.call(self.mock_zk,
                               '/rick/beth/morty',
                               func=self.watcher.process_folder_event,
                               send_event=True),
                     mock.call(self.mock_zk,
                               '/rick/beth/summer',
                               func=self.watcher.process_folder_event,
                               send_event=True)]
            mock_children_watch.assert_has_calls(calls)

            mock_watcher = mock.Mock(_prior_children=['instances'])
            mock_children_watch.return_value = mock_watcher
            self.watcher.watch_folder('/rick/beth')
            mock_watch_node.assert_called_with(self.watcher, '/rick/beth/instances')

    def test_watch_node(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.DataWatch', autospec=True
        ) as mock_data_watch:
            self.watcher.watch_node('/some/node')
            mock_data_watch.assert_called_with(self.mock_zk,
                                               '/some/node',
                                               func=self.watcher.process_node_event,
                                               send_event=True)

    def test_process_node_event(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.EventType', autospec=True
        ) as mock_event_type, mock.patch(
            'time.time', autospec=True, return_value=1
        ):
            mock_event_other = mock_event_type.DELETED
            mock_event = mock.Mock(type=mock_event_other,
                                   path='/autoscaling/service/instance/instances')
            assert not self.mock_inbox_q.put.called

            mock_event_created = mock_event_type.CREATED
            mock_event = mock.Mock(type=mock_event_created,
                                   path='/autoscaling/service/instance/instances')
            self.watcher.process_node_event(mock.Mock(), mock.Mock(), mock_event)
            self.mock_inbox_q.put.assert_called_with(ServiceInstance(service='service',
                                                                     instance='instance',
                                                                     bounce_by=1,
                                                                     bounce_timers=None,
                                                                     watcher=self.watcher.__class__.__name__))

            mock_event_changed = mock_event_type.CHANGED
            mock_event = mock.Mock(type=mock_event_changed,
                                   path='/autoscaling/service/instance/instances')
            self.watcher.process_node_event(mock.Mock(), mock.Mock(), mock_event)
            self.mock_inbox_q.put.assert_called_with(ServiceInstance(service='service',
                                                                     instance='instance',
                                                                     bounce_by=1,
                                                                     bounce_timers=None,
                                                                     watcher=self.watcher.__class__.__name__))

    def test_process_folder_event(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.EventType', autospec=True
        ) as mock_event_type, mock.patch(
            'paasta_tools.deployd.watchers.AutoscalerWatcher.watch_folder', autospec=True
        ) as mock_watch_folder:
            mock_event_other = mock_event_type.DELETED
            mock_event = mock.Mock(type=mock_event_other,
                                   path='/autoscaling/service/instance')
            self.watcher.process_folder_event([], mock_event)
            assert not mock_watch_folder.called

            mock_event_child = mock_event_type.CHILD
            mock_event = mock.Mock(type=mock_event_child,
                                   path='/rick/beth')
            self.watcher.process_folder_event(['morty', 'summer'], mock_event)
            calls = [mock.call(self.watcher, '/rick/beth/morty'),
                     mock.call(self.watcher, '/rick/beth/summer')]
            mock_watch_folder.assert_has_calls(calls)

    def test_run(self):
        with mock.patch(
            'time.sleep', autospec=True, side_effect=LoopBreak
        ), mock.patch(
            'paasta_tools.deployd.watchers.AutoscalerWatcher.watch_folder', autospec=True
        ) as mock_watch_folder:
            assert not self.watcher.is_ready
            with raises(LoopBreak):
                self.watcher.run()
            assert self.watcher.is_ready
            mock_watch_folder.assert_called_with(self.watcher, '/autoscaling')


class LoopBreak(Exception):
    pass


class TestFileWatcher(unittest.TestCase):
    def setUp(self):
        mock_inbox_q = mock.Mock()
        with mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.WatchManager', autospec=True
        ), mock.patch(
            'paasta_tools.deployd.watchers.YelpSoaEventHandler', autospec=True
        ), mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.Notifier', autospec=True
        ) as mock_notifier_class, mock.patch(
            'paasta_tools.deployd.watchers.FileWatcher.mask', autospec=True
        ):
            self.mock_notifier = mock.Mock()
            mock_notifier_class.return_value = self.mock_notifier
            self.watcher = FileWatcher(mock_inbox_q, 'westeros-prod')
            assert mock_notifier_class.called

    def test_mask(self):
        with mock.patch(
            'paasta_tools.deployd.watchers.pyinotify.EventsCodes', autospec=True
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


class TestYelpSoaEventHandler(unittest.TestCase):
    def setUp(self):
        self.handler = YelpSoaEventHandler()
        self.mock_filewatcher = mock.Mock()
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
        mock_event = mock.Mock(maskname='MAJORAS')
        self.handler.watch_new_folder(mock_event)
        assert not self.mock_filewatcher.wm.add_watch.called
        mock_event = mock.Mock(maskname='IN_CREATE|IN_ISDIR')
        self.handler.watch_new_folder(mock_event)
        assert self.mock_filewatcher.wm.add_watch.called

    def test_process_default(self):
        mock_event = mock.Mock(path='/folder/universe')
        with mock.patch(
            'paasta_tools.deployd.watchers.list_all_instances_for_service', autospec=True
        ) as mock_list_instances, mock.patch(
            'time.time', autospec=True, return_value=1
        ):
            mock_list_instances.return_value = ['c137']
            self.handler.process_default(mock_event)
            mock_list_instances.assert_called_with(service='universe',
                                                   clusters=[self.handler.filewatcher.cluster],
                                                   instance_type='marathon')
            expected_si = ServiceInstance(service='universe',
                                          instance='c137',
                                          bounce_by=1,
                                          watcher='YelpSoaEventHandler',
                                          bounce_timers=None)
            self.mock_filewatcher.inbox_q.put.assert_called_with(expected_si)
