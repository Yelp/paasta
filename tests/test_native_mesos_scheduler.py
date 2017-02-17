from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

import mock
import pytest
from mesos.interface import mesos_pb2

from paasta_tools import long_running_service_tools
from paasta_tools import native_mesos_scheduler
from paasta_tools import utils
from paasta_tools.native_mesos_scheduler import TASK_KILLED
from paasta_tools.native_mesos_scheduler import TASK_RUNNING


def test_main():
    with contextlib.nested(
        mock.patch('paasta_tools.native_mesos_scheduler.get_paasta_native_jobs_for_cluster',
                   return_value=[('service1', 'instance1'), ('service2', 'instance2')],
                   autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.create_driver', autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.sleep', autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.load_system_paasta_config', autospec=True),
        mock.patch('paasta_tools.native_mesos_scheduler.PaastaScheduler', autospec=True),
    ):
        native_mesos_scheduler.main(["--stay-alive-seconds=0"])


@pytest.fixture
def system_paasta_config():
    return utils.SystemPaastaConfig({
        "docker_registry": "fake",
        "volumes": [],
    }, "/fake/system/configs")


def make_fake_offer(cpu=50000, mem=50000, port_begin=31000, port_end=32000):
    offer = mock.Mock(
        resources=[
            mock.Mock(scalar=mock.Mock(value=cpu)),
            mock.Mock(scalar=mock.Mock(value=mem)),
            mock.Mock(ranges=mock.Mock(range=[mock.Mock(begin=port_begin, end=port_end)])),
        ],
        slave_id=mock.Mock(value="super big slave"),
    )

    offer.resources[0].name = "cpus"
    offer.resources[1].name = "mem"
    offer.resources[2].name = "ports"

    return offer


class TestPaastaScheduler(object):
    def test_start_upgrade_rollback_scaledown(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = []
        for force_bounce in xrange(2):
            service_configs.append(native_mesos_scheduler.PaastaNativeServiceConfig(
                service=service_name,
                instance=instance_name,
                cluster=cluster,
                config_dict={
                    "cpus": 0.1,
                    "mem": 50,
                    "instances": 3,
                    "cmd": 'sleep 50',
                    "drain_method": "test"
                },
                branch_dict={
                    'docker_image': 'busybox',
                    'desired_state': 'start',
                    'force_bounce': str(force_bounce),
                },
            ))

        scheduler = native_mesos_scheduler.PaastaScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
        )
        fake_driver = mock.Mock()

        # First, start up 3 old tasks
        old_tasks = scheduler.start_task(fake_driver, make_fake_offer())
        assert len(scheduler.tasks_with_flags) == 3
        # and mark the old tasks as up
        for task in old_tasks:
            scheduler.statusUpdate(fake_driver, mock.Mock(task_id=task.task_id, state=TASK_RUNNING))
        assert len(scheduler.drain_method.downed_task_ids) == 0

        # Now, change force_bounce
        scheduler.service_config = service_configs[1]

        # and start 3 more tasks
        new_tasks = scheduler.start_task(fake_driver, make_fake_offer())
        assert len(scheduler.tasks_with_flags) == 6
        # It should not drain anything yet, since the new tasks aren't up.
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(scheduler.tasks_with_flags) == 6
        assert len(scheduler.drain_method.downed_task_ids) == 0

        # Now we mark the new tasks as up.
        for i, task in enumerate(new_tasks):
            scheduler.statusUpdate(fake_driver, mock.Mock(task_id=task.task_id, state=TASK_RUNNING))
            # As each of these new tasks come up, we should drain an old one.
            assert len(scheduler.drain_method.downed_task_ids) == i + 1

        # Now let's roll back and make sure it undrains the old ones and drains new.
        scheduler.service_config = service_configs[0]
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert scheduler.drain_method.downed_task_ids == set([])
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert scheduler.drain_method.downed_task_ids == set([t.task_id.value for t in new_tasks])

        # Once we drain the new tasks, it should kill them.
        assert fake_driver.killTask.call_count == 0

        # we issue duplicate kills for tasks until we get notified about TASK_KILLED, so we keep track of
        # the unique IDs of tasks being killed.
        killed_tasks = set()

        def killTask_side_effect(task_id):
            killed_tasks.add(task_id.value)

        fake_driver.killTask.side_effect = killTask_side_effect

        scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(killed_tasks) == 1
        scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(killed_tasks) == 2
        scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert scheduler.drain_method.safe_to_kill_task_ids == set([t.task_id.value for t in new_tasks])
        assert len(killed_tasks) == 3

        for task in new_tasks:
            fake_driver.killTask.assert_any_call(task.task_id)

        # Now tell the scheduler those tasks have died.
        for task in new_tasks:
            scheduler.statusUpdate(fake_driver, mock.Mock(task_id=task.task_id, state=TASK_KILLED))

        # Clean up the TestDrainMethod for the rest of this test.
        for task in list(scheduler.drain_method.downed_task_ids):
            scheduler.drain_method.stop_draining(mock.Mock(id=task))
        assert len(scheduler.drain_method.downed_task_ids) == 0

        # Now scale down old app
        scheduler.service_config.config_dict['instances'] = 2
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(scheduler.drain_method.downed_task_ids) == 1

        # mark it as drained and let the scheduler kill it.
        scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
        killed_tasks.clear()
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(killed_tasks) == 1

    def test_start_task_chooses_port(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = []
        service_configs.append(native_mesos_scheduler.PaastaNativeServiceConfig(
            service=service_name,
            instance=instance_name,
            cluster=cluster,
            config_dict={
                "cpus": 0.1,
                "mem": 50,
                "instances": 1,
                "cmd": 'sleep 50',
                "drain_method": "test"
            },
            branch_dict={
                'docker_image': 'busybox',
                'desired_state': 'start',
                'force_bounce': '0',
            },
        ))

        scheduler = native_mesos_scheduler.PaastaScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
        )
        fake_driver = mock.Mock()

        fake_offer = make_fake_offer(port_begin=12345, port_end=12345)
        tasks = scheduler.start_task(fake_driver, fake_offer)

        assert len(tasks) == 1
        for resource in tasks[0].resources:
            if resource.name == "ports":
                assert resource.ranges.range[0].begin == 12345
                assert resource.ranges.range[0].end == 12345
                break
        else:
            raise Exception("never saw a ports resource")


class TestPaastaNativeServiceConfig(object):
    def test_base_task(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_config = native_mesos_scheduler.PaastaNativeServiceConfig(
            service=service_name,
            instance=instance_name,
            cluster=cluster,
            config_dict={
                "cpus": 0.1,
                "mem": 50,
                "instances": 3,
                "cmd": 'sleep 50',
                "drain_method": "test",
                "extra_volumes": [{"containerPath": "/foo", "hostPath": "/bar", "mode": "RW"}]
            },
            branch_dict={
                'docker_image': 'busybox',
                'desired_state': 'start',
                'force_bounce': '0',
            },
        )

        task = service_config.base_task(system_paasta_config)

        assert task.container.type == mesos_pb2.ContainerInfo.DOCKER
        assert task.container.docker.image == "fake/busybox"
        parameters = [(p.key, p.value) for p in task.container.docker.parameters]
        assert parameters == [
            ("memory-swap", mock.ANY),
            ("cpu-period", mock.ANY),
            ("cpu-quota", mock.ANY),
        ]

        assert task.container.docker.network == mesos_pb2.ContainerInfo.DockerInfo.BRIDGE

        assert len(task.container.volumes) == 1
        assert task.container.volumes[0].mode == mesos_pb2.Volume.RW
        assert task.container.volumes[0].container_path == "/foo"
        assert task.container.volumes[0].host_path == "/bar"

        assert len(task.container.docker.port_mappings) == 1
        assert task.container.docker.port_mappings[0].container_port == 8888
        assert task.container.docker.port_mappings[0].host_port == 0
        assert task.container.docker.port_mappings[0].protocol == "tcp"

        assert task.command.value == "sleep 50"

        assert len(task.resources) == 3

        for resource in task.resources:
            if resource.name == "cpus":
                assert resource.scalar.value == 0.1
            elif resource.name == "mem":
                assert resource.scalar.value == 50
            elif resource.name == "port":
                pass

        assert task.name.startswith("service_name.instance_name.gitbusybox.config")


@mock.patch('paasta_tools.mesos_tools.get_local_slave_state', autospec=True)
def test_paasta_native_services_running_here(mock_get_local_slave_state):
    id_1 = 'klingon.ships.detected.249qwiomelht4jioewglkemr.someuuid'
    id_2 = 'fire.photon.torpedos.jtgriemot5yhtwe94.someuuid'
    id_3 = 'dota.axe.cleave.482u9jyoi4wed.someuuid'
    id_4 = 'mesos.deployment.is.hard.someuuid'
    id_5 = 'how.to.fake.data.someuuid'
    ports_1 = '[111-111]'
    ports_2 = '[222-222]'
    ports_3 = '[333-333]'
    ports_4 = '[444-444]'
    ports_5 = '[555-555]'
    mock_get_local_slave_state.return_value = {
        'frameworks': [
            {
                'executors': [
                    {'id': id_1, 'resources': {'ports': ports_1},
                        'tasks': [{u'state': u'TASK_RUNNING'}]},
                    {'id': id_2, 'resources': {'ports': ports_2}, 'tasks': [{u'state': u'TASK_RUNNING'}]}
                ],
                'name': 'paasta_native service.instance-1111111'
            },
            {
                'executors': [
                    {'id': id_3, 'resources': {'ports': ports_3}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                    {'id': id_4, 'resources': {'ports': ports_4}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                ],
                'name': 'paasta_native service.instance-3145jgreoifd'
            },
            {
                'executors': [
                    {'id': id_5, 'resources': {'ports': ports_5}, 'tasks': [{u'state': u'TASK_STAGED'}]},
                ],
                'name': 'paasta_native service.instance-754rchoeurcho'
            },
            {
                'executors': [
                    {'id': 'bunk', 'resources': {'ports': '[65-65]'}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                ],
                'name': 'super_bunk'
            }
        ]
    }
    expected = [('klingon', 'ships', 111),
                ('fire', 'photon', 222),
                ('dota', 'axe', 333),
                ('mesos', 'deployment', 444)]
    actual = native_mesos_scheduler.paasta_native_services_running_here()
    mock_get_local_slave_state.assert_called_once_with(hostname=None)
    assert expected == actual


def test_get_paasta_native_services_running_here_for_nerve():
    cluster = 'edelweiss'
    soa_dir = 'the_sound_of_music'
    fake_marathon_services = [('no_test', 'left_behind', 1111),
                              ('no_docstrings', 'forever_abandoned', 2222)]
    registrations = [
        ['no_docstrings.dos'],
        ['no_test.uno']
    ]
    nerve_dicts = [long_running_service_tools.ServiceNamespaceConfig({'binary': 1, 'proxy_port': 6666}),
                   long_running_service_tools.ServiceNamespaceConfig({'clock': 0, 'proxy_port': 6666})]
    expected = [('no_test.uno', {'clock': 0, 'port': 1111, 'proxy_port': 6666}),
                ('no_docstrings.dos', {'binary': 1, 'port': 2222, 'proxy_port': 6666})]
    with contextlib.nested(
        mock.patch('paasta_tools.native_mesos_scheduler.paasta_native_services_running_here',
                   autospec=True,
                   return_value=fake_marathon_services),
        mock.patch('paasta_tools.native_mesos_scheduler.read_all_registrations_for_service_instance',
                   autospec=True,
                   side_effect=lambda *args, **kwargs: registrations.pop()),
        mock.patch('paasta_tools.native_mesos_scheduler.load_service_namespace_config',
                   autospec=True,
                   side_effect=lambda *args, **kwargs: nerve_dicts.pop()),
    ) as (
        pnsrh_patch,
        get_namespace_patch,
        read_ns_config_patch,
    ):
        actual = native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(cluster, soa_dir)
        assert expected == actual
        pnsrh_patch.assert_called_once_with(hostname=None)
        get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
        get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
        assert get_namespace_patch.call_count == 2
        read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
        read_ns_config_patch.assert_any_call('no_docstrings', 'dos', soa_dir)
        assert read_ns_config_patch.call_count == 2


def test_get_paasta_native_services_running_here_for_nerve_multiple_namespaces():
    cluster = 'edelweiss'
    soa_dir = 'the_sound_of_music'
    fake_marathon_services = [('no_test', 'left_behind', 1111),
                              ('no_docstrings', 'forever_abandoned', 2222)]
    namespaces = [
        ['no_docstrings.quatro'],
        ['no_test.uno', 'no_test.dos', 'no_test.tres']
    ]
    nerve_dicts = {
        ('no_test', 'uno'): long_running_service_tools.ServiceNamespaceConfig({'proxy_port': 6666}),
        ('no_test', 'dos'): long_running_service_tools.ServiceNamespaceConfig({'proxy_port': 6667}),
        ('no_test', 'tres'): long_running_service_tools.ServiceNamespaceConfig({'proxy_port': 6668}),
        ('no_docstrings', 'quatro'): long_running_service_tools.ServiceNamespaceConfig({'proxy_port': 6669})
    }
    expected = [('no_test.uno', {'port': 1111, 'proxy_port': 6666}),
                ('no_test.dos', {'port': 1111, 'proxy_port': 6667}),
                ('no_test.tres', {'port': 1111, 'proxy_port': 6668}),
                ('no_docstrings.quatro', {'port': 2222, 'proxy_port': 6669})]
    with contextlib.nested(
        mock.patch('paasta_tools.native_mesos_scheduler.paasta_native_services_running_here',
                   autospec=True,
                   return_value=fake_marathon_services),
        mock.patch('paasta_tools.native_mesos_scheduler.read_all_registrations_for_service_instance',
                   autospec=True,
                   side_effect=lambda *args, **kwargs: namespaces.pop()),
        mock.patch('paasta_tools.native_mesos_scheduler.load_service_namespace_config',
                   autospec=True,
                   side_effect=lambda service, namespace, soa_dir: nerve_dicts.pop((service, namespace))),
    ) as (
        pnsrh_patch,
        get_namespace_patch,
        read_ns_config_patch,
    ):
        actual = native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(cluster, soa_dir)
        assert expected == actual
        pnsrh_patch.assert_called_once_with(hostname=None)
        get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
        get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
        assert get_namespace_patch.call_count == 2
        read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
        read_ns_config_patch.assert_any_call('no_test', 'dos', soa_dir)
        read_ns_config_patch.assert_any_call('no_test', 'tres', soa_dir)
        read_ns_config_patch.assert_any_call('no_docstrings', 'quatro', soa_dir)
        assert read_ns_config_patch.call_count == 4


def test_get_paasta_native_services_running_here_for_nerve_when_not_in_smartstack():
    cluster = 'edelweiss'
    soa_dir = 'the_sound_of_music'
    fake_marathon_services = [('no_test', 'left_behind', 1111),
                              ('no_docstrings', 'forever_abandoned', 2222)]
    registrations = [
        ['no_docstrings.dos'],
        ['no_test.uno']
    ]
    nerve_dicts = [long_running_service_tools.ServiceNamespaceConfig({'binary': 1}),
                   long_running_service_tools.ServiceNamespaceConfig({'clock': 0, 'proxy_port': 6666})]
    expected = [('no_test.uno', {'clock': 0, 'port': 1111, 'proxy_port': 6666})]
    with contextlib.nested(
        mock.patch('paasta_tools.native_mesos_scheduler.paasta_native_services_running_here',
                   autospec=True,
                   return_value=fake_marathon_services),
        mock.patch('paasta_tools.native_mesos_scheduler.read_all_registrations_for_service_instance',
                   autospec=True,
                   side_effect=lambda *args, **kwargs: registrations.pop()),
        mock.patch('paasta_tools.native_mesos_scheduler.load_service_namespace_config',
                   autospec=True,
                   side_effect=lambda *args, **kwargs: nerve_dicts.pop()),
    ) as (
        pnsrh_patch,
        get_namespace_patch,
        read_ns_config_patch,
    ):
        actual = native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(cluster, soa_dir)
        assert expected == actual
        pnsrh_patch.assert_called_once_with(hostname=None)
        get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
        get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
        assert get_namespace_patch.call_count == 2
        read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
        read_ns_config_patch.assert_any_call('no_docstrings', 'dos', soa_dir)
        assert read_ns_config_patch.call_count == 2


def test_get_paasta_native_services_running_here_for_nerve_when_get_cluster_raises_custom_exception():
    cluster = None
    soa_dir = 'the_sound_of_music'
    with contextlib.nested(
        mock.patch(
            'paasta_tools.native_mesos_scheduler.load_system_paasta_config',
            autospec=True,
        ),
        mock.patch(
            'paasta_tools.native_mesos_scheduler.paasta_native_services_running_here',
            autospec=True,
            return_value=[],
        ),
    ) as (
        load_system_paasta_config_patch,
        paasta_native_services_running_here_patch,
    ):
        load_system_paasta_config_patch.return_value.get_cluster \
            = mock.Mock(side_effect=native_mesos_scheduler.PaastaNotConfiguredError)
        actual = native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(cluster, soa_dir)
        assert actual == []


def test_get_paasta_native_services_running_here_for_nerve_when_paasta_not_configured():
    cluster = None
    soa_dir = 'the_sound_of_music'
    with contextlib.nested(
        mock.patch(
            'paasta_tools.native_mesos_scheduler.load_system_paasta_config',
            autospec=True,
        ),
        mock.patch(
            'paasta_tools.native_mesos_scheduler.paasta_native_services_running_here',
            autospec=True,
            return_value=[],
        ),
    ) as (
        load_system_paasta_config_patch,
        paasta_native_services_running_here_patch,
    ):
        load_system_paasta_config_patch.return_value.get_cluster \
            = mock.Mock(side_effect=native_mesos_scheduler.PaastaNotConfiguredError)
        actual = native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(cluster, soa_dir)
        assert actual == []


def test_get_paasta_native_services_running_here_for_nerve_when_get_cluster_raises_other_exception():
    cluster = None
    soa_dir = 'the_sound_of_music'
    with contextlib.nested(
        mock.patch(
            'paasta_tools.native_mesos_scheduler.load_system_paasta_config',
            autospec=True,
        ),
        mock.patch(
            'paasta_tools.native_mesos_scheduler.paasta_native_services_running_here',
            autospec=True,
            return_value=[],
        ),
    ) as (
        load_system_paasta_config_patch,
        paasta_native_services_running_here_patch,
    ):
        load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(side_effect=Exception)
        with pytest.raises(Exception):
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(cluster, soa_dir)
