from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from mesos.interface import mesos_pb2

from paasta_tools import utils
from paasta_tools.frameworks import native_scheduler
from paasta_tools.frameworks.native_scheduler import TASK_KILLED
from paasta_tools.frameworks.native_scheduler import TASK_RUNNING
from paasta_tools.frameworks.native_service_config import NativeServiceConfig


@pytest.fixture
def system_paasta_config():
    return utils.SystemPaastaConfig({
        "docker_registry": "fake",
        "volumes": [],
        "dockercfg_location": "/foo/bar",
    }, "/fake/system/configs")


def make_fake_offer(cpu=50000, mem=50000, port_begin=31000, port_end=32000, pool='default'):
    offer = mesos_pb2.Offer()
    offer.slave_id.value = "super big slave"

    cpus_resource = offer.resources.add()
    cpus_resource.name = "cpus"
    cpus_resource.scalar.value = cpu

    mem_resource = offer.resources.add()
    mem_resource.name = "mem"
    mem_resource.scalar.value = mem

    ports_resource = offer.resources.add()
    ports_resource.name = "ports"
    ports_range = ports_resource.ranges.range.add()
    ports_range.begin = port_begin
    ports_range.end = port_end

    if pool is not None:
        pool_attribute = offer.attributes.add()
        pool_attribute.name = "pool"
        pool_attribute.text.value = pool

    return offer


class TestNativeScheduler(object):
    def test_start_upgrade_rollback_scaledown(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = []
        for force_bounce in range(2):
            service_configs.append(NativeServiceConfig(
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

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            staging_timeout=1,
        )
        fake_driver = mock.Mock()

        # First, start up 3 old tasks
        old_tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
        assert len(scheduler.tasks_with_flags) == 3
        # and mark the old tasks as up
        for task in old_tasks:
            scheduler.statusUpdate(fake_driver, mock.Mock(task_id=task.task_id, state=TASK_RUNNING))
        assert len(scheduler.drain_method.downed_task_ids) == 0

        # Now, change force_bounce
        scheduler.service_config = service_configs[1]

        # and start 3 more tasks
        new_tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
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
        assert scheduler.drain_method.downed_task_ids == set()
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert scheduler.drain_method.downed_task_ids == {t.task_id.value for t in new_tasks}

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
        assert scheduler.drain_method.safe_to_kill_task_ids == {t.task_id.value for t in new_tasks}
        assert len(killed_tasks) == 3

        for task in new_tasks:
            fake_driver.killTask.assert_any_call(task.task_id)

        # Now tell the scheduler those tasks have died.
        for task in new_tasks:
            scheduler.statusUpdate(fake_driver, mock.Mock(task_id=task.task_id, state=TASK_KILLED))

        # Clean up the TestDrainMethod for the rest of this test.
        assert not list(scheduler.drain_method.downed_task_ids)

        # Now scale down old app
        scheduler.service_config.config_dict['instances'] = 2
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(scheduler.drain_method.downed_task_ids) == 1

        # mark it as drained and let the scheduler kill it.
        scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
        killed_tasks.clear()
        scheduler.kill_tasks_if_necessary(fake_driver)
        assert len(killed_tasks) == 1

    def test_tasks_for_offer_chooses_port(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = []
        service_configs.append(NativeServiceConfig(
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

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            reconcile_start_time=0,
            staging_timeout=1,
        )

        tasks, _ = scheduler.tasks_and_state_for_offer(
            mock.Mock(), make_fake_offer(port_begin=12345, port_end=12345), {})

        assert len(tasks) == 1
        for task in tasks:
            for resource in task.resources:
                if resource.name == "ports":
                    assert resource.ranges.range[0].begin == 12345
                    assert resource.ranges.range[0].end == 12345
                    break
            else:
                raise AssertionError("never saw a ports resource")

    def test_offer_matches_pool(self):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_config = NativeServiceConfig(
            service=service_name,
            instance=instance_name,
            cluster=cluster,
            config_dict={
                "cpus": 0.1,
                "mem": 50,
                "instances": 1,
                "cmd": 'sleep 50',
                "drain_method": "test",
                "pool": "default",
            },
            branch_dict={
                'docker_image': 'busybox',
                'desired_state': 'start',
                'force_bounce': '0',
            },

        )

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_config,
            staging_timeout=1
        )

        assert scheduler.offer_matches_pool(make_fake_offer(port_begin=12345, port_end=12345, pool="default"))
        assert not scheduler.offer_matches_pool(make_fake_offer(port_begin=12345, port_end=12345, pool="somethingelse"))
        assert not scheduler.offer_matches_pool(make_fake_offer(port_begin=12345, port_end=12345, pool=None))


class TestNativeServiceConfig(object):
    def test_base_task(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_config = NativeServiceConfig(
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
            ("label", mock.ANY),  # service
            ("label", mock.ANY),  # instance
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
            elif resource.name == 'ports':
                pass
            else:
                raise AssertionError('Unreachable: {}'.format(resource.name))

        assert task.name.startswith("service_name.instance_name.gitbusybox.config")

        assert task.command.uris[0].value == system_paasta_config.get_dockercfg_location()

    def test_resource_offers_ignores_blacklisted_slaves(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = [
            NativeServiceConfig(
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
                },
            )
        ]

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            staging_timeout=1,
        )

        fake_driver = mock.Mock()

        scheduler.blacklist_slave('super big slave')
        assert len(scheduler.blacklisted_slaves) == 1
        scheduler.resourceOffers(fake_driver, [make_fake_offer()])
        assert len(scheduler.tasks_with_flags) == 0
