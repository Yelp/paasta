from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from mesos.interface import mesos_pb2

from paasta_tools import utils
from paasta_tools.frameworks import adhoc_scheduler
from paasta_tools.frameworks import native_scheduler
from paasta_tools.frameworks.native_service_config import NativeServiceConfig
from paasta_tools.frameworks.native_service_config import UnknownNativeServiceError


@pytest.fixture
def system_paasta_config():
    return utils.SystemPaastaConfig({
        "docker_registry": "fake",
        "volumes": [],
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


class TestAdhocScheduler(object):
    def test_raise_error_when_cmd_missing(self, system_paasta_config):
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
                    "drain_method": "test"
                },
                branch_dict={
                    'docker_image': 'busybox',
                    'desired_state': 'start',
                },
            )
        ]

        with pytest.raises(UnknownNativeServiceError):
            adhoc_scheduler.AdhocScheduler(
                service_name=service_name,
                instance_name=instance_name,
                cluster=cluster,
                system_paasta_config=system_paasta_config,
                service_config=service_configs[0],
                dry_run=False,
                reconcile_start_time=0,
                staging_timeout=30,
            )

    def test_can_only_launch_task_once(self, system_paasta_config):
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

        scheduler = adhoc_scheduler.AdhocScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            dry_run=False,
            reconcile_start_time=0,
            staging_timeout=30,
        )

        fake_driver = mock.Mock()

        # Check that offers with invalid pool don't get accepted
        tasks, _ = scheduler.tasks_and_state_for_offer(
            fake_driver, make_fake_offer(pool='notdefault'), {})
        assert len(tasks) == 0

        tasks, _ = scheduler.tasks_and_state_for_offer(
            fake_driver, make_fake_offer(pool=None), {})
        assert len(tasks) == 0

        tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
        task_id = tasks[0].task_id.value
        task_name = tasks[0].name
        assert len(scheduler.tasks_with_flags) == 1
        assert len(tasks) == 1
        assert scheduler.need_more_tasks(task_name, scheduler.tasks_with_flags, []) is False
        assert scheduler.need_to_stop() is False

        no_tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
        assert len(scheduler.tasks_with_flags) == 1
        assert len(no_tasks) == 0
        assert scheduler.need_to_stop() is False

        scheduler.statusUpdate(
            fake_driver,
            mock.Mock(task_id=mock.Mock(value=task_id), state=native_scheduler.TASK_FINISHED))
        assert len(scheduler.tasks_with_flags) == 1
        assert scheduler.tasks_with_flags[task_id].marked_for_gc is True
        assert scheduler.need_to_stop() is True

    def test_can_run_multiple_copies(self, system_paasta_config):
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

        scheduler = adhoc_scheduler.AdhocScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            dry_run=False,
            reconcile_start_time=0,
            staging_timeout=30,
            service_config_overrides={'instances': 5}
        )

        fake_driver = mock.Mock()

        tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
        task_name = tasks[0].name
        task_ids = [t.task_id.value for t in tasks]

        assert len(scheduler.tasks_with_flags) == 5
        assert len(tasks) == 5
        assert scheduler.need_more_tasks(task_name, scheduler.tasks_with_flags, []) is False
        assert scheduler.need_to_stop() is False

        no_tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
        assert len(scheduler.tasks_with_flags) == 5
        assert len(no_tasks) == 0
        assert scheduler.need_to_stop() is False

        for idx, task_id in enumerate(task_ids):
            scheduler.statusUpdate(
                fake_driver,
                mock.Mock(task_id=mock.Mock(value=task_id), state=native_scheduler.TASK_FINISHED))
            assert len(scheduler.tasks_with_flags) == 5 - idx
            assert scheduler.tasks_with_flags[task_id].marked_for_gc is True
            assert scheduler.need_to_stop() is (idx == 4)
