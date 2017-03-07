from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from paasta_tools import utils
from paasta_tools.frameworks import adhoc_scheduler
from paasta_tools.frameworks import native_scheduler


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


class TestAdhocScheduler(object):
    def test_can_only_launch_task_once(self, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = [
            native_scheduler.NativeServiceConfig(
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
            dry_run=False
        )
        fake_driver = mock.Mock()

        tasks = scheduler.tasksForOffer(fake_driver, make_fake_offer())
        assert len(scheduler.tasks_with_flags) == 1
        assert scheduler.need_more_tasks() is False
        assert scheduler.task_started is True

        scheduler.tasksForOffer(fake_driver, make_fake_offer())
        assert len(scheduler.tasks_with_flags) == 1
        assert scheduler.need_more_tasks() is False
        assert scheduler.task_started is True

        scheduler.statusUpdate(
            fake_driver,
            mock.Mock(task_id=tasks[0].task_id,
                      state=native_scheduler.TASK_FINISHED))
        assert len(scheduler.tasks_with_flags) == 0
        assert scheduler.need_more_tasks() is True
        assert scheduler.task_started is True

        scheduler.tasksForOffer(fake_driver, make_fake_offer())
        assert len(scheduler.tasks_with_flags) == 0
