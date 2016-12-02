from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

import mock
import pytest

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
    }, "/fake/system/configs")


def fake_offer():
    offer = mock.Mock(
        resources=[
            mock.Mock(scalar=mock.Mock(value=50000)),
            mock.Mock(scalar=mock.Mock(value=50000)),
        ],
        slave_id=mock.Mock(value="super big slave"),
    )

    offer.resources[0].name = "cpus"
    offer.resources[1].name = "mem"

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
        old_tasks = scheduler.start_task(fake_driver, fake_offer())
        assert len(scheduler.tasks_with_flags) == 3
        # and mark the old tasks as up
        for task in old_tasks:
            scheduler.statusUpdate(fake_driver, mock.Mock(task_id=task.task_id, state=TASK_RUNNING))
        assert len(scheduler.drain_method.downed_task_ids) == 0

        # Now, change force_bounce
        scheduler.service_config = service_configs[1]

        # and start 3 more tasks
        new_tasks = scheduler.start_task(fake_driver, fake_offer())
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
