from unittest import mock

import pytest
from addict import Dict

from paasta_tools import utils
from paasta_tools.frameworks import adhoc_scheduler
from paasta_tools.frameworks import native_scheduler
from paasta_tools.frameworks.native_service_config import NativeServiceConfig
from paasta_tools.frameworks.native_service_config import UnknownNativeServiceError
from paasta_tools.frameworks.task_store import DictTaskStore


@pytest.fixture
def system_paasta_config():
    return utils.SystemPaastaConfig(
        {"docker_registry": "fake", "volumes": []}, "/fake/system/configs"
    )


def make_fake_offer(
    cpu=50000, mem=50000, port_begin=31000, port_end=32000, pool="default"
):
    offer = Dict(
        agent_id=Dict(value="super_big_slave"),
        resources=[
            Dict(name="cpus", scalar=Dict(value=cpu)),
            Dict(name="mem", scalar=Dict(value=mem)),
            Dict(
                name="ports", ranges=Dict(range=[Dict(begin=port_begin, end=port_end)])
            ),
        ],
        attributes=[],
    )

    if pool is not None:
        offer.attributes = [Dict(name="pool", text=Dict(value=pool))]

    return offer


class TestAdhocScheduler:
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
                    "drain_method": "test",
                },
                branch_dict={"docker_image": "busybox", "desired_state": "start"},
                soa_dir="/nail/etc/services",
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
                task_store_type=DictTaskStore,
            )

    @mock.patch("paasta_tools.frameworks.native_scheduler._log", autospec=True)
    def test_can_only_launch_task_once(self, mock_log, system_paasta_config):
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
                    "cmd": "sleep 50",
                    "drain_method": "test",
                },
                branch_dict={
                    "docker_image": "busybox",
                    "desired_state": "start",
                    "force_bounce": None,
                },
                soa_dir="/nail/etc/services",
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
            task_store_type=DictTaskStore,
        )

        fake_driver = mock.Mock()

        scheduler.registered(
            driver=fake_driver, frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        with mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ):
            # Check that offers with invalid pool don't get accepted
            tasks, _ = scheduler.tasks_and_state_for_offer(
                fake_driver, make_fake_offer(pool="notdefault"), {}
            )
            assert len(tasks) == 0

            tasks, _ = scheduler.tasks_and_state_for_offer(
                fake_driver, make_fake_offer(pool=None), {}
            )
            assert len(tasks) == 0

            tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
            task_id = tasks[0]["task_id"]["value"]
            task_name = tasks[0]["name"]
            assert len(scheduler.task_store.get_all_tasks()) == 1
            assert len(tasks) == 1
            assert (
                scheduler.need_more_tasks(
                    task_name, scheduler.task_store.get_all_tasks(), []
                )
                is False
            )
            assert scheduler.need_to_stop() is False

            no_tasks = scheduler.launch_tasks_for_offers(
                fake_driver, [make_fake_offer()]
            )
            assert len(scheduler.task_store.get_all_tasks()) == 1
            assert len(no_tasks) == 0
            assert scheduler.need_to_stop() is False

            scheduler.statusUpdate(
                fake_driver,
                {
                    "task_id": {"value": task_id},
                    "state": native_scheduler.TASK_FINISHED,
                },
            )
            assert len(scheduler.task_store.get_all_tasks()) == 1
            assert scheduler.need_to_stop() is True

    @mock.patch("paasta_tools.frameworks.native_scheduler._log", autospec=True)
    def test_can_run_multiple_copies(self, mock_log, system_paasta_config):
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
                    "cmd": "sleep 50",
                    "drain_method": "test",
                },
                branch_dict={
                    "docker_image": "busybox",
                    "desired_state": "start",
                    "force_bounce": None,
                },
                soa_dir="/nail/etc/services",
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
            service_config_overrides={"instances": 5},
            task_store_type=DictTaskStore,
        )

        fake_driver = mock.Mock()

        scheduler.registered(
            driver=fake_driver, frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        with mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ):
            tasks = scheduler.launch_tasks_for_offers(fake_driver, [make_fake_offer()])
            task_name = tasks[0]["name"]
            task_ids = [t["task_id"]["value"] for t in tasks]

            assert len(scheduler.task_store.get_all_tasks()) == 5
            assert len(tasks) == 5
            assert (
                scheduler.need_more_tasks(
                    task_name, scheduler.task_store.get_all_tasks(), []
                )
                is False
            )
            assert scheduler.need_to_stop() is False

            no_tasks = scheduler.launch_tasks_for_offers(
                fake_driver, [make_fake_offer()]
            )
            assert len(scheduler.task_store.get_all_tasks()) == 5
            assert len(no_tasks) == 0
            assert scheduler.need_to_stop() is False

            for idx, task_id in enumerate(task_ids):
                scheduler.statusUpdate(
                    fake_driver,
                    {
                        "task_id": {"value": task_id},
                        "state": native_scheduler.TASK_FINISHED,
                    },
                )
                assert scheduler.need_to_stop() is (idx == 4)
