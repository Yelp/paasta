import mock
import pytest
from addict import Dict

from paasta_tools.frameworks import native_scheduler
from paasta_tools.frameworks.native_scheduler import TASK_KILLED
from paasta_tools.frameworks.native_scheduler import TASK_RUNNING
from paasta_tools.frameworks.native_service_config import NativeServiceConfig
from paasta_tools.frameworks.task_store import DictTaskStore
from paasta_tools.util.config_loading import SystemPaastaConfig


@pytest.fixture
def system_paasta_config():
    return SystemPaastaConfig(
        {"docker_registry": "fake", "volumes": [], "dockercfg_location": "/foo/bar"},
        "/fake/system/configs",
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


class TestNativeScheduler:
    @mock.patch("paasta_tools.frameworks.native_scheduler._log", autospec=True)
    def test_start_upgrade_rollback_scaledown(self, mock_log, system_paasta_config):
        service_name = "service_name"
        instance_name = "instance_name"
        cluster = "cluster"

        service_configs = []
        for force_bounce in range(2):
            service_configs.append(
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
                        "force_bounce": str(force_bounce),
                    },
                    soa_dir="/nail/etc/services",
                )
            )

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            staging_timeout=1,
            task_store_type=DictTaskStore,
        )
        fake_driver = mock.Mock()
        scheduler.registered(
            driver=fake_driver, frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        with mock.patch(
            "paasta_tools.util.config_loading.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ):
            # First, start up 3 old tasks
            old_tasks = scheduler.launch_tasks_for_offers(
                fake_driver, [make_fake_offer()]
            )
            assert len(scheduler.task_store.get_all_tasks()) == 3
            # and mark the old tasks as up
            for task in old_tasks:
                scheduler.statusUpdate(
                    fake_driver, dict(task_id=task["task_id"], state=TASK_RUNNING)
                )
            assert len(scheduler.drain_method.downed_task_ids) == 0

            # Now, change force_bounce
            scheduler.service_config = service_configs[1]

            # and start 3 more tasks
            new_tasks = scheduler.launch_tasks_for_offers(
                fake_driver, [make_fake_offer()]
            )
            assert len(scheduler.task_store.get_all_tasks()) == 6
            # It should not drain anything yet, since the new tasks aren't up.
            scheduler.kill_tasks_if_necessary(fake_driver)
            assert len(scheduler.task_store.get_all_tasks()) == 6
            assert len(scheduler.drain_method.downed_task_ids) == 0

            # Now we mark the new tasks as up.
            for i, task in enumerate(new_tasks):
                scheduler.statusUpdate(
                    fake_driver, dict(task_id=task["task_id"], state=TASK_RUNNING)
                )
                # As each of these new tasks come up, we should drain an old one.
                assert len(scheduler.drain_method.downed_task_ids) == i + 1

            # Now let's roll back and make sure it undrains the old ones and drains new.
            scheduler.service_config = service_configs[0]
            scheduler.kill_tasks_if_necessary(fake_driver)
            assert scheduler.drain_method.downed_task_ids == set()
            scheduler.kill_tasks_if_necessary(fake_driver)
            assert scheduler.drain_method.downed_task_ids == {
                t["task_id"]["value"] for t in new_tasks
            }

            # Once we drain the new tasks, it should kill them.
            assert fake_driver.killTask.call_count == 0

            # we issue duplicate kills for tasks until we get notified about TASK_KILLED, so we keep track of
            # the unique IDs of tasks being killed.
            killed_tasks = set()

            def killTask_side_effect(task_id):
                killed_tasks.add(task_id["value"])

            fake_driver.killTask.side_effect = killTask_side_effect

            scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
            scheduler.kill_tasks_if_necessary(fake_driver)
            assert len(killed_tasks) == 1
            scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
            scheduler.kill_tasks_if_necessary(fake_driver)
            assert len(killed_tasks) == 2
            scheduler.drain_method.mark_arbitrary_task_as_safe_to_kill()
            scheduler.kill_tasks_if_necessary(fake_driver)
            assert scheduler.drain_method.safe_to_kill_task_ids == {
                t["task_id"]["value"] for t in new_tasks
            }
            assert len(killed_tasks) == 3

            for task in new_tasks:
                fake_driver.killTask.assert_any_call(task["task_id"])

            # Now tell the scheduler those tasks have died.
            for task in new_tasks:
                scheduler.statusUpdate(
                    fake_driver, dict(task_id=task["task_id"], state=TASK_KILLED)
                )

            # Clean up the TestDrainMethod for the rest of this test.
            assert not list(scheduler.drain_method.downed_task_ids)

            # Now scale down old app
            scheduler.service_config.config_dict["instances"] = 2
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
        service_configs.append(
            NativeServiceConfig(
                service=service_name,
                instance=instance_name,
                cluster=cluster,
                config_dict={
                    "cpus": 0.1,
                    "mem": 50,
                    "instances": 1,
                    "cmd": "sleep 50",
                    "drain_method": "test",
                },
                branch_dict={
                    "docker_image": "busybox",
                    "desired_state": "start",
                    "force_bounce": "0",
                },
                soa_dir="/nail/etc/services",
            )
        )

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            reconcile_start_time=0,
            staging_timeout=1,
            task_store_type=DictTaskStore,
        )
        scheduler.registered(
            driver=mock.Mock(), frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        with mock.patch(
            "paasta_tools.util.config_loading.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ):
            tasks, _ = scheduler.tasks_and_state_for_offer(
                mock.Mock(), make_fake_offer(port_begin=12345, port_end=12345), {}
            )

        assert {
            "name": "ports",
            "ranges": {"range": [{"begin": 12345, "end": 12345}]},
            "type": "RANGES",
        } in tasks[0]["resources"]

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
                "cmd": "sleep 50",
                "drain_method": "test",
                "pool": "default",
            },
            branch_dict={
                "docker_image": "busybox",
                "desired_state": "start",
                "force_bounce": "0",
            },
            soa_dir="/nail/etc/services",
        )

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_config,
            staging_timeout=1,
            task_store_type=DictTaskStore,
        )
        scheduler.registered(
            driver=mock.Mock(), frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        assert scheduler.offer_matches_pool(
            make_fake_offer(port_begin=12345, port_end=12345, pool="default")
        )
        assert not scheduler.offer_matches_pool(
            make_fake_offer(port_begin=12345, port_end=12345, pool="somethingelse")
        )
        assert not scheduler.offer_matches_pool(
            make_fake_offer(port_begin=12345, port_end=12345, pool=None)
        )


class TestNativeServiceConfig:
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
                "cmd": "sleep 50",
                "drain_method": "test",
                "extra_volumes": [
                    {"containerPath": "/foo", "hostPath": "/bar", "mode": "RW"}
                ],
            },
            branch_dict={
                "docker_image": "busybox",
                "desired_state": "start",
                "force_bounce": "0",
            },
            soa_dir="/nail/etc/services",
        )

        with mock.patch(
            "paasta_tools.util.config_loading.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ), mock.patch(
            "paasta_tools.util.config_loading.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=True,
        ):
            task = service_config.base_task(system_paasta_config)

        assert task == {
            "container": {
                "type": "DOCKER",
                "docker": {
                    "image": "fake/busybox",
                    "parameters": [
                        {"key": "memory-swap", "value": mock.ANY},
                        {"key": "cpu-period", "value": mock.ANY},
                        {"key": "cpu-quota", "value": mock.ANY},
                        {"key": "storage-opt", "value": mock.ANY},
                        {"key": "label", "value": mock.ANY},  # service
                        {"key": "label", "value": mock.ANY},  # instance
                        {"key": "init", "value": "true"},
                        {"key": "cap-drop", "value": "SETPCAP"},
                        {"key": "cap-drop", "value": "MKNOD"},
                        {"key": "cap-drop", "value": "AUDIT_WRITE"},
                        {"key": "cap-drop", "value": "CHOWN"},
                        {"key": "cap-drop", "value": "NET_RAW"},
                        {"key": "cap-drop", "value": "DAC_OVERRIDE"},
                        {"key": "cap-drop", "value": "FOWNER"},
                        {"key": "cap-drop", "value": "FSETID"},
                        {"key": "cap-drop", "value": "KILL"},
                        {"key": "cap-drop", "value": "SETGID"},
                        {"key": "cap-drop", "value": "SETUID"},
                        {"key": "cap-drop", "value": "NET_BIND_SERVICE"},
                        {"key": "cap-drop", "value": "SYS_CHROOT"},
                        {"key": "cap-drop", "value": "SETFCAP"},
                    ],
                    "network": "BRIDGE",
                    "port_mappings": [
                        {"container_port": 8888, "host_port": 0, "protocol": "tcp"}
                    ],
                },
                "volumes": [
                    {"mode": "RW", "container_path": "/foo", "host_path": "/bar"}
                ],
            },
            "command": {
                "value": "sleep 50",
                "uris": [
                    {
                        "value": system_paasta_config.get_dockercfg_location(),
                        "extract": False,
                    }
                ],
            },
            "resources": [
                {"name": "cpus", "scalar": {"value": 0.1}, "type": "SCALAR"},
                {"name": "mem", "scalar": {"value": 50}, "type": "SCALAR"},
                {"name": "ports", "ranges": mock.ANY, "type": "RANGES"},
            ],
            "name": mock.ANY,
            "agent_id": {"value": ""},
            "task_id": {"value": ""},
        }

        assert task["name"].startswith("service_name.instance_name.gitbusybox.config")

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
                    "cmd": "sleep 50",
                    "drain_method": "test",
                },
                branch_dict={"docker_image": "busybox", "desired_state": "start"},
                soa_dir="/nail/etc/services",
            )
        ]

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_configs[0],
            staging_timeout=1,
            task_store_type=DictTaskStore,
        )
        fake_driver = mock.Mock()
        scheduler.registered(
            driver=fake_driver, frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        scheduler.blacklist_slave("super big slave")
        assert len(scheduler.blacklisted_slaves) == 1
        scheduler.resourceOffers(fake_driver, [make_fake_offer()])
        assert len(scheduler.task_store.get_all_tasks()) == 0

    def test_make_drain_task_works_with_hacheck_drain_method(
        self, system_paasta_config
    ):
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
                "cmd": "sleep 50",
                "drain_method": "hacheck",
                "pool": "default",
            },
            branch_dict={
                "docker_image": "busybox",
                "desired_state": "start",
                "force_bounce": "0",
            },
            soa_dir="/nail/etc/services",
        )

        scheduler = native_scheduler.NativeScheduler(
            service_name=service_name,
            instance_name=instance_name,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            service_config=service_config,
            staging_timeout=1,
            task_store_type=DictTaskStore,
        )

        fake_driver = mock.Mock()
        scheduler.registered(
            driver=fake_driver, frameworkId={"value": "foo"}, masterInfo=mock.Mock()
        )

        # launch a task
        offer = make_fake_offer(port_begin=31337, port_end=31337)
        with mock.patch(
            "paasta_tools.util.config_loading.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ):
            scheduler.launch_tasks_for_offers(driver=fake_driver, offers=[offer])

        expected = [
            "http://super_big_slave:6666/spool/service_name.instance_name/31337/status"
        ]
        actual = scheduler.drain_method.spool_urls(
            scheduler.make_drain_task(
                list(scheduler.task_store.get_all_tasks().keys())[0]
            )
        )
        assert actual == expected
