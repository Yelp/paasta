import mock
import pytest

from paasta_tools import long_running_service_tools
from paasta_tools import native_mesos_scheduler


def test_main():
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.get_paasta_native_jobs_for_cluster",
        return_value=[("service1", "instance1"), ("service2", "instance2")],
        autospec=True,
    ), mock.patch(
        "paasta_tools.native_mesos_scheduler.create_driver", autospec=True
    ), mock.patch(
        "paasta_tools.native_mesos_scheduler.sleep", autospec=True
    ), mock.patch(
        "paasta_tools.native_mesos_scheduler.load_system_paasta_config", autospec=True
    ), mock.patch(
        "paasta_tools.native_mesos_scheduler.compose_job_id", autospec=True
    ), mock.patch(
        "paasta_tools.native_mesos_scheduler.NativeScheduler", autospec=True
    ):
        native_mesos_scheduler.main(["--stay-alive-seconds=0"])


@mock.patch("paasta_tools.mesos_tools.get_local_slave_state", autospec=True)
def test_paasta_native_services_running_here(mock_get_local_slave_state):
    id_1 = "klingon.ships.detected.249qwiomelht4jioewglkemr.someuuid"
    id_2 = "fire.photon.torpedos.jtgriemot5yhtwe94.someuuid"
    id_3 = "dota.axe.cleave.482u9jyoi4wed.someuuid"
    id_4 = "mesos.deployment.is.hard.someuuid"
    id_5 = "how.to.fake.data.someuuid"
    ports_1 = "[111-111]"
    ports_2 = "[222-222]"
    ports_3 = "[333-333]"
    ports_4 = "[444-444]"
    ports_5 = "[555-555]"
    mock_get_local_slave_state.return_value = {
        "frameworks": [
            {
                "executors": [
                    {
                        "id": id_1,
                        "resources": {"ports": ports_1},
                        "tasks": [{"state": "TASK_RUNNING"}],
                    },
                    {
                        "id": id_2,
                        "resources": {"ports": ports_2},
                        "tasks": [{"state": "TASK_RUNNING"}],
                    },
                ],
                "name": "paasta_native service.instance-1111111",
            },
            {
                "executors": [
                    {
                        "id": id_3,
                        "resources": {"ports": ports_3},
                        "tasks": [{"state": "TASK_RUNNING"}],
                    },
                    {
                        "id": id_4,
                        "resources": {"ports": ports_4},
                        "tasks": [{"state": "TASK_RUNNING"}],
                    },
                ],
                "name": "paasta_native service.instance-3145jgreoifd",
            },
            {
                "executors": [
                    {
                        "id": id_5,
                        "resources": {"ports": ports_5},
                        "tasks": [{"state": "TASK_STAGED"}],
                    }
                ],
                "name": "paasta_native service.instance-754rchoeurcho",
            },
            {
                "executors": [
                    {
                        "id": "bunk",
                        "resources": {"ports": "[65-65]"},
                        "tasks": [{"state": "TASK_RUNNING"}],
                    }
                ],
                "name": "super_bunk",
            },
        ]
    }
    expected = [
        ("klingon", "ships", 111),
        ("fire", "photon", 222),
        ("dota", "axe", 333),
        ("mesos", "deployment", 444),
    ]
    actual = native_mesos_scheduler.paasta_native_services_running_here()
    mock_get_local_slave_state.assert_called_once_with(hostname=None)
    assert expected == actual


def test_get_paasta_native_services_running_here_for_nerve():
    cluster = "edelweiss"
    soa_dir = "the_sound_of_music"
    fake_marathon_services = [
        ("no_test", "left_behind", 1111),
        ("no_docstrings", "forever_abandoned", 2222),
    ]
    registrations = [["no_docstrings.dos"], ["no_test.uno"]]
    nerve_dicts = [
        long_running_service_tools.ServiceNamespaceConfig(
            {"binary": 1, "proxy_port": 6666}
        ),
        long_running_service_tools.ServiceNamespaceConfig(
            {"clock": 0, "proxy_port": 6666}
        ),
    ]
    expected = [
        ("no_test.uno", {"clock": 0, "port": 1111, "proxy_port": 6666}),
        ("no_docstrings.dos", {"binary": 1, "port": 2222, "proxy_port": 6666}),
    ]
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.paasta_native_services_running_here",
        autospec=True,
        return_value=fake_marathon_services,
    ) as pnsrh_patch, mock.patch(
        "paasta_tools.native_mesos_scheduler.load_paasta_native_job_config",
        autospec=True,
    ) as mock_load_paasta_native_job_config, mock.patch(
        "paasta_tools.native_mesos_scheduler.load_service_namespace_config",
        autospec=True,
        side_effect=lambda *args, **kwargs: nerve_dicts.pop(),
    ) as read_ns_config_patch:

        def mock_registrations_side_effect(*args, **kwargs):
            return registrations.pop()

        mock_load_paasta_native_job_config.return_value.get_registrations.side_effect = (
            mock_registrations_side_effect
        )
        actual = (
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(
                cluster, soa_dir
            )
        )
        assert expected == actual
        pnsrh_patch.assert_called_once_with(hostname=None)
        mock_load_paasta_native_job_config.assert_any_call(
            service="no_test",
            instance="left_behind",
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        mock_load_paasta_native_job_config.assert_any_call(
            service="no_docstrings",
            instance="forever_abandoned",
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        assert mock_load_paasta_native_job_config.call_count == 2
        read_ns_config_patch.assert_any_call("no_test", "uno", soa_dir)
        read_ns_config_patch.assert_any_call("no_docstrings", "dos", soa_dir)
        assert read_ns_config_patch.call_count == 2


def test_get_paasta_native_services_running_here_for_nerve_multiple_namespaces():
    cluster = "edelweiss"
    soa_dir = "the_sound_of_music"
    fake_marathon_services = [
        ("no_test", "left_behind", 1111),
        ("no_docstrings", "forever_abandoned", 2222),
    ]
    namespaces = [
        ["no_docstrings.quatro"],
        ["no_test.uno", "no_test.dos", "no_test.tres"],
    ]
    nerve_dicts = {
        ("no_test", "uno"): long_running_service_tools.ServiceNamespaceConfig(
            {"proxy_port": 6666}
        ),
        ("no_test", "dos"): long_running_service_tools.ServiceNamespaceConfig(
            {"proxy_port": 6667}
        ),
        ("no_test", "tres"): long_running_service_tools.ServiceNamespaceConfig(
            {"proxy_port": 6668}
        ),
        ("no_docstrings", "quatro"): long_running_service_tools.ServiceNamespaceConfig(
            {"proxy_port": 6669}
        ),
    }
    expected = [
        ("no_test.uno", {"port": 1111, "proxy_port": 6666}),
        ("no_test.dos", {"port": 1111, "proxy_port": 6667}),
        ("no_test.tres", {"port": 1111, "proxy_port": 6668}),
        ("no_docstrings.quatro", {"port": 2222, "proxy_port": 6669}),
    ]
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.paasta_native_services_running_here",
        autospec=True,
        return_value=fake_marathon_services,
    ) as pnsrh_patch, mock.patch(
        "paasta_tools.native_mesos_scheduler.load_paasta_native_job_config",
        autospec=True,
    ) as mock_load_paasta_native_job_config, mock.patch(
        "paasta_tools.native_mesos_scheduler.load_service_namespace_config",
        autospec=True,
        side_effect=lambda service, namespace, soa_dir: nerve_dicts.pop(
            (service, namespace)
        ),
    ) as read_ns_config_patch:

        def mock_registrations_side_effect(*args, **kwargs):
            return namespaces.pop()

        mock_load_paasta_native_job_config.return_value.get_registrations.side_effect = (
            mock_registrations_side_effect
        )
        actual = (
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(
                cluster, soa_dir
            )
        )
        assert expected == actual
        pnsrh_patch.assert_called_once_with(hostname=None)
        mock_load_paasta_native_job_config.assert_any_call(
            service="no_test",
            instance="left_behind",
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        mock_load_paasta_native_job_config.assert_any_call(
            service="no_docstrings",
            instance="forever_abandoned",
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        assert mock_load_paasta_native_job_config.call_count == 2
        read_ns_config_patch.assert_any_call("no_test", "uno", soa_dir)
        read_ns_config_patch.assert_any_call("no_test", "dos", soa_dir)
        read_ns_config_patch.assert_any_call("no_test", "tres", soa_dir)
        read_ns_config_patch.assert_any_call("no_docstrings", "quatro", soa_dir)
        assert read_ns_config_patch.call_count == 4


def test_get_paasta_native_services_running_here_for_nerve_when_not_in_smartstack():
    cluster = "edelweiss"
    soa_dir = "the_sound_of_music"
    fake_marathon_services = [
        ("no_test", "left_behind", 1111),
        ("no_docstrings", "forever_abandoned", 2222),
    ]
    registrations = [["no_docstrings.dos"], ["no_test.uno"]]
    nerve_dicts = [
        long_running_service_tools.ServiceNamespaceConfig({"binary": 1}),
        long_running_service_tools.ServiceNamespaceConfig(
            {"clock": 0, "proxy_port": 6666}
        ),
    ]
    expected = [("no_test.uno", {"clock": 0, "port": 1111, "proxy_port": 6666})]
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.paasta_native_services_running_here",
        autospec=True,
        return_value=fake_marathon_services,
    ) as pnsrh_patch, mock.patch(
        "paasta_tools.native_mesos_scheduler.load_paasta_native_job_config",
        autospec=True,
    ) as mock_load_paasta_native_job_config, mock.patch(
        "paasta_tools.native_mesos_scheduler.load_service_namespace_config",
        autospec=True,
        side_effect=lambda *args, **kwargs: nerve_dicts.pop(),
    ) as read_ns_config_patch:

        def mock_registrations_side_effect(*args, **kwargs):
            return registrations.pop()

        mock_load_paasta_native_job_config.return_value.get_registrations.side_effect = (
            mock_registrations_side_effect
        )
        actual = (
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(
                cluster, soa_dir
            )
        )
        assert expected == actual
        pnsrh_patch.assert_called_once_with(hostname=None)
        mock_load_paasta_native_job_config.assert_any_call(
            service="no_test",
            instance="left_behind",
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        mock_load_paasta_native_job_config.assert_any_call(
            service="no_docstrings",
            instance="forever_abandoned",
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        assert mock_load_paasta_native_job_config.call_count == 2
        read_ns_config_patch.assert_any_call("no_test", "uno", soa_dir)
        read_ns_config_patch.assert_any_call("no_docstrings", "dos", soa_dir)
        assert read_ns_config_patch.call_count == 2


def test_get_paasta_native_services_running_here_for_nerve_when_get_cluster_raises_custom_exception():
    cluster = None
    soa_dir = "the_sound_of_music"
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.load_system_paasta_config", autospec=True
    ) as load_system_paasta_config_patch, mock.patch(
        "paasta_tools.native_mesos_scheduler.paasta_native_services_running_here",
        autospec=True,
        return_value=[],
    ):
        load_system_paasta_config_patch.return_value = mock.Mock(
            side_effect=native_mesos_scheduler.PaastaNotConfiguredError
        )
        actual = (
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(
                cluster, soa_dir
            )
        )
        assert actual == []


def test_get_paasta_native_services_running_here_for_nerve_when_paasta_not_configured():
    cluster = None
    soa_dir = "the_sound_of_music"
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.load_system_paasta_config", autospec=True
    ) as load_system_paasta_config_patch, mock.patch(
        "paasta_tools.native_mesos_scheduler.paasta_native_services_running_here",
        autospec=True,
        return_value=[],
    ):
        load_system_paasta_config_patch.return_value = mock.Mock(
            side_effect=native_mesos_scheduler.PaastaNotConfiguredError
        )
        actual = (
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(
                cluster, soa_dir
            )
        )
        assert actual == []


def test_get_paasta_native_services_running_here_for_nerve_when_get_cluster_raises_other_exception():
    cluster = None
    soa_dir = "the_sound_of_music"
    with mock.patch(
        "paasta_tools.native_mesos_scheduler.load_system_paasta_config", autospec=True
    ) as load_system_paasta_config_patch, mock.patch(
        "paasta_tools.native_mesos_scheduler.paasta_native_services_running_here",
        autospec=True,
        return_value=[],
    ):
        load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(
            side_effect=Exception
        )
        with pytest.raises(Exception):
            native_mesos_scheduler.get_paasta_native_services_running_here_for_nerve(
                cluster, soa_dir
            )
