import mock

from paasta_tools import puppet_service_tools
from paasta_tools.utils import compose_job_id


def test_get_puppet_services_that_run_here():
    with mock.patch(
        "os.listdir", autospec=True, return_value=["b", "a"]
    ) as listdir_patch, mock.patch(
        "os.path.exists",
        autospec=True,
        side_effect=lambda x: x
        in ("/etc/nerve/puppet_services.d", "/etc/nerve/puppet_services.d/a"),
    ), mock.patch(
        "paasta_tools.puppet_service_tools.open",
        create=True,
        autospec=None,
        side_effect=mock.mock_open(read_data='{"namespaces": ["main"]}'),
    ):
        puppet_services = puppet_service_tools.get_puppet_services_that_run_here()
        assert puppet_services == {"a": ["main"]}
        listdir_patch.assert_called_once_with(puppet_service_tools.PUPPET_SERVICE_DIR)


def test_get_puppet_services_running_here_for_nerve():
    with mock.patch(
        "paasta_tools.puppet_service_tools.get_puppet_services_that_run_here",
        autospec=True,
        side_effect=lambda: {"d": ["main"], "c": ["main", "canary"]},
    ), mock.patch(
        "paasta_tools.puppet_service_tools._namespaced_get_classic_service_information_for_nerve",
        autospec=True,
        side_effect=lambda x, y, _: (compose_job_id(x, y), {}),
    ):
        assert puppet_service_tools.get_puppet_services_running_here_for_nerve(
            "foo"
        ) == [
            ("c.main", {}),
            ("c.canary", {}),
            ("d.main", {}),
        ]
