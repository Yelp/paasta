import os

import mock
import pytest

import paasta_tools.config_utils as config_utils
from paasta_tools import yaml_tools as yaml
from paasta_tools.utils import AUTO_SOACONFIG_SUBDIR


@pytest.fixture(autouse=True)
def mock_subprocess():
    with mock.patch(
        "paasta_tools.config_utils.subprocess", autospec=True
    ) as mock_subprocess:
        yield mock_subprocess


@pytest.fixture
def updater(tmpdir):
    updater = config_utils.AutoConfigUpdater(
        "test_source", "remote", working_dir=tmpdir
    )

    with updater:
        # make a valid service dir we can write in
        os.mkdir(os.path.join(updater.working_dir, "foo"))
        yield updater


def test_write_auto_config_data_service_dne(tmpdir):
    with mock.patch(
        "paasta_tools.config_utils.open",
        new=mock.mock_open(),
        autospec=None,
    ) as mock_open:
        result = config_utils.write_auto_config_data(
            service="something",
            extra_info="kubernetes-norcal-devc",
            data={"a": 1},
            soa_dir=tmpdir,
        )
        assert len(mock_open.mock_calls) == 0
    assert result is None


def test_write_auto_config_data_new_file(tmpdir):
    service = "foo"
    conf_file = "kubernetes-norcal-devc"
    data = {"a": 1}

    tmpdir.mkdir(service)
    result = config_utils.write_auto_config_data(
        service=service,
        extra_info=conf_file,
        data=data,
        soa_dir=tmpdir,
        sub_dir=AUTO_SOACONFIG_SUBDIR,
    )
    expected_path = f"{tmpdir}/{service}/{AUTO_SOACONFIG_SUBDIR}/{conf_file}.yaml"
    assert result == expected_path
    with open(expected_path) as f:
        assert yaml.safe_load(f) == data


def test_write_auto_config_data_file_exists(tmpdir):
    service = "foo"
    conf_file = "kubernetes-norcal-devc"

    tmpdir.mkdir(service)
    config_utils.write_auto_config_data(
        service=service,
        extra_info=conf_file,
        data={"a": 1},
        soa_dir=tmpdir,
        sub_dir=AUTO_SOACONFIG_SUBDIR,
    )
    # Contents should be replaced on second write
    result = config_utils.write_auto_config_data(
        service=service,
        extra_info=conf_file,
        data={"a": 2},
        soa_dir=tmpdir,
        sub_dir=AUTO_SOACONFIG_SUBDIR,
    )
    expected_path = f"{tmpdir}/{service}/{AUTO_SOACONFIG_SUBDIR}/{conf_file}.yaml"
    assert result == expected_path
    with open(expected_path) as f:
        assert yaml.safe_load(f) == {"a": 2}


@mock.patch("paasta_tools.config_utils.validate_schema", autospec=True)
def test_validate_auto_config_file_config_types(mock_validate, tmpdir):
    for config_type in (
        "kubernetes",
        "deploy",
        "smartstack",
        "cassandracluster",
    ):
        filepath = f"service/{config_type}-cluster.yaml"
        assert config_utils.validate_auto_config_file(filepath, AUTO_SOACONFIG_SUBDIR)
        mock_validate.assert_called_with(filepath, f"autotuned_defaults/{config_type}")


@mock.patch("paasta_tools.config_utils.validate_schema", autospec=True)
def test_validate_auto_config_file_unknown_type(mock_validate, tmpdir):
    assert not config_utils.validate_auto_config_file(
        "service/unknown-thing.yaml", AUTO_SOACONFIG_SUBDIR
    )


@pytest.mark.parametrize(
    "data,is_valid",
    [
        ({"not_an_instance": 1}, False),
        ({"instance": {"cpus": "bad_string_value"}}, False),
        ({"instance": {"cpus": 1, "invalid_key": 2}}, False),
        ({"instance": {"cpus": 1.2, "mem": 100}}, True),
        ({"instance": {"cpus": 1.2, "mem": 100, "disk": "very big"}}, False),
        ({"instance": {"cpus": 1.2, "mem": 100, "disk": 0}}, False),
        ({"instance": {"cpus": 1.2, "mem": 100, "disk": 1000}}, True),
        ({"instance": {"min_instances": 1.2}}, False),
        ({"instance": {"min_instances": 1}}, True),
    ],
)
def test_validate_auto_config_file_e2e(data, is_valid, tmpdir):
    service = "foo"
    conf_file = "kubernetes-norcal-devc"

    tmpdir.mkdir(service)
    filepath = config_utils.write_auto_config_data(
        service=service,
        extra_info=conf_file,
        data=data,
        soa_dir=tmpdir,
    )
    assert (
        config_utils.validate_auto_config_file(filepath, AUTO_SOACONFIG_SUBDIR)
        == is_valid
    )


@pytest.mark.parametrize(
    "branch, remote_branch_exists",
    [("master", True), ("other_test", True), ("other_test", False)],
)
def test_auto_config_updater_context(
    branch, remote_branch_exists, tmpdir, mock_subprocess
):
    remote = "git_remote"
    updater = config_utils.AutoConfigUpdater(
        "test_source", remote, branch=branch, working_dir=tmpdir
    )
    updater._remote_branch_exists = mock.MagicMock(
        autospec=True, return_value=remote_branch_exists
    )
    initial_wd = os.getcwd()

    with updater:
        clone_dir = updater.working_dir
        assert os.path.isdir(clone_dir)
        expected_calls = [mock.call.check_call(["git", "clone", remote, clone_dir])]
        if branch != "master":
            if remote_branch_exists:
                expected_calls.extend(
                    [
                        mock.call.check_call(["git", "fetch", "origin", branch]),
                        mock.call.check_call(
                            ["git", "checkout", "-b", branch, f"origin/{branch}"]
                        ),
                    ]
                )
            else:
                expected_calls.append(
                    mock.call.check_call(["git", "checkout", "-b", branch])
                )
        assert mock_subprocess.mock_calls == expected_calls
        assert os.getcwd() == clone_dir

    # Clean up after exiting context
    assert not os.path.exists(clone_dir)
    assert os.getcwd() == initial_wd


@pytest.mark.parametrize(
    "branch, remote_branch_exists",
    [("master", True), ("other_test", True), ("other_test", False)],
)
def test_auto_config_updater_context_no_clone(
    branch, remote_branch_exists, tmpdir, mock_subprocess
):
    remote = "git_remote"
    working_dir = tmpdir.mkdir("testing")
    updater = config_utils.AutoConfigUpdater(
        "test_source",
        remote,
        branch=branch,
        working_dir=working_dir,
        do_clone=False,
    )
    updater._remote_branch_exists = mock.MagicMock(
        autospec=True, return_value=remote_branch_exists
    )
    initial_wd = os.getcwd()

    with updater:
        if branch == "master":
            expected_calls = []
        else:
            if remote_branch_exists:
                expected_calls = [
                    mock.call.check_call(["git", "fetch", "origin", branch]),
                    mock.call.check_call(
                        ["git", "checkout", "-b", branch, f"origin/{branch}"]
                    ),
                ]
            else:
                expected_calls = [
                    mock.call.check_call(["git", "checkout", "-b", branch])
                ]
        assert mock_subprocess.mock_calls == expected_calls
        assert os.getcwd() == working_dir

    # Existing directory not deleted
    assert os.path.exists(working_dir)
    assert os.getcwd() == initial_wd


@pytest.mark.parametrize("all_valid", [True, False])
@mock.patch("paasta_tools.config_utils.validate_auto_config_file", autospec=True)
def test_auto_config_updater_validate(mock_validate_file, all_valid, updater):
    mock_validate_file.side_effect = [True, all_valid, True]

    updater.write_configs("foo", "kubernetes-norcal-devc", {"a": 2})
    updater.write_configs("foo", "eks-pnw-devc", {"a": 2})
    assert updater.validate() == all_valid
    assert mock_validate_file.call_count == 2


def test_auto_config_updater_read_write(updater):
    service = "foo"
    conf_file = "something"
    data = {"key": "g_minor"}

    # Nothing yet
    assert updater.get_existing_configs(service, conf_file) == {}

    # Write something
    updater.write_configs(service, conf_file, data)
    assert updater.get_existing_configs(service, conf_file) == data
    assert len(updater.files_changed) == 1

    # Try writing to service directory that doesn't exist, should not change anything
    updater.write_configs("baz", conf_file, data)
    assert updater.get_existing_configs("baz", conf_file) == {}
    assert len(updater.files_changed) == 1


def test_auto_config_updater_read_write_with_comments(updater):
    service = "foo"
    conf_file = "something"
    data = {"key": "g_minor", "bar": "foo"}

    with open(f"{updater.working_dir}/{service}/{conf_file}.yaml", "x") as f:
        f.write(
            """# top comment
key: c_minor # inline comment
"""
        )

    existing_data = updater.get_existing_configs(service, conf_file)
    assert existing_data == {"key": "c_minor"}

    # Update existing config and add another key
    existing_data["key"] = "g_minor"
    existing_data["bar"] = "foo"

    updater.write_configs(service, conf_file, existing_data)
    assert updater.get_existing_configs(service, conf_file) == data
    assert len(updater.files_changed) == 1
    with open(f"{updater.working_dir}/{service}/{conf_file}.yaml", "r") as f:
        assert (
            f.read()
            == """# top comment
key: g_minor # inline comment
bar: foo
"""
        )


@mock.patch("paasta_tools.config_utils._push_to_remote", autospec=True)
@mock.patch("paasta_tools.config_utils._commit_files", autospec=True)
def test_auto_config_updater_commit_validate_fails(mock_push, mock_commit, updater):
    updater.files_changed = {"a", "b"}
    with mock.patch.object(
        updater, "validate", autospec=True, return_value=False
    ), pytest.raises(config_utils.ValidationError):
        updater.commit_to_remote()

    assert mock_commit.call_count == 0
    assert mock_push.call_count == 0


@pytest.mark.parametrize("did_commit", [True, False])
@mock.patch("paasta_tools.config_utils._commit_files", autospec=True)
@mock.patch("paasta_tools.config_utils._push_to_remote", autospec=True)
def test_auto_config_updater_commit(mock_push, mock_commit, did_commit, updater):
    updater.files_changed = {"a", "b"}
    mock_commit.return_value = did_commit
    with mock.patch.object(updater, "validate", autospec=True, return_value=True):
        updater.commit_to_remote()

    assert mock_commit.call_count == 1
    if did_commit:
        mock_push.assert_called_once_with(updater.branch)
    else:
        assert mock_push.call_count == 0


def test_auto_config_updater_merge_recommendations_limits(updater):
    service = "foo"
    conf_file = "notk8s-euwest-prod"
    limited_instance = "some_instance"
    noop_instance = "other_instance"
    autotune_data = {
        limited_instance: {"cpus": 0.1, "mem": 167, "disk": 256, "cpu_burst_add": 0.5}
    }
    user_data = {
        limited_instance: {
            "cmd": "ls",
            "autotune_limits": {
                "cpus": {"min": 1, "max": 2},
                "mem": {"min": 1024, "max": 2048},
                "disk": {"min": 512, "max": 1024},
            },
        },
        noop_instance: {"cmd": "ls"},
    }

    recs = {
        (service, conf_file): {
            limited_instance: {
                "mem": 1,
                "disk": 700,
                "cpus": 3,
            },
            noop_instance: {
                "cpus": 100,
                "mem": 10000,
                "disk": 2048,
            },
        }
    }

    with mock.patch.object(
        updater,
        "get_existing_configs",
        autospec=True,
        side_effect=[
            # first get the autotune data
            autotune_data,
            # then we get both the eks- and kuberentes- data
            user_data,
            # there could be data in both of these, but for a
            # simpler test, we just assume that we're looking
            # at something that's 100% on Yelp-managed k8s
            {},
        ],
    ):
        assert updater.merge_recommendations(recs) == {
            (service, conf_file): {
                limited_instance: {
                    "mem": 1024,  # use lower bound
                    "disk": 700,  # unchanged
                    "cpus": 2,  # use upper bound
                    "cpu_burst_add": 0.5,  # no updated rec or limit, leave alone
                },
                # this instances recommendations should be left untouched
                noop_instance: {
                    "cpus": 100,
                    "mem": 10000,
                    "disk": 2048,
                },
            }
        }
