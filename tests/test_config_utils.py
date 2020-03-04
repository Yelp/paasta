import os

import mock
import pytest
import yaml

import paasta_tools.config_utils as config_utils


@pytest.fixture(autouse=True)
def mock_subprocess():
    with mock.patch(
        "paasta_tools.config_utils.subprocess", autospec=True
    ) as mock_subprocess:
        yield mock_subprocess


@pytest.fixture
def updater(tmpdir):
    updater = config_utils.AutoConfigUpdater("test_source", "remote", tmp_dir=tmpdir)

    with updater:
        # make a valid service dir we can write in
        os.mkdir(os.path.join(updater.working_dir.name, "foo"))
        yield updater


def test_write_auto_config_data_service_dne(tmpdir):
    with mock.patch(
        "paasta_tools.config_utils.open", new=mock.mock_open(), autospec=None,
    ) as mock_open:
        result = config_utils.write_auto_config_data(
            service="something",
            extra_info="marathon-norcal-devc",
            data={"a": 1},
            soa_dir=tmpdir,
        )
        assert len(mock_open.mock_calls) == 0
    assert result is None


def test_write_auto_config_data_new_file(tmpdir):
    service = "foo"
    conf_file = "marathon-norcal-devc"
    data = {"a": 1}

    tmpdir.mkdir(service)
    result = config_utils.write_auto_config_data(
        service=service, extra_info=conf_file, data=data, soa_dir=tmpdir,
    )
    expected_path = (
        f"{tmpdir}/{service}/{config_utils.AUTO_SOACONFIG_SUBDIR}/{conf_file}.yaml"
    )
    assert result == expected_path
    with open(expected_path) as f:
        assert yaml.safe_load(f) == data


def test_write_auto_config_data_file_exists(tmpdir):
    service = "foo"
    conf_file = "marathon-norcal-devc"

    tmpdir.mkdir(service)
    config_utils.write_auto_config_data(
        service=service, extra_info=conf_file, data={"a": 1}, soa_dir=tmpdir,
    )
    # Contents should be replaced on second write
    result = config_utils.write_auto_config_data(
        service=service, extra_info=conf_file, data={"a": 2}, soa_dir=tmpdir,
    )
    expected_path = (
        f"{tmpdir}/{service}/{config_utils.AUTO_SOACONFIG_SUBDIR}/{conf_file}.yaml"
    )
    assert result == expected_path
    with open(expected_path) as f:
        assert yaml.safe_load(f) == {"a": 2}


@mock.patch("paasta_tools.config_utils.validate_schema", autospec=True)
def test_validate_auto_config_file_config_types(mock_validate, tmpdir):
    for config_type in config_utils.KNOWN_CONFIG_TYPES:
        filepath = f"service/{config_type}-cluster.yaml"
        config_utils.validate_auto_config_file(filepath)
        mock_validate.assert_called_with(filepath, f"auto/{config_type}")


@mock.patch("paasta_tools.config_utils.validate_schema", autospec=True)
def test_validate_auto_config_file_unknown_type(mock_validate, tmpdir):
    assert not config_utils.validate_auto_config_file("service/unknown-thing.yaml")


@pytest.mark.parametrize(
    "data,is_valid",
    [
        ({"not_an_instance": 1}, False),
        ({"instance": {"cpus": "bad_string_value"}}, False),
        ({"instance": {"cpus": 1, "invalid_key": 2}}, False),
        ({"instance": {"cpus": 1.2, "mem": 100}}, True),
    ],
)
def test_validate_auto_config_file_e2e(data, is_valid, tmpdir):
    service = "foo"
    conf_file = "marathon-norcal-devc"

    tmpdir.mkdir(service)
    filepath = config_utils.write_auto_config_data(
        service=service, extra_info=conf_file, data=data, soa_dir=tmpdir,
    )
    assert config_utils.validate_auto_config_file(filepath) == is_valid


@pytest.mark.parametrize("branch", ["master", "other_test"])
def test_auto_config_updater_context(branch, tmpdir, mock_subprocess):
    remote = "git_remote"
    updater = config_utils.AutoConfigUpdater(
        "test_source", remote, branch=branch, tmp_dir=tmpdir
    )
    initial_wd = os.getcwd()

    with updater:
        clone_dir = updater.working_dir.name
        assert os.path.isdir(clone_dir)
        expected_calls = [mock.call.check_call(["git", "clone", remote, clone_dir])]
        if branch != "master":
            expected_calls.append(
                mock.call.check_call(["git", "checkout", "-b", branch])
            )
        assert mock_subprocess.mock_calls == expected_calls
        assert os.getcwd() == clone_dir

    # Clean up after exiting context
    assert not os.path.exists(clone_dir)
    assert os.getcwd() == initial_wd


@pytest.mark.parametrize("all_valid", [True, False])
@mock.patch("paasta_tools.config_utils.validate_auto_config_file", autospec=True)
def test_auto_config_updater_validate(mock_validate_file, all_valid, updater):
    mock_validate_file.side_effect = [True, all_valid, True]

    updater.write_configs("foo", "marathon-norcal-devc", {"a": 2})
    updater.write_configs("foo", "kubernetes-norcal-devc", {"a": 2})
    updater.write_configs("foo", "kubernetes-pnw-devc", {"a": 2})
    assert updater.validate() == all_valid
    assert mock_validate_file.call_count == 3


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


@pytest.mark.parametrize(
    "validate_result,did_commit", [(False, False), (True, True), (True, False)]
)
@mock.patch("paasta_tools.config_utils._commit_files", autospec=True)
@mock.patch("paasta_tools.config_utils._push_to_remote", autospec=True)
def test_auto_config_updater_commit(
    mock_push, mock_commit, validate_result, did_commit, updater
):
    updater.files_changed = {"a", "b"}
    mock_commit.return_value = did_commit
    with mock.patch.object(
        updater, "validate", autospec=True, return_value=validate_result
    ):
        updater.commit_to_remote()

    if not validate_result:
        assert mock_commit.call_count == 0
        assert mock_push.call_count == 0
    else:
        assert mock_commit.call_count == 1
        if did_commit:
            mock_push.assert_called_once_with(updater.branch)
        else:
            assert mock_push.call_count == 0
