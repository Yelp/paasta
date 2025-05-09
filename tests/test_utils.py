# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import json
import os
import stat
import sys
import time
import warnings
from typing import Any
from typing import Dict
from typing import List

import mock
import pytest
from freezegun import freeze_time
from pytest import raises

from paasta_tools import utils
from paasta_tools.utils import DEFAULT_SPARK_DRIVER_POOL
from paasta_tools.utils import PoolsNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import SystemPaastaConfigDict


def test_get_git_url_provided_by_serviceyaml():
    service = "giiiiiiiiiiit"
    expected = "git@some_random_host:foobar"
    with mock.patch(
        "service_configuration_lib.read_service_configuration", autospec=True
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {"git_url": expected}
        assert utils.get_git_url(service) == expected
        mock_read_service_configuration.assert_called_once_with(
            service, soa_dir=utils.DEFAULT_SOA_DIR
        )


def test_get_git_url_default():
    service = "giiiiiiiiiiit"
    expected = "git@github.yelpcorp.com:services/%s" % service
    with mock.patch(
        "service_configuration_lib.read_service_configuration", autospec=True
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {}
        assert utils.get_git_url(service) == expected
        mock_read_service_configuration.assert_called_once_with(
            service, soa_dir=utils.DEFAULT_SOA_DIR
        )


def test_format_log_line():
    input_line = "foo"
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_component = "build"
    fake_level = "debug"
    fake_now = "fake_now"
    expected = json.dumps(
        {
            "timestamp": fake_now,
            "level": fake_level,
            "cluster": fake_cluster,
            "service": fake_service,
            "instance": fake_instance,
            "component": fake_component,
            "message": input_line,
        },
        sort_keys=True,
    )
    with mock.patch("paasta_tools.utils._now", autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_log_line(
            level=fake_level,
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            component=fake_component,
            line=input_line,
        )
        assert actual == expected


def test_deploy_whitelist_to_constraints():
    fake_whitelist = ("fake_location_type", ["fake_location", "anotherfake_location"])
    expected_constraints = [
        ["fake_location_type", "LIKE", "fake_location|anotherfake_location"]
    ]

    constraints = utils.deploy_whitelist_to_constraints(fake_whitelist)
    assert constraints == expected_constraints


def test_format_log_line_with_timestamp():
    input_line = "foo"
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_component = "build"
    fake_level = "debug"
    fake_timestamp = "fake_timestamp"
    expected = json.dumps(
        {
            "timestamp": fake_timestamp,
            "level": fake_level,
            "cluster": fake_cluster,
            "service": fake_service,
            "instance": fake_instance,
            "component": fake_component,
            "message": input_line,
        },
        sort_keys=True,
    )
    actual = utils.format_log_line(
        fake_level,
        fake_cluster,
        fake_service,
        fake_instance,
        fake_component,
        input_line,
        timestamp=fake_timestamp,
    )
    assert actual == expected


def test_format_log_line_rejects_invalid_components():
    with raises(utils.NoSuchLogComponent):
        utils.format_log_line(
            level="debug",
            cluster="fake_cluster",
            service="fake_service",
            instance="fake_instance",
            line="fake_line",
            component="BOGUS_COMPONENT",
        )


def test_format_audit_log_line_no_details():
    fake_user = "fake_user"
    fake_hostname = "fake_hostname"
    fake_action = "mark-for-deployment"
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_now = "fake_now"

    expected_dict = {
        "timestamp": fake_now,
        "cluster": fake_cluster,
        "service": fake_service,
        "instance": fake_instance,
        "user": fake_user,
        "host": fake_hostname,
        "action": fake_action,
        "action_details": {},
    }

    expected = json.dumps(expected_dict, sort_keys=True)

    with mock.patch("paasta_tools.utils._now", autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_audit_log_line(
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            user=fake_user,
            host=fake_hostname,
            action=fake_action,
        )
        assert actual == expected


def test_format_audit_log_line_with_details():
    fake_user = "fake_user"
    fake_hostname = "fake_hostname"
    fake_action = "mark-for-deployment"
    fake_action_details = {"sha": "deadbeef"}
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_now = "fake_now"

    expected_dict = {
        "timestamp": fake_now,
        "cluster": fake_cluster,
        "service": fake_service,
        "instance": fake_instance,
        "user": fake_user,
        "host": fake_hostname,
        "action": fake_action,
        "action_details": fake_action_details,
    }

    expected = json.dumps(expected_dict, sort_keys=True)

    with mock.patch("paasta_tools.utils._now", autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_audit_log_line(
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            user=fake_user,
            host=fake_hostname,
            action=fake_action,
            action_details=fake_action_details,
        )
        assert actual == expected


try:
    from paasta_tools.utils import ScribeLogWriter

    def test_ScribeLogWriter_log_raise_on_unknown_level():
        with raises(utils.NoSuchLogLevel):
            ScribeLogWriter().log("fake_service", "fake_line", "build", "BOGUS_LEVEL")

    @freeze_time("2022-12-28")
    def test_ScribeLogWriter_logs_audit_messages():
        user = "fake_user"
        host = "fake_hostname"
        action = "mark-for-deployment"
        action_details = {"sha": "deadbeef"}
        service = "fake_service"
        cluster = "fake_cluster"
        instance = "fake_instance"

        expected_log_name = utils.AUDIT_LOG_STREAM
        expected_line = utils.format_audit_log_line(
            user=user,
            host=host,
            action=action,
            action_details=action_details,
            service=service,
            cluster=cluster,
            instance=instance,
        )

        with mock.patch("paasta_tools.utils.clog", autospec=True) as mock_clog:
            slw = ScribeLogWriter(scribe_disable=True)
            slw.log_audit(
                user=user,
                host=host,
                action=action,
                action_details=action_details,
                service=service,
                cluster=cluster,
                instance=instance,
            )

        mock_clog.log_line.assert_called_once_with(expected_log_name, expected_line)

except ImportError:
    warnings.warn("ScribeLogWriter is unavailable")


def test_get_log_name_for_service():
    service = "foo"
    expected = "stream_paasta_%s" % service
    assert utils.get_log_name_for_service(service) == expected


def test_get_readable_files_in_glob_ignores_unreadable(tmpdir):
    tmpdir.join("readable.json").ensure().chmod(0o644)
    tmpdir.join("unreadable.json").ensure().chmod(0o000)
    ret = utils.get_readable_files_in_glob("*.json", tmpdir.strpath)
    assert ret == [tmpdir.join("readable.json").strpath]


def test_get_readable_files_in_glob_is_recursive(tmpdir):
    a = tmpdir.join("a.json").ensure()
    b = tmpdir.join("b.json").ensure()
    c = tmpdir.join("subdir").ensure_dir().join("c.json").ensure()
    ret = utils.get_readable_files_in_glob("*.json", tmpdir.strpath)
    assert set(ret) == {a.strpath, b.strpath, c.strpath}


def test_load_system_paasta_config():
    json_load_return_value: utils.SystemPaastaConfigDict = {"cluster": "bar"}
    expected = utils.SystemPaastaConfig(json_load_return_value, "/some/fake/dir")
    file_mock = mock.mock_open()
    with mock.patch("os.path.isdir", return_value=True, autospec=True), mock.patch(
        "os.access", return_value=True, autospec=True
    ), mock.patch(
        "builtins.open", file_mock, autospec=None
    ) as open_file_patch, mock.patch(
        "paasta_tools.utils.get_readable_files_in_glob",
        autospec=True,
        return_value=["/some/fake/dir/some_file.json"],
    ), mock.patch(
        "paasta_tools.utils.json.load",
        autospec=True,
        return_value=json_load_return_value,
    ) as json_patch, mock.patch(
        "paasta_tools.utils.os.stat", autospec=True
    ), mock.patch(
        "paasta_tools.utils.deep_merge_dictionaries",
        autospec=True,
        return_value=json_load_return_value,
    ) as mock_deep_merge:
        actual = utils.load_system_paasta_config(path="/some/fake/dir")
        assert actual == expected
        # Kinda weird but without this load_system_paasta_config() can (and
        # did! during development) return a plain dict without the test
        # complaining.
        assert actual.__class__ == expected.__class__
        open_file_patch.assert_any_call("/some/fake/dir/some_file.json")
        json_patch.assert_any_call(file_mock.return_value.__enter__.return_value)
        assert json_patch.call_count == 1
        mock_deep_merge.assert_called_with(
            json_load_return_value, {}, allow_duplicate_keys=False
        )


def test_load_system_paasta_config_file_non_existent_dir():
    fake_path = "/var/dir_of_fake"
    with mock.patch("os.path.isdir", return_value=False, autospec=True):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        expected = (
            "Could not find system paasta configuration directory: %s" % fake_path
        )
        assert str(excinfo.value) == expected


def test_load_system_paasta_config_file_non_readable_dir():
    fake_path = "/var/dir_of_fake"
    with mock.patch("os.path.isdir", return_value=True, autospec=True), mock.patch(
        "os.access", return_value=False, autospec=True
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        expected = (
            "Could not read from system paasta configuration directory: %s" % fake_path
        )
        assert str(excinfo.value) == expected


def test_load_system_paasta_config_file_dne():
    fake_path = "/var/dir_of_fake"
    with mock.patch("os.path.isdir", return_value=True, autospec=True), mock.patch(
        "os.access", return_value=True, autospec=True
    ), mock.patch(
        "builtins.open", side_effect=IOError(2, "a", "b"), autospec=None
    ), mock.patch(
        "paasta_tools.utils.os.stat", autospec=True
    ), mock.patch(
        "paasta_tools.utils.get_readable_files_in_glob",
        autospec=True,
        return_value=[fake_path],
    ):
        with raises(utils.PaastaNotConfiguredError) as excinfo:
            utils.load_system_paasta_config(fake_path)
        assert str(excinfo.value) == "Could not load system paasta config file b: a"


def test_load_system_paasta_config_duplicate_keys_errors():
    fake_file_a = {
        "cluster": "this value will be overridden",
        "sensu_host": "fake_data",
    }
    fake_file_b = {"cluster": "overriding value"}
    file_mock = mock.mock_open()
    with mock.patch("os.path.isdir", return_value=True, autospec=True), mock.patch(
        "os.access", return_value=True, autospec=True
    ), mock.patch("builtins.open", file_mock, autospec=None), mock.patch(
        "paasta_tools.utils.os.stat", autospec=True
    ), mock.patch(
        "paasta_tools.utils.get_readable_files_in_glob",
        autospec=True,
        return_value=["a", "b"],
    ), mock.patch(
        "paasta_tools.utils.json.load",
        autospec=True,
        side_effect=[fake_file_a, fake_file_b],
    ):
        with raises(utils.DuplicateKeyError):
            utils.load_system_paasta_config(path="/some/fake/dir")


def test_SystemPaastaConfig_get_cluster():
    fake_config = utils.SystemPaastaConfig({"cluster": "peanut"}, "/some/fake/dir")
    expected = "peanut"
    actual = fake_config.get_cluster()
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_dne():
    fake_config = utils.SystemPaastaConfig({}, "/some/fake/dir")
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_cluster()


def test_SystemPaastaConfig_get_volumes():
    fake_config = utils.SystemPaastaConfig(
        {
            "volumes": [
                {"hostPath": "fake_other_path", "containerPath": "/blurp", "mode": "ro"}
            ]
        },
        "/some/fake/dir",
    )
    expected = [
        {"hostPath": "fake_other_path", "containerPath": "/blurp", "mode": "ro"}
    ]
    actual = fake_config.get_volumes()
    assert actual == expected


def test_SystemPaastaConfig_get_tron_default_pool_override():
    fake_config = utils.SystemPaastaConfig(
        {"tron_default_pool_override": "spam"},
        "/some/fake/dir",
    )
    actual = fake_config.get_tron_default_pool_override()
    expected = "spam"
    assert actual == expected


@pytest.mark.parametrize(
    argnames=["paasta_config", "expected_pool"],
    argvalues=[
        pytest.param({}, DEFAULT_SPARK_DRIVER_POOL, id="default"),
        pytest.param(
            {"default_spark_driver_pool_override": "spam-stable"},
            "spam-stable",
            id="spam",
        ),
    ],
)
def test_SystemPaastaConfig_get_default_spark_driver_pool_override(
    paasta_config, expected_pool
):
    fake_config = utils.SystemPaastaConfig(
        paasta_config,
        "/some/fake/dir",
    )

    actual_pool = fake_config.get_default_spark_driver_pool_override()

    assert actual_pool == expected_pool


def test_SystemPaastaConfig_get_hacheck_sidecar_volumes():
    fake_config = utils.SystemPaastaConfig(
        {
            "hacheck_sidecar_volumes": [
                {"hostPath": "fake_other_path", "containerPath": "/blurp", "mode": "ro"}
            ]
        },
        "/some/fake/dir",
    )
    expected = [
        {"hostPath": "fake_other_path", "containerPath": "/blurp", "mode": "ro"}
    ]
    actual = fake_config.get_hacheck_sidecar_volumes()
    assert actual == expected


def test_SystemPaastaConfig_get_volumes_dne():
    fake_config = utils.SystemPaastaConfig({}, "/some/fake/dir")
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_volumes()


def test_SystemPaastaConfig_get_zk():
    fake_config = utils.SystemPaastaConfig(
        {"zookeeper": "zk://fake_zookeeper_host"}, "/some/fake/dir"
    )
    expected = "fake_zookeeper_host"
    actual = fake_config.get_zk_hosts()
    assert actual == expected


def test_SystemPaastaConfig_get_zk_dne():
    fake_config = utils.SystemPaastaConfig({}, "/some/fake/dir")
    with raises(utils.PaastaNotConfiguredError):
        fake_config.get_zk_hosts()


def test_get_service_registry():
    fake_registry = "fake_registry"
    fake_service_config = {
        "description": "This service is fake",
        "external_link": "www.yelp.com",
        "git_url": "git@mercurial-scm.org:fake-service",
        "docker_registry": fake_registry,
    }
    with mock.patch(
        "service_configuration_lib.read_service_configuration",
        return_value=fake_service_config,
        autospec=True,
    ):
        actual = utils.get_service_docker_registry("fake_service", "fake_soa_dir")
        assert actual == fake_registry


def test_get_service_registry_dne():
    fake_registry = "fake_registry"
    fake_service_config = {
        "description": "This service is fake",
        "external_link": "www.yelp.com",
        "git_url": "git@mercurial-scm.org:fake-service",
        # no docker_registry configured for this service
    }
    fake_system_config = utils.SystemPaastaConfig(
        {"docker_registry": fake_registry}, "/some/fake/dir"
    )
    with mock.patch(
        "service_configuration_lib.read_service_configuration",
        return_value=fake_service_config,
        autospec=True,
    ):
        with mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            return_value=fake_system_config,
            autospec=True,
        ):
            actual = utils.get_service_docker_registry("fake_service", "fake_soa_dir")
            assert actual == fake_registry


def test_SystemPaastaConfig_get_sensu_host_default():
    fake_config = utils.SystemPaastaConfig({}, "/some/fake/dir")
    actual = fake_config.get_sensu_host()
    expected = "localhost"
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_host():
    fake_config = utils.SystemPaastaConfig({"sensu_host": "blurp"}, "/some/fake/dir")
    actual = fake_config.get_sensu_host()
    expected = "blurp"
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_host_None():
    fake_config = utils.SystemPaastaConfig({"sensu_host": None}, "/some/fake/dir")
    actual = fake_config.get_sensu_host()
    expected = None
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_port_default():
    fake_config = utils.SystemPaastaConfig({}, "/some/fake/dir")
    actual = fake_config.get_sensu_port()
    expected = 3030
    assert actual == expected


def test_SystemPaastaConfig_get_sensu_port():
    fake_config = utils.SystemPaastaConfig({"sensu_port": 4040}, "/some/fake/dir")
    actual = fake_config.get_sensu_port()
    expected = 4040
    assert actual == expected


def test_SystemPaastaConfig_get_metrics_provider():
    fake_config = utils.SystemPaastaConfig(
        {"deployd_metrics_provider": "bar"}, "/some/fake/dir"
    )
    actual = fake_config.get_metrics_provider()
    expected = "bar"
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_fqdn_format_default():
    fake_config = utils.SystemPaastaConfig({}, "/some/fake/dir")
    actual = fake_config.get_cluster_fqdn_format()
    expected = "{cluster:s}.paasta"
    assert actual == expected


def test_SystemPaastaConfig_get_cluster_fqdn_format():
    fake_config = utils.SystemPaastaConfig(
        {"cluster_fqdn_format": "paasta-{cluster:s}.something"}, "/some/fake/dir"
    )
    actual = fake_config.get_cluster_fqdn_format()
    expected = "paasta-{cluster:s}.something"
    assert actual == expected


@pytest.mark.parametrize(
    "config,expected_git,expected_primary",
    [
        ({}, "sysgit.yelpcorp.com", "sysgit.yelpcorp.com"),
        (
            {
                "git_config": {
                    "repos": {
                        "yelpsoa-configs": {
                            "git_server": "a_server",
                            "deploy_server": "b_server",
                        },
                    },
                },
            },
            "a_server",
            "b_server",
        ),
    ],
)
def test_SystemPaastaConfig_get_git_config(config, expected_git, expected_primary):
    fake_config = utils.SystemPaastaConfig(config, "/some/fake/dir")

    actual_git = fake_config.get_git_config()
    actual_repo = fake_config.get_git_repo_config("yelpsoa-configs")

    assert actual_git["repos"]["yelpsoa-configs"]["git_server"] == expected_git
    assert actual_repo["git_server"] == expected_git
    assert actual_git["repos"]["yelpsoa-configs"]["deploy_server"] == expected_primary
    assert actual_repo["deploy_server"] == expected_primary


@pytest.fixture
def umask_022():
    old_umask = os.umask(0o022)
    yield
    os.umask(old_umask)


def test_atomic_file_write_itest(umask_022, tmpdir):
    target_file_name = tmpdir.join("test_atomic_file_write_itest.txt").strpath

    with open(target_file_name, "w") as f_before:
        f_before.write("old content")

    with utils.atomic_file_write(target_file_name) as f_new:
        f_new.write("new content")

        with open(target_file_name) as f_existing:
            # While in the middle of an atomic_file_write, the existing
            # file should still contain the old content, and should not
            # be truncated, etc.
            assert f_existing.read() == "old content"

    with open(target_file_name) as f_done:
        # once we're done, the content should be in place.
        assert f_done.read() == "new content"

    file_stat = os.stat(target_file_name)
    assert stat.S_ISREG(file_stat.st_mode)
    assert stat.S_IMODE(file_stat.st_mode) == 0o0644


def test_configure_log():
    fake_log_writer_config = {"driver": "fake", "options": {"fake_arg": "something"}}
    with mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config().get_log_writer.return_value = (
            fake_log_writer_config
        )
        with mock.patch(
            "paasta_tools.utils.get_log_writer_class", autospec=True
        ) as mock_get_log_writer_class:
            utils.configure_log()
            mock_get_log_writer_class.assert_called_once_with("fake")
            mock_get_log_writer_class("fake").assert_called_once_with(
                fake_arg="something"
            )


def test_compose_job_id_without_hashes():
    fake_service = "my_cool_service"
    fake_instance = "main"
    expected = "my_cool_service.main"
    actual = utils.compose_job_id(fake_service, fake_instance)
    assert actual == expected


def test_compose_job_id_with_git_hash():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_git_hash = "git123abc"
    with raises(utils.InvalidJobNameError):
        utils.compose_job_id(fake_service, fake_instance, git_hash=fake_git_hash)


def test_compose_job_id_with_config_hash():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_config_hash = "config456def"
    with raises(utils.InvalidJobNameError):
        utils.compose_job_id(fake_service, fake_instance, config_hash=fake_config_hash)


def test_compose_job_id_with_hashes():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_git_hash = "git123abc"
    fake_config_hash = "config456def"
    expected = "my_cool_service.main.git123abc.config456def"
    actual = utils.compose_job_id(
        fake_service, fake_instance, fake_git_hash, fake_config_hash
    )
    assert actual == expected


def test_decompose_job_id_too_short():
    with raises(utils.InvalidJobNameError):
        utils.decompose_job_id("foo")


def test_decompose_job_id_without_hashes():
    fake_job_id = "my_cool_service.main"
    expected = ("my_cool_service", "main", None, None)
    actual = utils.decompose_job_id(fake_job_id)
    assert actual == expected


def test_decompose_job_id_with_hashes():
    fake_job_id = "my_cool_service.main.git123abc.config456def"
    expected = ("my_cool_service", "main", "git123abc", "config456def")
    actual = utils.decompose_job_id(fake_job_id)
    assert actual == expected


def test_build_docker_image_name():
    registry_url = "fake_registry"
    upstream_job_name = "a_really_neat_service"
    expected = f"{registry_url}/services-{upstream_job_name}"
    with mock.patch(
        "paasta_tools.utils.get_service_docker_registry",
        autospec=True,
        return_value=registry_url,
    ):
        actual = utils.build_docker_image_name(upstream_job_name)
    assert actual == expected


@mock.patch("paasta_tools.utils.build_docker_image_name", autospec=True)
def test_build_docker_tag(mock_build_docker_image_name):
    upstream_job_name = "foo"
    upstream_git_commit = "bar"
    mock_build_docker_image_name.return_value = "fake-registry/services-foo"
    expected = f"fake-registry/services-foo:paasta-{upstream_git_commit}"
    actual = utils.build_docker_tag(upstream_job_name, upstream_git_commit)
    assert actual == expected


@mock.patch("paasta_tools.utils.build_docker_image_name", autospec=True)
def test_check_docker_image_false(mock_build_docker_image_name):
    mock_build_docker_image_name.return_value = "fake-registry/services-foo"
    fake_app = "fake_app"
    fake_commit = "fake_commit"
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch(
        "paasta_tools.utils.get_docker_client", autospec=True
    ) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.images.return_value = [
            {
                "Created": 1425430339,
                "VirtualSize": 250344331,
                "ParentId": "1111",
                "RepoTags": [docker_tag],
                "Id": "ef978820f195dede62e206bbd41568463ab2b79260bc63835a72154fe7e196a2",
                "Size": 0,
            }
        ]
        assert utils.check_docker_image("test_service", "tag2") is False


@pytest.mark.parametrize(("fake_image_version",), ((None,), ("extrastuff",)))
@mock.patch("paasta_tools.utils.build_docker_image_name", autospec=True)
def test_check_docker_image_true(mock_build_docker_image_name, fake_image_version):
    fake_app = "fake_app"
    fake_commit = "fake_commit"
    mock_build_docker_image_name.return_value = "fake-registry/services-foo"
    docker_tag = utils.build_docker_tag(fake_app, fake_commit, fake_image_version)
    with mock.patch(
        "paasta_tools.utils.get_docker_client", autospec=True
    ) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.images.return_value = [
            {
                "Created": 1425430339,
                "VirtualSize": 250344331,
                "ParentId": "1111",
                "RepoTags": [docker_tag],
                "Id": "ef978820f195dede62e206bbd41568463ab2b79260bc63835a72154fe7e196a2",
                "Size": 0,
            }
        ]
        assert (
            utils.check_docker_image(fake_app, fake_commit, fake_image_version) is True
        )


def test_remove_ansi_escape_sequences():
    plain_string = "blackandwhite"
    colored_string = "\033[34m" + plain_string + "\033[0m"
    assert utils.remove_ansi_escape_sequences(colored_string) == plain_string


def test_missing_cluster_configs_are_ignored():
    fake_soa_dir = "/nail/etc/services"
    fake_cluster_configs = [
        "/nail/etc/services/service1/kubernetes-cluster1.yaml",
        "/nail/etc/services/service2/kubernetes-cluster2.yaml",
    ]
    fake_system_paasta_config = utils.SystemPaastaConfig(
        {"clusters": ["cluster1", "cluster2"]}, fake_soa_dir
    )
    expected: List[Any] = []
    with mock.patch(
        "os.path.join", autospec=True, return_value="%s/*" % fake_soa_dir
    ) as mock_join_path, mock.patch(
        "glob.glob", autospec=True, return_value=fake_cluster_configs
    ) as mock_glob, mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        return_value=fake_system_paasta_config,
        autospec=True,
    ):
        actual = utils.list_clusters(soa_dir=fake_soa_dir)
        assert actual == expected
        mock_join_path.assert_called_once_with(fake_soa_dir, "*")
        mock_glob.assert_called_once_with("%s/*/*.yaml" % fake_soa_dir)


def test_list_clusters_no_service_given_lists_all_of_them():
    fake_soa_dir = "/nail/etc/services"
    fake_soa_cluster_configs = [
        ["cluster1", "/nail/etc/services/service1/kubernetes-cluster1.yaml"],
        ["cluster2", "/nail/etc/services/service1/kubernetes-cluster2.yaml"],
    ]
    expected = ["cluster1", "cluster2"]
    with mock.patch(
        "paasta_tools.utils.get_soa_cluster_deploy_files",
        autospec=True,
        return_value=fake_soa_cluster_configs,
    ):
        actual = utils.list_clusters(soa_dir=fake_soa_dir)
        assert actual == expected


def test_list_clusters_with_service():
    fake_soa_dir = "/nail/etc/services"
    fake_service = "fake_service"
    fake_soa_cluster_configs = [
        ["cluster1", "/nail/etc/services/service1/kubernetes-cluster1.yaml"],
        ["cluster2", "/nail/etc/services/service1/kubernetes-cluster2.yaml"],
    ]
    expected = ["cluster1", "cluster2"]
    with mock.patch(
        "paasta_tools.utils.get_soa_cluster_deploy_files",
        autospec=True,
        return_value=fake_soa_cluster_configs,
    ):
        actual = utils.list_clusters(fake_service, fake_soa_dir)
        assert actual == expected


def test_list_clusters_ignores_bogus_clusters():
    fake_soa_dir = "/nail/etc/services"
    fake_service = "fake_service"
    fake_cluster_configs = [
        "/nail/etc/services/service1/kubernetes-cluster1.yaml",
        "/nail/etc/services/service1/kubernetes-PROD.yaml",
        "/nail/etc/services/service1/kubernetes-cluster2.yaml",
        "/nail/etc/services/service1/kubernetes-SHARED.yaml",
    ]
    fake_system_paasta_config = utils.SystemPaastaConfig(
        {"clusters": ["cluster1", "cluster2"]}, fake_soa_dir
    )
    expected = ["cluster1", "cluster2"]
    with mock.patch(
        "os.path.join", autospec=True, return_value=f"{fake_soa_dir}/{fake_service}"
    ), mock.patch(
        "glob.glob", autospec=True, return_value=fake_cluster_configs
    ), mock.patch(
        "builtins.open", autospec=None, path=mock.mock_open(read_data="fakedata")
    ), mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        return_value=fake_system_paasta_config,
        autospec=True,
    ):
        actual = utils.list_clusters(service=fake_service)

        assert actual == expected


def test_list_all_instances_for_service():
    service = "fake_service"
    clusters = ["fake_cluster"]
    mock_instances = [(service, "instance1"), (service, "instance2")]
    expected = {"instance1", "instance2"}
    with mock.patch(
        "paasta_tools.utils.list_clusters", autospec=True
    ) as mock_list_clusters, mock.patch(
        "paasta_tools.utils.get_service_instance_list", autospec=True
    ) as mock_service_instance_list:
        mock_list_clusters.return_value = clusters
        mock_service_instance_list.return_value = mock_instances
        actual = utils.list_all_instances_for_service(service)
        assert actual == expected
        mock_list_clusters.assert_called_once_with(service, soa_dir=mock.ANY)
        mock_service_instance_list.assert_called_once_with(
            service, clusters[0], None, soa_dir=mock.ANY
        )


def test_get_service_instance_list():
    fake_name = "hint"
    fake_instance_1 = "unsweet"
    fake_instance_2 = "water"
    fake_cluster = "16floz"
    fake_dir = "/nail/home/hipster"
    fake_instances = [(fake_name, fake_instance_1), (fake_name, fake_instance_2)]
    expected = fake_instances * len(utils.INSTANCE_TYPES)
    with mock.patch(
        "paasta_tools.utils.read_service_instance_names",
        autospec=True,
        return_value=fake_instances,
    ) as read_instance_names_patch:
        actual = utils.get_service_instance_list(
            fake_name, fake_cluster, soa_dir=fake_dir
        )
        for instance_type in utils.INSTANCE_TYPES:
            read_instance_names_patch.assert_any_call(
                fake_name, instance_type, fake_cluster, soa_dir=fake_dir
            )
        assert read_instance_names_patch.call_count == len(utils.INSTANCE_TYPES)
        assert sorted(expected) == sorted(actual)


def test_read_service_instance_names_ignores_underscore():
    fake_name = "hint"
    fake_type = "drink"
    fake_instance_1 = "unsweet"
    fake_instance_2 = "_ignore_me"
    fake_cluster = "16floz"
    fake_dir = "/nail/home/hipster"
    fake_job_config: Dict[str, Dict] = {fake_instance_1: {}, fake_instance_2: {}}
    expected = [
        (fake_name, fake_instance_1),
    ]
    with mock.patch(
        "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
        autospec=True,
        return_value=fake_job_config,
    ):
        actual = utils.read_service_instance_names(
            service=fake_name,
            instance_type=fake_type,
            cluster=fake_cluster,
            soa_dir=fake_dir,
        )
        assert sorted(expected) == sorted(actual)


def test_read_service_instance_names_tron():
    fake_tron_job_config: Dict[str, Dict[str, Any]] = {
        "_template": {"foo": "bar"},
        "job1": {"actions": {"actionA": {}, "actionB": {}}},
        "job2": {"actions": {"actionC": {}, "actionD": {}}},
    }
    expected = [
        ("fake_service", "job1.actionA"),
        ("fake_service", "job1.actionB"),
        ("fake_service", "job2.actionC"),
        ("fake_service", "job2.actionD"),
    ]
    with mock.patch(
        "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
        autospec=True,
        return_value=fake_tron_job_config,
    ):
        actual = utils.read_service_instance_names(
            service="fake_service",
            instance_type="tron",
            cluster="fake",
            soa_dir="fake_dir",
        )
        assert sorted(expected) == sorted(actual)


@mock.patch(
    "paasta_tools.utils.load_service_instance_auto_configs",
    autospec=True,
)
@mock.patch(
    "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
def test_load_service_instance_configs(
    mock_read_extra_service_information, mock_load_auto_configs
):
    mock_read_extra_service_information.return_value = {
        "foo": {"cpus": 1},
        "bar": {"cpus": 1},
    }
    mock_load_auto_configs.return_value = {
        "bar": {"mem": 100},
        "baz": {"mem": 200},
    }
    expected = {
        "foo": {"cpus": 1},
        "bar": {"cpus": 1, "mem": 100},
    }
    result = utils.load_service_instance_configs(
        service="fake_service",
        instance_type="kubernetes",
        cluster="fake",
        soa_dir="fake_dir",
    )
    assert result == expected
    mock_read_extra_service_information.assert_called_with(
        "fake_service",
        "kubernetes-fake",
        soa_dir="fake_dir",
        deepcopy=False,
    )
    mock_load_auto_configs.assert_called_with(
        service="fake_service",
        instance_type="kubernetes",
        cluster="fake",
        soa_dir="fake_dir",
    )


@mock.patch(
    "paasta_tools.utils.load_service_instance_auto_configs",
    autospec=True,
)
@mock.patch(
    "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
def test_flink_load_service_instance_configs(
    mock_read_extra_service_information, mock_load_auto_configs
):
    mock_read_extra_service_information.return_value = {
        "foo": {
            "taskmanager": {"cpus": 1},
            "jobmanager": {"mem": 2},
            "supervisor": {"cpus": 2},
        },
    }
    mock_load_auto_configs.return_value = {
        "foo": {"taskmanager": {"mem": 2}},
    }
    expected = {
        "foo": {
            "taskmanager": {"cpus": 1, "mem": 2},
            "jobmanager": {"mem": 2},
            "supervisor": {"cpus": 2},
        }
    }
    result = utils.load_service_instance_configs(
        service="fake_service",
        instance_type="flink",
        cluster="fake",
        soa_dir="fake_dir",
    )
    assert result == expected
    mock_read_extra_service_information.assert_called_with(
        "fake_service",
        "flink-fake",
        soa_dir="fake_dir",
        deepcopy=False,
    )
    mock_load_auto_configs.assert_called_with(
        service="fake_service",
        instance_type="flink",
        cluster="fake",
        soa_dir="fake_dir",
    )


def test_load_service_instance_config_underscore():
    with pytest.raises(utils.InvalidJobNameError):
        utils.load_service_instance_config(
            service="fake_service",
            instance="_underscore",
            instance_type="kubernetes",
            cluster="fake",
            soa_dir="fake_dir",
        )


@mock.patch(
    "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
def test_load_service_instance_config_not_found(mock_read_service_information):
    mock_read_service_information.return_value = {"bar": {"cpus": 10}}
    with pytest.raises(utils.NoConfigurationForServiceError):
        utils.load_service_instance_config(
            service="fake_service",
            instance="foo",
            instance_type="kubernetes",
            cluster="fake",
            soa_dir="fake_dir",
        )


@mock.patch(
    "paasta_tools.utils.load_service_instance_auto_configs",
    autospec=True,
)
@mock.patch(
    "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
@pytest.mark.parametrize(
    "user_config,auto_config,expected_config",
    [
        # Nothing in auto_config for 'foo'
        ({"foo": {"cpus": 2}}, {"bar": {"cpus": 3}}, {"cpus": 2}),
        # User config overrides auto_config for 'foo' cpus
        ({"foo": {"cpus": 2, "mem": 80}}, {"foo": {"cpus": 3}}, {"cpus": 2, "mem": 80}),
        # auto_config used for 'foo' cpus
        ({"foo": {"mem": 80}}, {"foo": {"cpus": 3}}, {"cpus": 3, "mem": 80}),
    ],
)
def test_load_service_instance_config(
    mock_read_extra_service_information,
    mock_load_auto_configs,
    user_config,
    auto_config,
    expected_config,
):
    mock_read_extra_service_information.return_value = user_config
    mock_load_auto_configs.return_value = auto_config
    result = utils.load_service_instance_config(
        service="fake_service",
        instance="foo",
        instance_type="kubernetes",
        cluster="fake",
        soa_dir="fake_dir",
    )
    assert result == expected_config
    mock_read_extra_service_information.assert_called_with(
        "fake_service",
        "kubernetes-fake",
        soa_dir="fake_dir",
        deepcopy=False,
    )
    mock_load_auto_configs.assert_called_with(
        service="fake_service",
        instance_type="kubernetes",
        cluster="fake",
        soa_dir="fake_dir",
    )


@mock.patch(
    "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
@mock.patch(
    "paasta_tools.utils.load_system_paasta_config",
    autospec=True,
)
@pytest.mark.parametrize("instance_type_enabled", [(True,), (False,)])
def test_load_service_instance_auto_configs_no_aliases(
    mock_load_system_paasta_config,
    mock_read_extra_service_information,
    instance_type_enabled,
):
    mock_load_system_paasta_config.return_value.get_auto_config_instance_types_enabled.return_value = {
        "kubernetes": instance_type_enabled,
    }
    mock_load_system_paasta_config.return_value.get_auto_config_instance_type_aliases.return_value = (
        {}
    )
    result = utils.load_service_instance_auto_configs(
        service="fake_service",
        instance_type="kubernetes",
        cluster="fake",
        soa_dir="fake_dir",
    )
    if instance_type_enabled:
        mock_read_extra_service_information.assert_called_with(
            "fake_service",
            f"{utils.AUTO_SOACONFIG_SUBDIR}/kubernetes-fake",
            soa_dir="fake_dir",
            deepcopy=False,
        )
        assert result == mock_read_extra_service_information.return_value
    else:
        assert result == {}


@pytest.mark.parametrize(
    "instance_type_aliases, instance_type, expected_instance_type",
    (({}, "kubernetes", "kubernetes"), ({"eks": "kubernetes"}, "eks", "kubernetes")),
)
def test_load_service_instance_auto_configs_with_autotune_aliases(
    instance_type_aliases, instance_type, expected_instance_type
):
    with mock.patch(
        "paasta_tools.utils.service_configuration_lib.read_extra_service_information",
        autospec=True,
    ) as mock_read_extra_service_information, mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value.get_auto_config_instance_types_enabled.return_value = {
            expected_instance_type: True,
        }
        mock_load_system_paasta_config.return_value.get_auto_config_instance_type_aliases.return_value = (
            instance_type_aliases
        )
        result = utils.load_service_instance_auto_configs(
            service="fake_service",
            instance_type=instance_type,
            cluster="fake",
            soa_dir="fake_dir",
        )
        mock_read_extra_service_information.assert_called_with(
            "fake_service",
            f"{utils.AUTO_SOACONFIG_SUBDIR}/{expected_instance_type}-fake",
            soa_dir="fake_dir",
            deepcopy=False,
        )
        assert result == mock_read_extra_service_information.return_value


def test_get_services_for_cluster():
    cluster = "honey_bunches_of_oats"
    soa_dir = "completely_wholesome"
    instances = [
        [("fake_service1", "this_is_testing"), ("fake_service1", "all_the_things")],
        [("fake_service2", "my_nerf_broke")],
    ]
    expected = [
        ("fake_service2", "my_nerf_broke"),
        ("fake_service1", "this_is_testing"),
        ("fake_service1", "all_the_things"),
    ]
    with mock.patch(
        "os.path.abspath", autospec=True, return_value="chex_mix"
    ) as abspath_patch, mock.patch(
        "os.listdir", autospec=True, return_value=["dir1", "dir2"]
    ) as listdir_patch, mock.patch(
        "paasta_tools.utils.get_service_instance_list",
        side_effect=lambda a, b, c, d: instances.pop(),
        autospec=True,
    ) as get_instances_patch:
        actual = utils.get_services_for_cluster(cluster, soa_dir=soa_dir)
        assert expected == actual
        abspath_patch.assert_called_once_with(soa_dir)
        listdir_patch.assert_called_once_with("chex_mix")
        get_instances_patch.assert_any_call("dir1", cluster, None, soa_dir)
        get_instances_patch.assert_any_call("dir2", cluster, None, soa_dir)
        assert get_instances_patch.call_count == 2


def test_color_text():
    expected = f"{utils.PaastaColors.RED}hi{utils.PaastaColors.DEFAULT}"
    actual = utils.PaastaColors.color_text(utils.PaastaColors.RED, "hi")
    assert actual == expected


def test_color_text_nested():
    expected = "{}red{}blue{}red{}".format(
        utils.PaastaColors.RED,
        utils.PaastaColors.BLUE,
        utils.PaastaColors.DEFAULT + utils.PaastaColors.RED,
        utils.PaastaColors.DEFAULT,
    )
    actual = utils.PaastaColors.color_text(
        utils.PaastaColors.RED, "red%sred" % utils.PaastaColors.blue("blue")
    )
    assert actual == expected


def test_color_text_with_no_color():
    expected = "hi"
    with mock.patch.dict(os.environ, {"NO_COLOR": "1"}):
        actual = utils.PaastaColors.color_text(utils.PaastaColors.RED, "hi")
    assert actual == expected


def test_DeploymentsJson_read():
    file_mock = mock.mock_open()
    fake_dir = "/var/dir_of_fake"
    fake_path = "/var/dir_of_fake/fake_service/deployments.json"
    fake_json = {
        "v1": {
            "no_srv:blaster": {
                "docker_image": "test_rocker:9.9",
                "desired_state": "start",
                "force_bounce": None,
            },
            "dont_care:about": {
                "docker_image": "this:guy",
                "desired_state": "stop",
                "force_bounce": "12345",
            },
        }
    }
    with mock.patch(
        "builtins.open", file_mock, autospec=None
    ) as open_patch, mock.patch(
        "json.load", autospec=True, return_value=fake_json
    ) as json_patch, mock.patch(
        "paasta_tools.utils.os.path.isfile", autospec=True, return_value=True
    ):
        actual = utils.load_deployments_json("fake_service", fake_dir)
        open_patch.assert_called_once_with(fake_path)
        json_patch.assert_called_once_with(
            file_mock.return_value.__enter__.return_value
        )
        assert actual == utils.DeploymentsJsonV1(fake_json["v1"])  # type: ignore


def test_get_running_mesos_docker_containers():

    fake_container_data = [
        {
            "Status": "Up 2 hours",
            "Names": ["/mesos-legit.e1ad42eb-3ed7-4c9b-8711-aff017ef55a5"],
            "Id": "05698f4156c4f30c8dcd747f7724b14c9af7771c9a4b96fdd6aa37d6419a12a3",
        },
        {
            "Status": "Up 3 days",
            "Names": [
                "/definitely_not_meeeeesos-.6d2fb3aa-2fef-4f98-8fed-df291481e91f"
            ],
            "Id": "ae66e2c3fe3c4b2a7444212592afea5cc6a4d8ca70ee595036b19949e00a257c",
        },
    ]

    with mock.patch(
        "paasta_tools.utils.get_docker_client", autospec=True
    ) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.containers.return_value = fake_container_data
        assert len(utils.get_running_mesos_docker_containers()) == 1


def test_run_cancels_timer_thread_on_keyboard_interrupt():
    mock_process = mock.Mock()
    mock_timer_object = mock.Mock()
    with mock.patch(
        "paasta_tools.utils.Popen", autospec=True, return_value=mock_process
    ), mock.patch(
        "paasta_tools.utils.threading.Timer",
        autospec=True,
        return_value=mock_timer_object,
    ):
        mock_process.stdout.readline.side_effect = KeyboardInterrupt
        with raises(KeyboardInterrupt):
            utils._run("sh echo foo", timeout=10)
        assert mock_timer_object.cancel.call_count == 1


def test_run_returns_when_popen_fails():
    fake_exception = OSError(1234, "fake error")
    with mock.patch(
        "paasta_tools.utils.Popen", autospec=True, side_effect=fake_exception
    ):
        return_code, output = utils._run("nonexistent command", timeout=10)
    assert return_code == 1234
    assert "fake error" in output


@pytest.mark.parametrize(
    ("dcts", "expected"),
    (
        ([{"a": "b"}, {"c": "d"}], [{"a": "b"}, {"c": "d"}]),
        ([{"c": "d"}, {"a": "b"}], [{"a": "b"}, {"c": "d"}]),
        ([{"a": "b", "c": "d"}, {"a": "b"}], [{"a": "b"}, {"a": "b", "c": "d"}]),
    ),
)
def test_sort_dcts(dcts, expected):
    assert utils.sort_dicts(dcts) == expected


class TestInstanceConfig:
    def test_repr(self):
        actual = repr(
            utils.InstanceConfig(
                service="fakeservice",
                instance="fakeinstance",
                cluster="fakecluster",
                config_dict={},
                branch_dict={
                    "git_sha": "d15ea5e",
                    "docker_image": "docker_image",
                    "image_version": None,
                    "desired_state": "start",
                    "force_bounce": None,
                },
            )
        )
        expect = "InstanceConfig('fakeservice', 'fakeinstance', 'fakecluster', {}, {'git_sha': 'd15ea5e', 'docker_image': 'docker_image', 'image_version': None, 'desired_state': 'start', 'force_bounce': None}, '/nail/etc/services')"
        assert actual == expect

    def test_get_monitoring(self):
        fake_info: utils.MonitoringDict = {"notification_email": "fake_value"}
        assert (
            utils.InstanceConfig(
                service="",
                cluster="",
                instance="",
                config_dict={"monitoring": fake_info},
                branch_dict=None,
            ).get_monitoring()
            == fake_info
        )

    def test_get_cpus_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"cpus": -5},
            branch_dict=None,
        )
        assert fake_conf.get_cpus() == -5

    def test_get_cpus_in_config_float(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"cpus": 0.66},
            branch_dict=None,
        )
        assert fake_conf.get_cpus() == 0.66

    def test_get_cpus_default(self):
        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_cpus() == 1

    def test_get_mem_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"mem": -999},
            branch_dict=None,
        )
        assert fake_conf.get_mem() == -999

    def test_get_mem_default(self):
        fake_conf = utils.InstanceConfig(
            service="", instance="", cluster="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_mem() == 4096

    def test_zero_cpu_burst(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={"cpu_burst_add": 0, "cpus": 1},
            branch_dict=None,
        )
        assert fake_conf.get_cpu_quota() == 100000

    def test_nonzero_cpu_burst(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={"cpu_burst_add": 10, "cpus": 1},
            branch_dict=None,
        )
        assert fake_conf.get_cpu_quota() == 1100000

    def test_format_docker_parameters_default(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={"cpus": 1, "mem": 1024, "disk": 7000},
            branch_dict=None,
        )
        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ):
            assert fake_conf.format_docker_parameters() == [
                {"key": "memory-swap", "value": "1088m"},
                {"key": "cpu-period", "value": "100000"},
                {"key": "cpu-quota", "value": "200000"},
                {"key": "label", "value": "paasta_service=fake_name"},
                {"key": "label", "value": "paasta_instance=fake_instance"},
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
            ]

    def test_format_docker_parameters_with_disk_quota_default(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={"cpus": 1, "mem": 1024, "disk": 7000},
            branch_dict=None,
        )
        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=True,
        ):
            assert fake_conf.format_docker_parameters() == [
                {"key": "memory-swap", "value": "1088m"},
                {"key": "cpu-period", "value": "100000"},
                {"key": "cpu-quota", "value": "200000"},
                {"key": "storage-opt", "value": "size=7340032000"},
                {"key": "label", "value": "paasta_service=fake_name"},
                {"key": "label", "value": "paasta_instance=fake_instance"},
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
            ]

    def test_format_docker_parameters_non_default(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={
                "cpu_burst_add": 2,
                "cfs_period_us": 200000,
                "cpus": 1,
                "mem": 1024,
                "disk": 1234,
                "cap_add": ["IPC_LOCK", "SYS_PTRACE"],
            },
            branch_dict=None,
        )
        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ):
            assert fake_conf.format_docker_parameters() == [
                {"key": "memory-swap", "value": "1088m"},
                {"key": "cpu-period", "value": "200000"},
                {"key": "cpu-quota", "value": "600000"},
                {"key": "label", "value": "paasta_service=fake_name"},
                {"key": "label", "value": "paasta_instance=fake_instance"},
                {"key": "init", "value": "true"},
                {"key": "cap-add", "value": "IPC_LOCK"},
                {"key": "cap-add", "value": "SYS_PTRACE"},
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
            ]

    def test_format_docker_parameters_overlapping_caps(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={
                "cpu_burst_add": 2,
                "cfs_period_us": 200000,
                "cpus": 1,
                "mem": 1024,
                "disk": 1234,
                "cap_add": ["IPC_LOCK", "SYS_PTRACE", "DAC_OVERRIDE", "NET_RAW"],
            },
            branch_dict=None,
        )
        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ):
            assert fake_conf.format_docker_parameters() == [
                {"key": "memory-swap", "value": "1088m"},
                {"key": "cpu-period", "value": "200000"},
                {"key": "cpu-quota", "value": "600000"},
                {"key": "label", "value": "paasta_service=fake_name"},
                {"key": "label", "value": "paasta_instance=fake_instance"},
                {"key": "init", "value": "true"},
                {"key": "cap-add", "value": "IPC_LOCK"},
                {"key": "cap-add", "value": "SYS_PTRACE"},
                {"key": "cap-add", "value": "DAC_OVERRIDE"},
                {"key": "cap-add", "value": "NET_RAW"},
                {"key": "cap-drop", "value": "SETPCAP"},
                {"key": "cap-drop", "value": "MKNOD"},
                {"key": "cap-drop", "value": "AUDIT_WRITE"},
                {"key": "cap-drop", "value": "CHOWN"},
                {"key": "cap-drop", "value": "FOWNER"},
                {"key": "cap-drop", "value": "FSETID"},
                {"key": "cap-drop", "value": "KILL"},
                {"key": "cap-drop", "value": "SETGID"},
                {"key": "cap-drop", "value": "SETUID"},
                {"key": "cap-drop", "value": "NET_BIND_SERVICE"},
                {"key": "cap-drop", "value": "SYS_CHROOT"},
                {"key": "cap-drop", "value": "SETFCAP"},
            ]

    def test_format_docker_parameters_with_disk_quota_non_default(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={
                "cpu_burst_add": 2,
                "cfs_period_us": 200000,
                "cpus": 1,
                "mem": 1024,
                "disk": 1234,
                "cap_add": ["IPC_LOCK", "SYS_PTRACE"],
            },
            branch_dict=None,
        )
        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=True,
        ):
            assert fake_conf.format_docker_parameters() == [
                {"key": "memory-swap", "value": "1088m"},
                {"key": "cpu-period", "value": "200000"},
                {"key": "cpu-quota", "value": "600000"},
                {"key": "storage-opt", "value": "size=1293942784"},
                {"key": "label", "value": "paasta_service=fake_name"},
                {"key": "label", "value": "paasta_instance=fake_instance"},
                {"key": "init", "value": "true"},
                {"key": "cap-add", "value": "IPC_LOCK"},
                {"key": "cap-add", "value": "SYS_PTRACE"},
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
            ]

    def test_full_cpu_burst(self):
        fake_conf = utils.InstanceConfig(
            service="fake_name",
            cluster="",
            instance="fake_instance",
            config_dict={"cpu_burst_add": 2, "cpus": 1},
            branch_dict=None,
        )
        assert fake_conf.get_cpu_quota() == 300000

    def test_get_mem_swap_int(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"mem": 50},
            branch_dict=None,
        )
        assert fake_conf.get_mem_swap() == "114m"

    def test_get_mem_swap_float_rounds_up(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"mem": 50.4},
            branch_dict=None,
        )
        assert fake_conf.get_mem_swap() == "115m"

    def test_get_disk_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"disk": -999},
            branch_dict=None,
        )
        assert fake_conf.get_disk() == -999

    def test_get_disk_default(self):
        fake_conf = utils.InstanceConfig(
            service="", instance="", cluster="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_disk() == 1024

    def test_get_gpus_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"gpus": -123},
            branch_dict=None,
        )
        assert fake_conf.get_gpus() == -123

    def test_get_gpus_default(self):
        fake_conf = utils.InstanceConfig(
            service="", instance="", cluster="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_gpus() is None

    def test_get_cap_add_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"cap_add": ["IPC_LOCK", "SYS_PTRACE"]},
            branch_dict=None,
        )
        assert list(fake_conf.get_cap_add()) == [
            {"key": "cap-add", "value": "IPC_LOCK"},
            {"key": "cap-add", "value": "SYS_PTRACE"},
        ]

    def test_get_cap_add_default(self):
        fake_conf = utils.InstanceConfig(
            service="", instance="", cluster="", config_dict={}, branch_dict=None
        )
        assert list(fake_conf.get_cap_add()) == []

    def test_deploy_group_default(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="fake_instance",
            cluster="fake_cluster",
            config_dict={},
            branch_dict=None,
        )
        assert fake_conf.get_deploy_group() == "fake_cluster.fake_instance"

    def test_deploy_group_if_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="",
            config_dict={"deploy_group": "fake_deploy_group"},
            branch_dict=None,
        )
        assert fake_conf.get_deploy_group() == "fake_deploy_group"

    def test_deploy_group_string_interpolation(self):
        fake_conf = utils.InstanceConfig(
            service="",
            instance="",
            cluster="fake_cluster",
            config_dict={"deploy_group": "cluster_is_{cluster}"},
            branch_dict=None,
        )
        assert fake_conf.get_deploy_group() == "cluster_is_fake_cluster"

    def test_get_cmd_default(self):
        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_cmd() is None

    def test_get_cmd_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"cmd": "FAKECMD"},
            branch_dict=None,
        )
        assert fake_conf.get_cmd() == "FAKECMD"

    def test_get_env_default(self):
        fake_conf = utils.InstanceConfig(
            service="fake_service",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict=None,
        )
        assert fake_conf.get_env() == {
            "PAASTA_SERVICE": "fake_service",
            "PAASTA_INSTANCE": "fake_instance",
            "PAASTA_CLUSTER": "fake_cluster",
            "PAASTA_DEPLOY_GROUP": "fake_cluster.fake_instance",
            "PAASTA_DOCKER_IMAGE": "",
            "PAASTA_RESOURCE_CPUS": "1",
            "PAASTA_RESOURCE_DISK": "1024",
            "PAASTA_RESOURCE_MEM": "4096",
            "AWS_SDK_UA_APP_ID": "fake_service.fake_instance",
        }

    def test_get_env_image_version(self):
        fake_conf = utils.InstanceConfig(
            service="fake_service",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={},
            branch_dict={
                "desired_state": "start",
                "force_bounce": "12345",
                "docker_image": "something",
                "git_sha": "9",
                "image_version": "extrastuff",
            },
        )
        with mock.patch(
            "paasta_tools.utils.get_service_docker_registry",
            autospec=True,
            return_value="something",
        ):
            assert fake_conf.get_env() == {
                "PAASTA_SERVICE": "fake_service",
                "PAASTA_INSTANCE": "fake_instance",
                "PAASTA_CLUSTER": "fake_cluster",
                "PAASTA_DEPLOY_GROUP": "fake_cluster.fake_instance",
                "PAASTA_GIT_SHA": "somethin",
                "PAASTA_DOCKER_IMAGE": "something",
                "PAASTA_IMAGE_VERSION": "extrastuff",
                "PAASTA_RESOURCE_CPUS": "1",
                "PAASTA_RESOURCE_DISK": "1024",
                "PAASTA_RESOURCE_MEM": "4096",
                "AWS_SDK_UA_APP_ID": "fake_service.fake_instance",
            }

    def test_get_env_handles_non_strings_and_returns_strings(self):
        fake_conf = utils.InstanceConfig(
            service="fake_service",
            cluster="fake_cluster",
            instance="fake_instance",
            config_dict={"deploy_group": None},
            branch_dict=None,
        )
        assert fake_conf.get_env() == {
            "PAASTA_SERVICE": "fake_service",
            "PAASTA_INSTANCE": "fake_instance",
            "PAASTA_CLUSTER": "fake_cluster",
            "PAASTA_DEPLOY_GROUP": "None",
            "PAASTA_DOCKER_IMAGE": "",
            "PAASTA_RESOURCE_CPUS": "1",
            "PAASTA_RESOURCE_DISK": "1024",
            "PAASTA_RESOURCE_MEM": "4096",
            "AWS_SDK_UA_APP_ID": "fake_service.fake_instance",
        }

    def test_get_env_with_config(self):
        with mock.patch(
            "paasta_tools.utils.get_service_docker_registry",
            autospec=True,
            return_value="something",
        ):
            fake_conf = utils.InstanceConfig(
                service="",
                cluster="",
                instance="",
                config_dict={
                    "env": {"SPECIAL_ENV": "TRUE"},
                    "deploy_group": "fake_deploy_group",
                    "monitoring": {"team": "generic_team"},
                },
                branch_dict={
                    "git_sha": "c0defeed",
                    "docker_image": "something",
                    "image_version": None,
                    "desired_state": "start",
                    "force_bounce": None,
                },
            )
            assert fake_conf.get_env() == {
                "SPECIAL_ENV": "TRUE",
                "PAASTA_SERVICE": "",
                "PAASTA_INSTANCE": "",
                "PAASTA_CLUSTER": "",
                "PAASTA_DEPLOY_GROUP": "fake_deploy_group",
                "PAASTA_DOCKER_IMAGE": "something",
                "PAASTA_MONITORING_TEAM": "generic_team",
                "PAASTA_RESOURCE_CPUS": "1",
                "PAASTA_RESOURCE_DISK": "1024",
                "PAASTA_RESOURCE_MEM": "4096",
                "PAASTA_GIT_SHA": "somethin",
                "AWS_SDK_UA_APP_ID": ".",
            }

    def test_get_args_default_no_cmd(self):
        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_args() == []

    def test_get_args_default_with_cmd(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"cmd": "FAKECMD"},
            branch_dict=None,
        )
        assert fake_conf.get_args() is None

    def test_get_args_in_config(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"args": ["arg1", "arg2"]},
            branch_dict=None,
        )
        assert fake_conf.get_args() == ["arg1", "arg2"]

    def test_get_args_in_config_with_cmd(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"args": ["A"], "cmd": "C"},
            branch_dict=None,
        )
        fake_conf.get_cmd()
        with raises(utils.InvalidInstanceConfig):
            fake_conf.get_args()

    def test_get_force_bounce(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={},
            branch_dict={
                "git_sha": "abcdef0",
                "docker_image": "whale",
                "image_version": None,
                "desired_state": "start",
                "force_bounce": "blurp",
            },
        )
        assert fake_conf.get_force_bounce() == "blurp"

    def test_get_desired_state(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={},
            branch_dict={
                "git_sha": "abcdef0",
                "docker_image": "whale",
                "image_version": None,
                "desired_state": "stop",
                "force_bounce": None,
            },
        )
        assert fake_conf.get_desired_state() == "stop"

    def test_deploy_blacklist_default(self):
        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_deploy_blacklist() == []

    def test_deploy_blacklist_reads_blacklist(self):
        fake_deploy_blacklist = [("region", "fake_region")]
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"deploy_blacklist": fake_deploy_blacklist},
            branch_dict=None,
        )
        assert fake_conf.get_deploy_blacklist() == fake_deploy_blacklist

    def test_extra_volumes_default(self):
        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_extra_volumes() == []

    def test_extra_volumes_normal(self):
        fake_extra_volumes: List[utils.DockerVolume] = [
            {"containerPath": "/etc/a", "hostPath": "/var/data/a", "mode": "RO"}
        ]
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"extra_volumes": fake_extra_volumes},
            branch_dict=None,
        )
        assert fake_conf.get_extra_volumes() == fake_extra_volumes

    def test_get_pool(self):
        pool = "poolname"
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"pool": pool},
            branch_dict=None,
        )
        assert fake_conf.get_pool() == pool

    def test_get_pool_default(self):
        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )
        assert fake_conf.get_pool() == "default"

    def test_get_volumes_dedupes_correctly_when_mode_differs_last_wins(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "extra_volumes": [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RW"},
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                ],
                "uses_bulkdata": False,
            },
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = []
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"}
        ]

    def test_get_volumes_dedupes_respects_hostpath(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "extra_volumes": [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                    {"containerPath": "/a", "hostPath": "/other_a", "mode": "RO"},
                ],
                "uses_bulkdata": False,
            },
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"}
        ]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/a", "hostPath": "/other_a", "mode": "RO"},
        ]

    def test_get_volumes_handles_dupes_everywhere(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "extra_volumes": [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                    {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
                    {"containerPath": "/c", "hostPath": "/c", "mode": "RO"},
                ],
                "uses_bulkdata": False,
            },
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
            {"containerPath": "/d", "hostPath": "/d", "mode": "RO"},
        ]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
            {"containerPath": "/c", "hostPath": "/c", "mode": "RO"},
            {"containerPath": "/d", "hostPath": "/d", "mode": "RO"},
        ]

    def test_get_volumes_prefers_extra_volumes_over_system(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "extra_volumes": [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RW"}
                ],
                "uses_bulkdata": False,
            },
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"}
        ]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RW"}
        ]

    def test_get_volumes_handles_dupes_with_trailing_slashes(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "extra_volumes": [
                    {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
                    {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
                ],
                "uses_bulkdata": False,
            },
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/b/", "hostPath": "/b/", "mode": "RO"},
        ]
        # note: prefers extra_volumes over system_volumes
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a", "hostPath": "/a", "mode": "RO"},
            {"containerPath": "/b", "hostPath": "/b", "mode": "RO"},
        ]

    def test_get_volumes_preserves_trailing_slash(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "extra_volumes": [
                    {"containerPath": "/a/", "hostPath": "/a/", "mode": "RW"}
                ],
                "uses_bulkdata": False,
            },
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = [
            {"containerPath": "/b/", "hostPath": "/b/", "mode": "RW"}
        ]
        assert fake_conf.get_volumes(system_volumes) == [
            {"containerPath": "/a/", "hostPath": "/a/", "mode": "RW"},
            {"containerPath": "/b/", "hostPath": "/b/", "mode": "RW"},
        ]

    def test_get_volumes_with_bulkdata(self):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"uses_bulkdata": True},
            branch_dict=None,
        )
        system_volumes: List[utils.DockerVolume] = []
        assert fake_conf.get_volumes(system_volumes) == [
            {
                "hostPath": "/nail/bulkdata",
                "containerPath": "/nail/bulkdata",
                "mode": "RO",
            },
        ]

    def test_get_docker_url_no_error(self):
        fake_registry = "im.a-real.vm"
        fake_image = "and-i-can-run:1.0"

        fake_conf = utils.InstanceConfig(
            service="", cluster="", instance="", config_dict={}, branch_dict=None
        )

        with mock.patch(
            "paasta_tools.utils.InstanceConfig.get_docker_registry",
            autospec=True,
            return_value=fake_registry,
        ), mock.patch(
            "paasta_tools.utils.InstanceConfig.get_docker_image",
            autospec=True,
            return_value=fake_image,
        ):
            expected_url = f"{fake_registry}/{fake_image}"
            assert fake_conf.get_docker_url() == expected_url

    @pytest.mark.parametrize(
        ("dependencies_reference", "dependencies", "expected"),
        [
            (None, None, None),
            ("aaa", None, None),
            ("aaa", {}, None),
            ("aaa", {"aaa": [{"foo": "bar"}]}, {"foo": "bar"}),
            ("aaa", {"bbb": [{"foo": "bar"}]}, None),
        ],
    )
    def test_get_dependencies(self, dependencies_reference, dependencies, expected):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "dependencies_reference": dependencies_reference,
                "dependencies": dependencies,
            },
            branch_dict=None,
        )
        fake_conf.get_dependencies() == expected

    @pytest.mark.parametrize(
        ("security", "expected"),
        [
            ({}, None),
            (None, None),
            ({"outbound_firewall": "monitor"}, "monitor"),
            ({"outbound_firewall": "foo"}, "foo"),
        ],
    )
    def test_get_outbound_firewall(self, security, expected):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"security": security},
            branch_dict=None,
        )
        fake_conf.get_outbound_firewall() == expected

    @pytest.mark.parametrize(
        ("security", "expected"),
        [
            ({}, (True, "")),
            ({"outbound_firewall": "monitor"}, (True, "")),
            ({"outbound_firewall": "block"}, (True, "")),
            (
                {"outbound_firewall": "foo"},
                (False, 'Unrecognized outbound_firewall value "foo"'),
            ),
            (
                {"outbound_firewall": "monitor", "foo": 1},
                (False, 'Unrecognized items in security dict of service config: "foo"'),
            ),
        ],
    )
    def test_check_security(self, security, expected):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={"security": security},
            branch_dict=None,
        )
        assert fake_conf.check_security() == expected

    @pytest.mark.parametrize(
        ("dependencies_reference", "dependencies", "expected"),
        [
            (None, None, (True, "")),
            ("aaa", {"aaa": []}, (True, "")),
            (
                "aaa",
                None,
                (
                    False,
                    'dependencies_reference "aaa" declared but no dependencies found',
                ),
            ),
            (
                "aaa",
                {"bbb": []},
                (
                    False,
                    'dependencies_reference "aaa" not found in dependencies dictionary',
                ),
            ),
        ],
    )
    def test_check_dependencies_reference(
        self, dependencies_reference, dependencies, expected
    ):
        fake_conf = utils.InstanceConfig(
            service="",
            cluster="",
            instance="",
            config_dict={
                "dependencies_reference": dependencies_reference,
                "dependencies": dependencies,
            },
            branch_dict=None,
        )
        assert fake_conf.check_dependencies_reference() == expected


def test_is_under_replicated_ok():
    num_available = 1
    expected_count = 1
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_zero():
    num_available = 1
    expected_count = 0
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_critical():
    num_available = 0
    expected_count = 1
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (True, float(0))


def test_deploy_blacklist_to_constraints():
    fake_deploy_blacklist = [("region", "useast1-prod"), ("habitat", "fake_habitat")]
    expected_constraints = [
        ["region", "UNLIKE", "useast1-prod"],
        ["habitat", "UNLIKE", "fake_habitat"],
    ]
    actual = utils.deploy_blacklist_to_constraints(fake_deploy_blacklist)
    assert actual == expected_constraints


def test_validate_service_instance_valid_kubernetes():
    mock_kubernetes_instances = [("service1", "main"), ("service1", "main2")]
    my_service = "service1"
    my_instance = "main"
    fake_cluster = "fake_cluster"
    fake_soa_dir = "fake_soa_dir"
    with mock.patch(
        "paasta_tools.utils.get_service_instance_list",
        autospec=True,
        side_effect=[mock_kubernetes_instances],
    ):
        assert (
            utils.validate_service_instance(
                my_service, my_instance, fake_cluster, fake_soa_dir
            )
            == "paasta_native"  # the first entry in utils.INSTANCE_TYPES
        )


def test_validate_service_instance_invalid():
    mock_kubernetes_instances = [("service1", "main1"), ("service1", "main2")]
    mock_paasta_native_instances = [("service1", "main2"), ("service1", "main3")]
    mock_adhoc_instances = [("service1", "interactive")]
    mock_k8s_instances = [("service1", "k8s")]
    mock_eks_instances = [("service1", "eks")]
    mock_tron_instances = [("service1", "job.action")]
    mock_flink_instances = [("service1", "flink")]
    mock_flinkeks_instances = [("service1", "flinkeks")]
    mock_cassandracluster_instances = [("service1", "cassandracluster")]
    mock_kafkacluster_instances = [("service1", "kafkacluster")]
    mock_nrtsearch_instances = [("service1", "nrtsearch")]
    mock_nrtsearcheks_instances = [("service1", "nrtsearcheks")]
    mock_monkrelaycluster_instances = [("service1", "monkrelays")]
    my_service = "service1"
    my_instance = "main"
    fake_cluster = "fake_cluster"
    fake_soa_dir = "fake_soa_dir"
    with mock.patch(
        "paasta_tools.utils.get_service_instance_list",
        autospec=True,
        side_effect=[
            mock_kubernetes_instances,
            mock_paasta_native_instances,
            mock_adhoc_instances,
            mock_k8s_instances,
            mock_eks_instances,
            mock_tron_instances,
            mock_flink_instances,
            mock_flinkeks_instances,
            mock_cassandracluster_instances,
            mock_kafkacluster_instances,
            mock_nrtsearch_instances,
            mock_nrtsearcheks_instances,
            mock_monkrelaycluster_instances,
        ],
    ):
        with raises(
            utils.NoConfigurationForServiceError,
            match="Did you mean one of: main3, main2, main1?",
        ):
            utils.validate_service_instance(
                service=my_service,
                instance=my_instance,
                cluster=fake_cluster,
                soa_dir=fake_soa_dir,
            )


def test_terminal_len():
    assert len("some text") == utils.terminal_len(utils.PaastaColors.red("some text"))


def test_format_table():
    actual = utils.format_table(
        [["looooong", "y", "z"], ["a", "looooong", "c"], ["j", "k", "looooong"]]
    )
    expected = [
        "looooong  y         z",
        "a         looooong  c",
        "j         k         looooong",
    ]
    assert actual == expected
    assert ["a     b     c"] == utils.format_table([["a", "b", "c"]], min_spacing=5)


def test_format_table_with_interjected_lines():
    actual = utils.format_table(
        [
            ["looooong", "y", "z"],
            "interjection",
            ["a", "looooong", "c"],
            "unicode interjection",
            ["j", "k", "looooong"],
        ]
    )
    expected = [
        "looooong  y         z",
        "interjection",
        "a         looooong  c",
        "unicode interjection",
        "j         k         looooong",
    ]
    assert actual == expected


def test_format_table_all_strings():
    actual = utils.format_table(["foo", "bar", "baz"])
    expected = ["foo", "bar", "baz"]
    assert actual == expected


def test_parse_timestamp():
    actual = utils.parse_timestamp("19700101T000000")
    expected = datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0)
    assert actual == expected


def test_null_log_writer():
    """Basic smoke test for NullLogWriter"""
    lw = utils.NullLogWriter(driver="null")
    lw.log("fake_service", "fake_line", "build", "BOGUS_LEVEL")
    lw.log_audit("fake_user", "fake_hostname", "fake_action", service="fake_service")


class TestFileLogWriter:
    def test_smoke(self):
        """Smoke test for FileLogWriter"""
        fw = utils.FileLogWriter("/dev/null")
        fw.log("fake_service", "fake_line", "build", "BOGUS_LEVEL")
        fw.log_audit(
            "fake_user", "fake_hostname", "fake_action", service="fake_service"
        )

    def test_format_path(self):
        """Test the path formatting for FileLogWriter"""
        fw = utils.FileLogWriter(
            "/logs/{service}/{component}/{level}/{cluster}/{instance}"
        )
        expected = "/logs/a/b/c/d/e"
        assert expected == fw.format_path("a", "b", "c", "d", "e")

    def test_maybe_flock(self):
        """Make sure we flock and unflock when flock=True"""
        with mock.patch("paasta_tools.utils.fcntl", autospec=True) as mock_fcntl:
            fw = utils.FileLogWriter("/dev/null", flock=True)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                mock_fcntl.flock.assert_called_once_with(
                    mock_file.fileno(), mock_fcntl.LOCK_EX
                )
                mock_fcntl.flock.reset_mock()

            mock_fcntl.flock.assert_called_once_with(
                mock_file.fileno(), mock_fcntl.LOCK_UN
            )

    def test_maybe_flock_flock_false(self):
        """Make sure we don't flock/unflock when flock=False"""
        with mock.patch("paasta_tools.utils.fcntl", autospec=True) as mock_fcntl:
            fw = utils.FileLogWriter("/dev/null", flock=False)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                assert mock_fcntl.flock.call_count == 0

            assert mock_fcntl.flock.call_count == 0

    def test_log_makes_exactly_one_write_call(self):
        """We want to make sure that log() makes exactly one call to write, since that's how we ensure atomicity."""
        fake_file = mock.Mock()
        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file, __exit__=lambda _self, t, v, tb: None
        )

        fake_line = "text" * 1000000

        with mock.patch(
            "paasta_tools.utils.io.FileIO", return_value=fake_contextmgr, autospec=True
        ) as mock_FileIO:
            fw = utils.FileLogWriter("/dev/null", flock=False)

            with mock.patch(
                "paasta_tools.utils.format_log_line",
                return_value=fake_line,
                autospec=True,
            ) as fake_fll:
                fw.log(
                    "service",
                    "line",
                    "component",
                    level="level",
                    cluster="cluster",
                    instance="instance",
                )

            fake_fll.assert_called_once_with(
                "level", "cluster", "service", "instance", "component", "line"
            )

            mock_FileIO.assert_called_once_with("/dev/null", mode=fw.mode, closefd=True)
            fake_file.write.assert_called_once_with(f"{fake_line}\n".encode("UTF-8"))

    def test_write_raises_IOError(self):
        fake_file = mock.Mock()
        fake_file.write.side_effect = IOError("hurp durp")

        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file, __exit__=lambda _self, t, v, tb: None
        )

        fake_line = "line"

        with mock.patch(
            "paasta_tools.utils.io.FileIO", return_value=fake_contextmgr, autospec=True
        ), mock.patch("builtins.print", autospec=True) as mock_print, mock.patch(
            "paasta_tools.utils.format_log_line", return_value=fake_line, autospec=True
        ):
            fw = utils.FileLogWriter("/dev/null", flock=False)
            fw.log(
                service="service",
                line="line",
                component="build",
                level="level",
                cluster="cluster",
                instance="instance",
            )

        mock_print.assert_called_once_with(mock.ANY, file=sys.stderr)

        # On python3, they merged IOError and OSError. Once paasta is fully py3, replace mock.ANY above with the OSError
        # message below.
        assert mock_print.call_args[0][0] in {
            "Could not log to /dev/null: IOError: hurp durp -- would have logged: line\n",
            "Could not log to /dev/null: OSError: hurp durp -- would have logged: line\n",
        }


def test_deep_merge_dictionaries():
    overrides = {
        "common_key": "value",
        "common_dict": {"subkey1": 1, "subkey2": 2, "subkey3": 3},
        "just_in_overrides": "value",
        "just_in_overrides_dict": {"key": "value"},
        "overwriting_key": "value",
        "overwriting_dict": {"test": "value"},
    }
    defaults = {
        "common_key": "overwritten_value",
        "common_dict": {"subkey1": "overwritten_value", "subkey4": 4, "subkey5": 5},
        "just_in_defaults": "value",
        "just_in_defaults_dict": {"key": "value"},
        "overwriting_key": {"overwritten-key", "overwritten-value"},
        "overwriting_dict": "overwritten-value",
    }
    expected = {
        "common_key": "value",
        "common_dict": {
            "subkey1": 1,
            "subkey2": 2,
            "subkey3": 3,
            "subkey4": 4,
            "subkey5": 5,
        },
        "just_in_overrides": "value",
        "just_in_overrides_dict": {"key": "value"},
        "just_in_defaults": "value",
        "just_in_defaults_dict": {"key": "value"},
        "overwriting_key": "value",
        "overwriting_dict": {"test": "value"},
    }
    assert (
        utils.deep_merge_dictionaries(overrides, defaults, allow_duplicate_keys=True)
        == expected
    )


def test_deep_merge_dictionaries_no_duplicate_keys_allowed():
    # Nested dicts should be allowed
    overrides = {"nested": {"a": "override"}}
    defaults = {"nested": {"b": "default"}}
    expected = {"nested": {"a": "override", "b": "default"}}
    assert (
        utils.deep_merge_dictionaries(overrides, defaults, allow_duplicate_keys=True)
        == expected
    )
    del expected

    overrides2 = {"a": "override"}
    defaults2 = {"a": "default"}

    with raises(utils.DuplicateKeyError):
        utils.deep_merge_dictionaries(overrides2, defaults2, allow_duplicate_keys=False)

    overrides = {"nested": {"a": "override"}}
    defaults = {"nested": {"a": "default"}}

    with raises(utils.DuplicateKeyError):
        utils.deep_merge_dictionaries(overrides, defaults, allow_duplicate_keys=False)


def test_function_composition():
    def func_one(count):
        return count + 1

    def func_two(count):
        return count + 1

    composed_func = utils.compose(func_one, func_two)
    assert composed_func(0) == 2


def test_is_deploy_step():
    assert utils.is_deploy_step("prod.main")
    assert utils.is_deploy_step("thingy")

    assert not utils.is_deploy_step("itest")
    assert not utils.is_deploy_step("command-thingy")


def test_long_job_id_to_short_job_id():
    assert (
        utils.long_job_id_to_short_job_id("service.instance.git.config")
        == "service.instance"
    )


def test_mean():
    iterable = [1.0, 2.0, 3.0]
    assert utils.mean(iterable) == 2.0


def test_prompt_pick_one_happy():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(return_value="choiceA"))
        assert utils.prompt_pick_one(["choiceA"], "test") == "choiceA"


def test_prompt_pick_one_quit():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(return_value=(None, "quit")))
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_keyboard_interrupt():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(side_effect=KeyboardInterrupt))
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_eoferror():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(side_effect=EOFError))
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_exits_no_tty():
    with mock.patch("paasta_tools.utils.sys.stdin", autospec=True) as mock_stdin:
        mock_stdin.isatty.return_value = False
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_exits_no_choices():
    with mock.patch("paasta_tools.utils.sys.stdin", autospec=True) as mock_stdin:
        mock_stdin.isatty.return_value = True
        with raises(SystemExit):
            utils.prompt_pick_one([], "test")


def test_get_code_sha_from_dockerurl():
    fake_docker_url = (
        "docker-paasta.yelpcorp.com:443/services-cieye:paasta-93340779404579"
    )
    actual = utils.get_code_sha_from_dockerurl(fake_docker_url)
    assert actual == "git93340779"

    # Useful mostly for integration tests, where we run busybox a lot.
    assert utils.get_code_sha_from_dockerurl("docker.io/busybox") == "gitbusybox"


@mock.patch("paasta_tools.utils.fcntl.flock", autospec=True, wraps=utils.fcntl.flock)
def test_flock(mock_flock, tmpdir):
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with utils.flock(f):
            mock_flock.assert_called_once_with(f.fileno(), utils.fcntl.LOCK_EX)
            mock_flock.reset_mock()

        mock_flock.assert_called_once_with(f.fileno(), utils.fcntl.LOCK_UN)


@mock.patch("paasta_tools.utils.Timeout", autospec=True)
@mock.patch("paasta_tools.utils.fcntl.flock", autospec=True, wraps=utils.fcntl.flock)
def test_timed_flock_ok(mock_flock, mock_timeout, tmpdir):
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with utils.timed_flock(f, seconds=mock.sentinel.seconds):
            mock_timeout.assert_called_once_with(seconds=mock.sentinel.seconds)
            mock_flock.assert_called_once_with(f.fileno(), utils.fcntl.LOCK_EX)
            mock_flock.reset_mock()

        mock_flock.assert_called_once_with(f.fileno(), utils.fcntl.LOCK_UN)


@mock.patch(
    "paasta_tools.utils.Timeout",
    autospec=True,
    side_effect=utils.TimeoutError("Oh noes"),
)
@mock.patch("paasta_tools.utils.fcntl.flock", autospec=True, wraps=utils.fcntl.flock)
def test_timed_flock_timeout(mock_flock, mock_timeout, tmpdir):
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with pytest.raises(utils.TimeoutError):
            with utils.timed_flock(f):
                assert False  # pragma: no cover
        assert mock_flock.mock_calls == []


@mock.patch("paasta_tools.utils.fcntl.flock", autospec=True, wraps=utils.fcntl.flock)
def test_timed_flock_inner_timeout_ok(mock_flock, tmpdir):
    # Doing something slow inside the 'with' context of timed_flock doesn't cause a timeout
    # (the timeout should only apply to the flock operation itself)
    my_file = tmpdir.join("my-file")
    with open(str(my_file), "w") as f:
        with utils.timed_flock(f, seconds=1):
            time.true_slow_sleep(0.1)  # type: ignore
        assert mock_flock.mock_calls == [
            mock.call(f.fileno(), utils.fcntl.LOCK_EX),
            mock.call(f.fileno(), utils.fcntl.LOCK_UN),
        ]


def test_suggest_possibilities_none():
    expected = ""
    actual = utils.suggest_possibilities(word="FOO", possibilities=[])
    assert actual == expected


def test_suggest_possibilities_many():
    expected = "FOOO, FOOBAR"
    actual = utils.suggest_possibilities(word="FOO", possibilities=["FOOO", "FOOBAR"])
    assert expected in actual


def test_suggest_possibilities_one():
    expected = "FOOBAR?"
    actual = utils.suggest_possibilities(word="FOO", possibilities=["FOOBAR", "BAZ"])
    assert expected in actual


def test_filter_templates_from_config_with_empty_dict():
    assert utils.filter_templates_from_config({}) == {}


def test_filter_templates_from_config():
    config = {"_template": "foo", "instance0": "bar", "instance1": "baz"}
    assert utils.filter_templates_from_config(config) == {
        "instance0": "bar",
        "instance1": "baz",
    }


def test_is_secrets_for_teams_enabled():
    with mock.patch(
        "paasta_tools.utils.read_extra_service_information", autospec=True
    ) as mock_read_extra_service_information:
        service = "example_secrets_for_teams"

        # if enabled
        mock_read_extra_service_information.return_value = {
            "description": "something",
            "secrets_for_owner_team": True,
        }
        assert utils.is_secrets_for_teams_enabled(service)

        # if specifically not enabled
        mock_read_extra_service_information.return_value = {
            "description": "something",
            "secrets_for_owner_team": False,
        }
        assert not utils.is_secrets_for_teams_enabled(service)

        # if not present
        mock_read_extra_service_information.return_value = {"description": "something"}
        assert not utils.is_secrets_for_teams_enabled(service)


@pytest.mark.parametrize(
    "cluster,pool,system_paasta_config,expected",
    [
        (
            # allowed_pools key has test-cluster and test-pool
            "test-cluster",
            "test-pool",
            SystemPaastaConfig(
                SystemPaastaConfigDict(
                    {"allowed_pools": {"test-cluster": ["test-pool", "fake-pool"]}}
                ),
                "fake_dir",
            ),
            True,
        ),
        (
            # allowed_pools key has test-cluster but doesn't have test-pool
            "test-cluster",
            "test-pool",
            SystemPaastaConfig(
                SystemPaastaConfigDict(
                    {"allowed_pools": {"test-cluster": ["fail-test-pool", "fake-pool"]}}
                ),
                "fake_dir",
            ),
            False,
        ),
    ],
)
def test_validate_pool(cluster, pool, system_paasta_config, expected):
    assert utils.validate_pool(cluster, pool, system_paasta_config) == expected


@pytest.mark.parametrize(
    "cluster,pool,system_paasta_config",
    [
        (
            # allowed_pools key doesn't have test-cluster
            "test-cluster",
            "test-pool",
            SystemPaastaConfig(
                SystemPaastaConfigDict(
                    {"allowed_pools": {"fail-test-cluster": ["test-pool", "fake-pool"]}}
                ),
                "fake_dir",
            ),
        ),
        (
            # allowed_pools key is not present
            "test-cluster",
            "test-pool",
            SystemPaastaConfig(
                SystemPaastaConfigDict(
                    {"fail_allowed_pools": {"test-cluster": ["test-pool", "fake-pool"]}}  # type: ignore
                ),
                "fake_dir",
            ),
        ),
    ],
)
def test_validate_pool_error(cluster, pool, system_paasta_config):
    assert pytest.raises(
        PoolsNotConfiguredError,
        utils.validate_pool,
        cluster,
        pool,
        system_paasta_config,
    )


@pytest.mark.parametrize(
    "docker_url,long,expected",
    (
        (
            "registry.localhost:443/services-whatever:paasta-aaaabbbbccccdddd",
            False,
            "aaaabbbb",
        ),
        (
            "registry.localhost:443/services-whatever:paasta-aaaabbbbccccdddd",
            True,
            "aaaabbbbccccdddd",
        ),
        (
            "registry.localhost:443/services-whatever:foobar-aaaabbbbccccdddd",
            False,
            "aaaabbbb",
        ),
        (
            "registry.localhost:443/services-whatever:foobar-aaaabbbbccccdddd",
            True,
            "aaaabbbbccccdddd",
        ),
        ("registry.localhost:443/toolbox-something-something:1.2.3", False, "1.2.3"),
        ("registry.localhost:443/toolbox-something-something:1.2.3", True, "1.2.3"),
    ),
)
def test_get_git_sha_from_dockerurl(docker_url, long, expected):
    assert utils.get_git_sha_from_dockerurl(docker_url, long) == expected
