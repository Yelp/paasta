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
import os

import mock
import pytest
from mock import patch

from paasta_tools.cli.cmds.validate import check_secrets_for_instance
from paasta_tools.cli.cmds.validate import check_service_path
from paasta_tools.cli.cmds.validate import get_config_file_dict
from paasta_tools.cli.cmds.validate import get_schema
from paasta_tools.cli.cmds.validate import get_service_path
from paasta_tools.cli.cmds.validate import list_upcoming_runs
from paasta_tools.cli.cmds.validate import paasta_validate
from paasta_tools.cli.cmds.validate import paasta_validate_soa_configs
from paasta_tools.cli.cmds.validate import SCHEMA_INVALID
from paasta_tools.cli.cmds.validate import SCHEMA_VALID
from paasta_tools.cli.cmds.validate import UNKNOWN_SERVICE
from paasta_tools.cli.cmds.validate import validate_autoscaling_configs
from paasta_tools.cli.cmds.validate import validate_cpu_burst
from paasta_tools.cli.cmds.validate import validate_instance_names
from paasta_tools.cli.cmds.validate import validate_min_max_instances
from paasta_tools.cli.cmds.validate import validate_paasta_objects
from paasta_tools.cli.cmds.validate import validate_rollback_bounds
from paasta_tools.cli.cmds.validate import validate_schema
from paasta_tools.cli.cmds.validate import validate_secrets
from paasta_tools.cli.cmds.validate import validate_tron
from paasta_tools.cli.cmds.validate import validate_unique_instance_names
from paasta_tools.utils import SystemPaastaConfig


@pytest.fixture(autouse=True)
def clear_get_config_file_dict_cache():
    # this will clear the cache in-between tests, but you'll need to clear in tests
    # if you're calling validate_* functions multiple times and changing the underlying
    # files
    get_config_file_dict.cache_clear()


@patch("paasta_tools.cli.cmds.validate.validate_cpu_burst", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_autoscaling_configs", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_unique_instance_names", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_min_max_instances", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_paasta_objects", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_all_schemas", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_tron", autospec=True)
@patch("paasta_tools.cli.cmds.validate.get_service_path", autospec=True)
@patch("paasta_tools.cli.cmds.validate.check_service_path", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_secrets", autospec=True)
def test_paasta_validate_calls_everything(
    mock_validate_cpu_burst,
    mock_validate_autoscaling_configs,
    mock_validate_secrets,
    mock_check_service_path,
    mock_get_service_path,
    mock_validate_tron,
    mock_validate_all_schemas,
    mock_validate_paasta_objects,
    mock_validate_unique_instance_names,
    mock_validate_min_max_instances,
):
    # Ensure each check in 'paasta_validate' is called
    mock_validate_cpu_burst.return_value = True
    mock_validate_autoscaling_configs.return_value = True
    mock_validate_secrets.return_value = True
    mock_check_service_path.return_value = True
    mock_get_service_path.return_value = "unused_path"
    mock_validate_all_schemas.return_value = True
    mock_validate_tron.return_value = True
    mock_validate_paasta_objects.return_value = True
    mock_validate_unique_instance_names.return_value = True
    mock_validate_min_max_instances.return_value = True

    args = mock.MagicMock()
    args.service = "test"
    args.soa_dir = None

    paasta_validate(args)

    assert mock_validate_all_schemas.called
    assert mock_validate_tron.called
    assert mock_validate_unique_instance_names.called
    assert mock_validate_paasta_objects.called
    assert mock_validate_secrets.called
    assert mock_validate_autoscaling_configs.called
    assert mock_validate_cpu_burst.called


@patch("paasta_tools.cli.cmds.validate.get_instance_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.path_to_soa_dir_service", autospec=True)
def test_validate_paasta_objects(
    mock_path_to_soa_dir_service,
    mock_list_all_instances_for_service,
    mock_list_clusters,
    mock_get_instance_config,
    capsys,
):

    fake_service = "fake-service"
    fake_instance = "fake-instance"
    fake_cluster = "penguin"

    mock_paasta_instance = mock.Mock(autospec=True)
    mock_paasta_instance.validate.return_value = ["Something is wrong!"]

    mock_path_to_soa_dir_service.return_value = ("fake_soa_dir", fake_service)
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_get_instance_config.return_value = mock_paasta_instance

    assert validate_paasta_objects("fake-service-path") is False, capsys
    captured = capsys.readouterr()
    assert "Something is wrong!" in captured.out


def test_get_service_path_unknown(capsys):
    service = None
    soa_dir = "unused"

    assert get_service_path(service, soa_dir) is None

    output, _ = capsys.readouterr()
    assert UNKNOWN_SERVICE in output


def test_validate_unknown_service():
    args = mock.MagicMock()
    args.service = None
    args.yelpsoa_config_root = "unused"
    paasta_validate(args) == 1


def test_validate_service_name():
    args = mock.MagicMock()
    args.service = "aa________________________________a"
    args.yelpsoa_config_root = "unused"
    paasta_validate(args) == 1


def test_validate_unknown_service_service_path():
    service_path = "unused/path"
    service = "unused"

    assert not paasta_validate_soa_configs(service, service_path)


@patch("paasta_tools.cli.cmds.validate.get_instance_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.path_to_soa_dir_service", autospec=True)
def test_validate_min_max_instances_success(
    mock_path_to_soa_dir_service,
    mock_list_clusters,
    mock_list_all_instances_for_service,
    mock_get_instance_config,
    capsys,
):
    mock_path_to_soa_dir_service.return_value = ("fake_soa_dir", "fake_service")
    mock_list_clusters.return_value = ["fake_cluster"]
    mock_list_all_instances_for_service.return_value = {"fake_instance1"}
    mock_get_instance_config.return_value = mock.Mock(
        get_instance=mock.Mock(return_value="fake_instance1"),
        get_instance_type=mock.Mock(return_value="fake_type"),
        get_min_instances=mock.Mock(return_value=3),
        get_max_instances=mock.Mock(return_value=1),
    )

    assert validate_min_max_instances("fake-service-path") is False
    output, _ = capsys.readouterr()
    assert (
        "Instance fake_instance1 on cluster fake_cluster has a greater number of min_instances than max_instances."
        in output
    )
    assert (
        "The number of min_instances (3) cannot be greater than the max_instances (1)."
        in output
    )


@patch("paasta_tools.cli.cmds.validate.os.path.isdir", autospec=True)
@patch("paasta_tools.cli.cmds.validate.glob", autospec=True)
def test_get_service_path_cwd(mock_glob, mock_isdir):
    mock_isdir.return_value = True
    mock_glob.return_value = ["something.yaml"]

    service = None
    soa_dir = os.getcwd()

    service_path = get_service_path(service, soa_dir)

    assert service_path == os.getcwd()


@patch("paasta_tools.cli.cmds.validate.os.path.isdir", autospec=True)
@patch("paasta_tools.cli.cmds.validate.glob", autospec=True)
def test_get_service_path_soa_dir(mock_glob, mock_isdir):
    mock_isdir.return_value = True
    mock_glob.return_value = ["something.yaml"]

    service = "some_service"
    soa_dir = "some/path"

    service_path = get_service_path(service, soa_dir)

    assert service_path == f"{soa_dir}/{service}"


def is_schema(schema):
    assert schema is not None
    assert isinstance(schema, dict)
    assert "$schema" in schema


def test_get_schema_marathon_found():
    schema = get_schema("marathon")
    is_schema(schema)


def test_get_schema_tron_found():
    schema = get_schema("tron")
    is_schema(schema)


def test_get_schema_missing():
    assert get_schema("fake_schema") is None


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_schema_list_hashes_good(mock_get_file_contents, capsys):
    marathon_content = """
---
main_worker:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
  healthcheck_mode: cmd
  healthcheck_cmd: '/bin/true'
_main_http:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  registrations: ['foo.bar', 'bar.baz']
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_validate_instance_names(mock_get_file_contents, capsys):
    fake_instances = {
        "a_________________________________________a": {},
        "b_________________________________________b": {},
    }
    assert not validate_instance_names(fake_instances, "fake_path")
    output, _ = capsys.readouterr()
    assert "Length of instance name" in output


@pytest.mark.parametrize(
    "mock_config, expected",
    (
        ({}, False),
        ({"upper_bound": None}, False),
        ({"lower_bound": None}, False),
        ({"lower_bound": None, "upper_bound": None}, False),
        ({"lower_bound": 1, "upper_bound": None}, True),
        ({"lower_bound": None, "upper_bound": 1}, True),
        ({"lower_bound": 1}, True),
        ({"upper_bound": 1}, True),
    ),
)
def test_validate_rollback_bounds(mock_config, expected):
    assert (
        validate_rollback_bounds(
            {
                "prometheus": [
                    {
                        "query": "test",
                        **mock_config,
                    }
                ]
            },
            "fake_path",
        )
        is expected
    )


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_understands_underscores(mock_get_file_contents, capsys):
    marathon_content = """
---
_template: &template
  foo: bar

main:
  cpus: 0.1
  instances: 2
  env:
    <<: *template
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_schema_healthcheck_non_cmd(mock_get_file_contents, capsys):
    marathon_content = """
---
main_worker:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
  healthcheck_mode: tcp
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output
        marathon_content = """
---
main_worker:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_id(mock_get_file_contents, capsys):
    marathon_content = """
---
valid:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output

    marathon_content = """
---
this_is_okay_too_1:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output

    marathon_content = """
---
dashes-are-okay-too:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        get_config_file_dict.cache_clear()  # HACK: ensure cache is cleared for future calls
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output

    marathon_content = """
---
main_worker_CAPITALS_INVALID:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert not validate_schema("unused_service_path.yaml", schema_type)
        get_config_file_dict.cache_clear()  # HACK: ensure cache is cleared for future calls
        output, _ = capsys.readouterr()
        assert SCHEMA_INVALID in output

    marathon_content = """
---
$^&*()(&*^%&definitely_not_okay:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert not validate_schema("unused_service_path.yaml", schema_type)
        output, _ = capsys.readouterr()
        assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_schema_healthcheck_cmd_has_cmd(
    mock_get_file_contents, capsys
):
    marathon_content = """
---
main_worker:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
  healthcheck_mode: cmd
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert not validate_schema("unused_service_path.yaml", schema_type)
        get_config_file_dict.cache_clear()  # HACK: ensure cache is cleared for future calls
        output, _ = capsys.readouterr()
        assert SCHEMA_INVALID in output
        marathon_content = """
---
main_worker:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
  healthcheck_mode: cmd
  healthcheck_cmd: '/bin/true'
"""
    mock_get_file_contents.return_value = marathon_content
    for schema_type in ["marathon", "kubernetes"]:
        assert validate_schema("unused_service_path.yaml", schema_type)
        get_config_file_dict.cache_clear()  # HACK: ensure cache is cleared for future calls
        output, _ = capsys.readouterr()
        assert SCHEMA_VALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_schema_keys_outside_instance_blocks_bad(
    mock_get_file_contents, capsys
):
    mock_get_file_contents.return_value = """
{
    "main": {
        "instances": 5
    },
    "page": false
}
"""
    for schema_type in ["marathon", "kubernetes"]:
        assert not validate_schema("unused_service_path.json", schema_type)
        get_config_file_dict.cache_clear()  # HACK: ensure cache is cleared for future calls

        output, _ = capsys.readouterr()
        assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_schema_security_good(mock_get_file_contents, capsys):
    mock_get_file_contents.return_value = """
main:
    dependencies_reference: main
    security:
        outbound_firewall: block
"""
    assert validate_schema("unused_service_path.yaml", "marathon")

    output, _ = capsys.readouterr()
    assert SCHEMA_VALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_schema_security_bad(mock_get_file_contents, capsys):
    mock_get_file_contents.return_value = """
main:
    dependencies_reference: main
    security:
        outbound_firewall: bblock
"""
    for schema_type in ["marathon", "kubernetes"]:
        assert not validate_schema("unused_service_path.yaml", schema_type)

        output, _ = capsys.readouterr()
        assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_marathon_validate_invalid_key_bad(mock_get_file_contents, capsys):
    mock_get_file_contents.return_value = """
{
    "main": {
        "fake_key": 5
    }
}
"""
    for schema_type in ["marathon", "kubernetes"]:
        assert not validate_schema("unused_service_path.json", schema_type)

        output, _ = capsys.readouterr()
        assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_tron_validate_schema_understands_underscores(mock_get_file_contents, capsys):
    tron_content = """
_my_template: &a_template
  actions:
    first:
      command: echo hello world

test_job:
  node: batch_box
  schedule:
    type: cron
    value: "0 7 * * 5"
  <<: *a_template
"""
    mock_get_file_contents.return_value = tron_content
    assert validate_schema("unused_service_path.yaml", "tron")
    output, _ = capsys.readouterr()
    assert SCHEMA_VALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_tron_validate_schema_job_extra_properties_bad(mock_get_file_contents, capsys):
    tron_content = """
test_job:
  node: batch_box
  schedule: "daily 04:00:00"
  unexpected: 100
  actions:
    first:
      command: echo hello world
"""
    mock_get_file_contents.return_value = tron_content
    assert not validate_schema("unused_service_path.yaml", "tron")
    output, _ = capsys.readouterr()
    assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_tron_validate_schema_actions_extra_properties_bad(
    mock_get_file_contents, capsys
):
    tron_content = """
test_job:
  node: batch_box
  schedule: "daily 04:00:00"
  actions:
    first:
      command: echo hello world
      something_else: true
"""
    mock_get_file_contents.return_value = tron_content
    assert not validate_schema("unused_service_path.yaml", "tron")
    output, _ = capsys.readouterr()
    assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
def test_tron_validate_schema_cleanup_action_extra_properties_bad(
    mock_get_file_contents, capsys
):
    tron_content = """
test_job:
  node: batch_box
  schedule: "daily 04:00:00"
  actions:
    first:
      command: echo hello world
  cleanup_action:
    command: rm output
    other_key: value
"""
    mock_get_file_contents.return_value = tron_content
    assert not validate_schema("unused_service_path.yaml", "tron")
    output, _ = capsys.readouterr()
    assert SCHEMA_INVALID in output


@pytest.mark.parametrize(
    "mock_content",
    (
        """\
    conditions:
        signalfx: []
        prometheus: []
        splunk: []
    """,
        """\
    conditions:
        signalfx: []
        prometheus: []
        splunk: []
    rollback_window_s: 1000
    check_interval_s: 10
    enable_slo_rollback: false
    allowed_failing_queries: 1
    """,
        """\
    conditions:
        signalfx: []
        splunk: []
    """,
        """\
    conditions:
        splunk:
            - query: "some fun splunk query here"
              upper_bound: 100
              lower_bound: null
    """,
        """\
    conditions:
        splunk:
            - query: "some fun splunk query here"
              upper_bound: 100
    """,
        """\
    conditions:
        splunk:
            - query: "some fun splunk query here"
              query_type: results
              upper_bound: 100
              dry_run: true
    """,
    ),
)
def test_rollback_validate_schema(mock_content, capsys):
    # TODO: if we wanted to get fancy, we could use some advanced pytest
    # parametrization to get an exhaustive test of all sources (in this
    # test and in test_rollback_validate_schema_invalid), but that doesn't
    # seem worth it at the moment
    with mock.patch(
        "paasta_tools.cli.cmds.validate.get_file_contents",
        autospec=True,
        return_value=mock_content,
    ):
        assert validate_schema("rollback-not-real.yaml", "rollback")
    output, _ = capsys.readouterr()
    assert SCHEMA_VALID in output


@pytest.mark.parametrize(
    "mock_content",
    (
        "",
        "not_a_property: true",
        "conditions: []",
        """\
        conditions:
            splunk:
                - upper_bound: 100
        """,
        """\
        conditions:
            splunk:
                - query: testing...
                  upper_bound: "100"
        """,
        """\
        conditions:
            splunk:
                - query: testing...
                  query_type: "100"
        """,
        """\
        conditions:
            prometheus:
                - query: testing...
                  query_type: "results"
        """,
        """\
    conditions:
        splarnk:
            - query: "some fun splunk query here"
              query_type: results
              upper_bound: 100
              dry_run: true
    """,
    ),
)
def test_rollback_validate_schema_invalid(mock_content, capsys):
    with mock.patch(
        "paasta_tools.cli.cmds.validate.get_file_contents",
        autospec=True,
        return_value=mock_content,
    ):
        assert not validate_schema("rollback-not-real.yaml", "rollback")
    output, _ = capsys.readouterr()
    assert SCHEMA_INVALID in output


@patch("paasta_tools.cli.cmds.validate.list_tron_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_complete_config", autospec=True)
def test_validate_tron_with_service_invalid(
    mock_validate_tron_config, mock_list_clusters, capsys
):
    mock_list_clusters.return_value = ["dev", "stage", "prod"]
    mock_validate_tron_config.side_effect = [[], ["some error"], []]

    assert not validate_tron("soa/my_service")
    mock_list_clusters.assert_called_once_with("my_service", "soa")
    expected_calls = [
        mock.call("my_service", cluster, "soa")
        for cluster in mock_list_clusters.return_value
    ]
    assert mock_validate_tron_config.call_args_list == expected_calls

    output, _ = capsys.readouterr()
    assert "some error" in output


@patch("paasta_tools.cli.cmds.validate.list_tron_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.validate_complete_config", autospec=True)
def test_validate_tron_with_service_valid(
    mock_validate_tron_config, mock_list_clusters, capsys
):
    mock_list_clusters.return_value = ["dev", "prod"]
    mock_validate_tron_config.side_effect = [[], []]

    assert validate_tron("soa/my_service")
    mock_list_clusters.assert_called_once_with("my_service", "soa")
    expected_calls = [
        mock.call("my_service", cluster, "soa")
        for cluster in mock_list_clusters.return_value
    ]
    assert mock_validate_tron_config.call_args_list == expected_calls

    output, _ = capsys.readouterr()
    assert "tron-dev.yaml is valid" in output


def test_check_service_path_none(capsys):
    service_path = None
    assert not check_service_path(service_path)

    output, _ = capsys.readouterr()
    assert "%s is not a directory" % service_path in output


@patch("paasta_tools.cli.cmds.validate.os.path.isdir", autospec=True)
def test_check_service_path_empty(mock_isdir, capsys):
    mock_isdir.return_value = True
    service_path = "fake/path"
    assert not check_service_path(service_path)

    output, _ = capsys.readouterr()
    assert "%s does not contain any .yaml files" % service_path in output


@patch("paasta_tools.cli.cmds.validate.os.path.isdir", autospec=True)
@patch("paasta_tools.cli.cmds.validate.glob", autospec=True)
def test_check_service_path_good(mock_glob, mock_isdir):
    mock_isdir.return_value = True
    mock_glob.return_value = True
    service_path = "fake/path"
    assert check_service_path(service_path)


@patch("paasta_tools.cli.cmds.validate.get_service_instance_list", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
def test_validate_unique_service_name_success(
    mock_list_clusters, mock_get_service_instance_list
):
    service_name = "service_1"
    mock_list_clusters.return_value = ["cluster_1"]
    mock_get_service_instance_list.return_value = [
        (service_name, "instance_1"),
        (service_name, "instance_2"),
        (service_name, "instance_3"),
    ]
    assert validate_unique_instance_names(f"soa/{service_name}")


@patch("paasta_tools.cli.cmds.validate.get_service_instance_list", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
def test_validate_unique_service_name_failure(
    mock_list_clusters, mock_get_service_instance_list, capsys
):
    service_name = "service_1"
    mock_list_clusters.return_value = ["cluster_1"]
    mock_get_service_instance_list.return_value = [
        (service_name, "instance_1"),
        (service_name, "instance_2"),
        (service_name, "instance_1"),
    ]
    assert not validate_unique_instance_names(f"soa/{service_name}")

    output, _ = capsys.readouterr()
    assert "instance_1" in output


@patch("paasta_tools.cli.cmds.validate.get_instance_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.path_to_soa_dir_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.check_secrets_for_instance", autospec=True)
def test_validate_secrets(
    mock_check_secrets_for_instance,
    mock_load_system_paasta_config,
    mock_path_to_soa_dir_service,
    mock_list_all_instances_for_service,
    mock_list_clusters,
    mock_get_instance_config,
    capsys,
):
    mock_path_to_soa_dir_service.return_value = ("fake_soa_dir", "fake_service")
    mock_list_clusters.return_value = ["fake_cluster"]
    mock_load_system_paasta_config.return_value = mock.Mock(
        get_vault_cluster_config=mock.Mock(
            return_value={"fake_cluster": "fake_vault_env"}
        )
    )
    mock_list_all_instances_for_service.return_value = [
        "fake_instance",
        "fake_instance2",
    ]
    mock_paasta_instance = mock.Mock(
        config_dict={"env": {"SUPER_SECRET1": "SECRET(secret1)"}}
    )
    mock_paasta_instance2 = mock.Mock(
        config_dict={"env": {"SUPER_SECRET1": "SHARED_SECRET(secret1)"}}
    )
    mock_get_instance_config.side_effect = [mock_paasta_instance, mock_paasta_instance2]
    mock_check_secrets_for_instance.return_value = True
    assert validate_secrets("fake-service-path"), capsys
    captured = capsys.readouterr()
    assert "No orphan secrets found" in captured.out
    assert mock_check_secrets_for_instance.call_count == 2


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
@patch("paasta_tools.cli.cmds.validate.os.path.isfile", autospec=True)
def test_check_secrets_for_instance(mock_isfile, mock_get_file_contents):
    instance_config_dict = {"env": {"SUPER_SECRET1": "SECRET(secret1)"}}
    soa_dir = "fake_soa_dir"
    service_path = "fake-service-path"
    vault_env = "fake_vault_env"
    secret_content = """
{
    "environments": {
        "fake_vault_env": {
            "ciphertext": "bla"
        }
    }
}
"""
    mock_get_file_contents.return_value = secret_content
    mock_isfile.return_value = True
    assert check_secrets_for_instance(
        instance_config_dict, soa_dir, service_path, vault_env
    )
    mock_get_file_contents.assert_called_with("fake-service-path/secrets/secret1.json")
    instance_config_dict = {"env": {"SUPER_SECRET1": "SHARED_SECRET(secret1)"}}
    assert check_secrets_for_instance(
        instance_config_dict, soa_dir, service_path, vault_env
    )
    mock_get_file_contents.assert_called_with(
        "fake_soa_dir/_shared/secrets/secret1.json"
    )


@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
@patch("paasta_tools.cli.cmds.validate.os.path.isfile", autospec=True)
def test_check_secrets_for_instance_missing_secret(
    mock_isfile, mock_get_file_contents, capsys
):
    instance_config_dict = {"env": {"SUPER_SECRET1": "SECRET(secret1)"}}
    soa_dir = "fake_soa_dir"
    service_path = "fake-service-path"
    vault_env = "even_more_fake_vault_env"
    secret_content = """
{
    "environments": {
        "fake_vault_env": {
            "ciphertext": "bla"
        }
    }
}
"""
    mock_get_file_contents.return_value = secret_content
    mock_isfile.return_value = True
    assert not check_secrets_for_instance(
        instance_config_dict, soa_dir, service_path, vault_env
    ), capsys
    captured = capsys.readouterr()
    assert (
        "Secret secret1 not defined for ecosystem even_more_fake_vault_env on secret file fake-service-path/secrets/secret1.json"
        in captured.out
    )


@pytest.mark.parametrize(
    "setpoint,offset,expected",
    [
        (0.5, 0.5, False),
        (0.5, 0.6, False),
        (0.8, 0.25, True),
    ],
)
@patch("paasta_tools.cli.cmds.validate.get_instance_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.path_to_soa_dir_service", autospec=True)
def test_validate_autoscaling_configs(
    mock_path_to_soa_dir_service,
    mock_list_clusters,
    mock_list_all_instances_for_service,
    mock_get_instance_config,
    setpoint,
    offset,
    expected,
):
    mock_path_to_soa_dir_service.return_value = ("fake_soa_dir", "fake_service")
    mock_list_clusters.return_value = ["fake_cluster"]
    mock_list_all_instances_for_service.return_value = {"fake_instance1"}
    mock_get_instance_config.return_value = mock.Mock(
        get_instance=mock.Mock(return_value="fake_instance1"),
        get_instance_type=mock.Mock(return_value="kubernetes"),
        is_autoscaling_enabled=mock.Mock(return_value=True),
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "uwsgi",
                "setpoint": setpoint,
                "offset": offset,
            }
        ),
    )

    with mock.patch(
        "paasta_tools.cli.cmds.validate.load_system_paasta_config",
        autospec=True,
        return_value=SystemPaastaConfig(
            config={"skip_cpu_override_validation": ["not-a-real-service"]},
            directory="/some/test/dir",
        ),
    ):
        assert validate_autoscaling_configs("fake-service-path") is expected


@patch("paasta_tools.cli.cmds.validate.get_instance_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.path_to_soa_dir_service", autospec=True)
def test_validate_autoscaling_configs_no_offset_specified(
    mock_path_to_soa_dir_service,
    mock_list_clusters,
    mock_list_all_instances_for_service,
    mock_get_instance_config,
):
    mock_path_to_soa_dir_service.return_value = ("fake_soa_dir", "fake_service")
    mock_list_clusters.return_value = ["fake_cluster"]
    mock_list_all_instances_for_service.return_value = {"fake_instance1"}
    mock_get_instance_config.return_value = mock.Mock(
        get_instance=mock.Mock(return_value="fake_instance1"),
        get_instance_type=mock.Mock(return_value="kubernetes"),
        is_autoscaling_enabled=mock.Mock(return_value=True),
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "uwsgi",
                "setpoint": 0.8,
            }
        ),
    )

    with mock.patch(
        "paasta_tools.cli.cmds.validate.load_system_paasta_config",
        autospec=True,
        return_value=SystemPaastaConfig(
            config={"skip_cpu_override_validation": ["not-a-real-service"]},
            directory="/some/test/dir",
        ),
    ):
        assert validate_autoscaling_configs("fake-service-path") is True


@pytest.mark.parametrize(
    "filecontents,expected",
    [
        ("# overridexxx-cpu-setting", False),
        ("# override-cpu-setting", False),
        ("", False),
        ("# override-cpu-setting (PAASTA-17522)", True),
    ],
)
@patch("paasta_tools.cli.cmds.validate.get_file_contents", autospec=True)
@patch("paasta_tools.cli.cmds.validate.get_instance_config", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.validate.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.validate.path_to_soa_dir_service", autospec=True)
def test_validate_cpu_autotune_override(
    mock_path_to_soa_dir_service,
    mock_list_clusters,
    mock_list_all_instances_for_service,
    mock_get_instance_config,
    mock_get_file_contents,
    filecontents,
    expected,
):
    mock_path_to_soa_dir_service.return_value = ("fake_soa_dir", "fake_service")
    mock_list_clusters.return_value = ["fake_cluster"]
    mock_list_all_instances_for_service.return_value = {"fake_instance1"}
    mock_get_instance_config.return_value = mock.Mock(
        get_instance=mock.Mock(return_value="fake_instance1"),
        get_instance_type=mock.Mock(return_value="kubernetes"),
        is_autoscaling_enabled=mock.Mock(return_value=True),
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "cpu",
                "setpoint": 0.8,
            }
        ),
    )
    mock_get_file_contents.return_value = f"""
---
fake_instance1:
  cpus: 1 {filecontents}
"""

    with mock.patch(
        "paasta_tools.cli.cmds.validate.load_system_paasta_config",
        autospec=True,
        return_value=SystemPaastaConfig(
            config={"skip_cpu_override_validation": ["not-a-real-service"]},
            directory="/some/test/dir",
        ),
    ):
        assert validate_autoscaling_configs("fake-service-path") is expected


@pytest.mark.parametrize(
    "schedule, starting_from, num_runs, expected",
    [
        (
            "0 22 * * 1-5",
            datetime.datetime(2022, 4, 12, 0, 0),
            5,
            [
                datetime.datetime(2022, 4, 12, 22, 0),
                datetime.datetime(2022, 4, 13, 22, 0),
                datetime.datetime(2022, 4, 14, 22, 0),
                datetime.datetime(2022, 4, 15, 22, 0),
                datetime.datetime(2022, 4, 18, 22, 0),
            ],
        ),
        (
            "5 4 * * *",
            datetime.datetime(2020, 12, 4, 18, 0),
            2,
            [
                datetime.datetime(2020, 12, 5, 4, 5),
                datetime.datetime(2020, 12, 6, 4, 5),
            ],
        ),
        (
            "0 17 29 2 *",
            datetime.datetime(2022, 4, 12, 0, 0),
            3,
            [
                datetime.datetime(2024, 2, 29, 17, 0),
                datetime.datetime(2028, 2, 29, 17, 0),
                datetime.datetime(2032, 2, 29, 17, 0),
            ],
        ),
    ],
)
def test_list_upcoming_runs(schedule, starting_from, num_runs, expected):
    assert list_upcoming_runs(schedule, starting_from, num_runs) == expected


@pytest.mark.parametrize(
    "burst, comment, expected",
    [
        (3, "# overridexxx-cpu-burst", False),
        (4, "# override-cpu-burst", False),
        (5, "", False),
        (6, "# override-cpu-burst (MAGIC-42)", True),
        (7, "# override-cpu-burst (SECURE-1234#some comment)", True),
        (1, "# override-cpu-burst (HWAT-789)", True),
        (1, "# override-cpu-burst", True),
    ],
)
def test_validate_cpu_burst_override(
    burst,
    comment,
    expected,
):
    instance_config = f"""
---
fake_instance1:
  cpu_burst_add: {burst} {comment}
"""

    with mock.patch(
        "paasta_tools.cli.cmds.validate.load_system_paasta_config",
        autospec=True,
        return_value=SystemPaastaConfig(
            config={"skip_cpu_burst_validation": ["not-a-real-service"]},
            directory="/some/test/dir",
        ),
    ), mock.patch(
        "paasta_tools.cli.cmds.validate.get_file_contents",
        autospec=True,
        return_value=instance_config,
    ), mock.patch(
        "paasta_tools.cli.cmds.validate.get_instance_config",
        autospec=True,
        return_value=mock.Mock(
            get_instance=mock.Mock(return_value="fake_instance1"),
            get_instance_type=mock.Mock(return_value="kubernetes"),
        ),
    ), mock.patch(
        "paasta_tools.cli.cmds.validate.list_all_instances_for_service",
        autospec=True,
        return_value={"fake_instance1"},
    ), mock.patch(
        "paasta_tools.cli.cmds.validate.list_clusters",
        autospec=True,
        return_value=["fake_cluster"],
    ), mock.patch(
        "paasta_tools.cli.cmds.validate.path_to_soa_dir_service",
        autospec=True,
        return_value=("fake_soa_dir", "fake_service"),
    ):
        assert validate_cpu_burst("fake-service-path") is expected
