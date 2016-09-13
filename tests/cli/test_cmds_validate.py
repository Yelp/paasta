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
import os
from StringIO import StringIO

import mock
from mock import patch

import paasta_tools.chronos_tools
from paasta_tools.cli.cmds.validate import check_service_path
from paasta_tools.cli.cmds.validate import get_schema
from paasta_tools.cli.cmds.validate import get_service_path
from paasta_tools.cli.cmds.validate import invalid_chronos_instance
from paasta_tools.cli.cmds.validate import paasta_validate
from paasta_tools.cli.cmds.validate import paasta_validate_soa_configs
from paasta_tools.cli.cmds.validate import SCHEMA_INVALID
from paasta_tools.cli.cmds.validate import SCHEMA_VALID
from paasta_tools.cli.cmds.validate import UNKNOWN_SERVICE
from paasta_tools.cli.cmds.validate import valid_chronos_instance
from paasta_tools.cli.cmds.validate import validate_chronos
from paasta_tools.cli.cmds.validate import validate_schema


@patch('paasta_tools.cli.cmds.validate.validate_all_schemas', autospec=True)
@patch('paasta_tools.cli.cmds.validate.validate_chronos', autospec=True)
@patch('paasta_tools.cli.cmds.validate.get_service_path', autospec=True)
@patch('paasta_tools.cli.cmds.validate.check_service_path', autospec=True)
def test_paasta_validate_calls_everything(
    mock_check_service_path,
    mock_get_service_path,
    mock_validate_chronos,
    mock_validate_all_schemas
):
    # Ensure each check in 'paasta_validate' is called

    mock_check_service_path.return_value = True
    mock_get_service_path.return_value = 'unused_path'
    mock_validate_all_schemas.return_value = True
    mock_validate_chronos.return_value = True

    args = mock.MagicMock()
    args.service = None
    args.soa_dir = None

    paasta_validate(args)

    assert mock_validate_all_schemas.called
    assert mock_validate_chronos.called


@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_get_service_path_unknown(
    mock_stdout
):
    service = None
    soa_dir = 'unused'

    assert get_service_path(service, soa_dir) is None

    output = mock_stdout.getvalue()

    assert UNKNOWN_SERVICE in output


def test_validate_unknown_service():
    args = mock.MagicMock()
    args.service = None
    args.yelpsoa_config_root = 'unused'
    paasta_validate(args) == 1


def test_validate_unknown_service_service_path():
    service_path = 'unused/path'

    assert not paasta_validate_soa_configs(service_path)


@patch('paasta_tools.cli.cmds.validate.os.path.isdir', autospec=True)
@patch('paasta_tools.cli.cmds.validate.glob', autospec=True)
def test_get_service_path_cwd(
    mock_glob,
    mock_isdir
):
    mock_isdir.return_value = True
    mock_glob.return_value = ['something.yaml']

    service = None
    soa_dir = os.getcwd()

    service_path = get_service_path(service, soa_dir)

    assert service_path == os.getcwd()


@patch('paasta_tools.cli.cmds.validate.os.path.isdir', autospec=True)
@patch('paasta_tools.cli.cmds.validate.glob', autospec=True)
def test_get_service_path_soa_dir(
    mock_glob,
    mock_isdir
):
    mock_isdir.return_value = True
    mock_glob.return_value = ['something.yaml']

    service = 'some_service'
    soa_dir = 'some/path'

    service_path = get_service_path(service, soa_dir)

    assert service_path == '%s/%s' % (soa_dir, service)


def is_schema(schema):
    assert schema is not None
    assert isinstance(schema, dict)
    assert '$schema' in schema


def test_get_schema_marathon_found():
    schema = get_schema('marathon')
    is_schema(schema)


def test_get_schema_chronos_found():
    schema = get_schema('chronos')
    is_schema(schema)


def test_get_schema_missing():
    assert get_schema('fake_schema') is None


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_marathon_validate_schema_list_hashes_good(
    mock_stdout,
    mock_get_file_contents
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
  healthcheck_cmd: '/bin/true'
main_http:
  cpus: 0.1
  instances: 2
  mem: 250
  disk: 512
  registrations: ['foo.bar', 'bar.baz']
"""
    mock_get_file_contents.return_value = marathon_content
    assert validate_schema('unused_service_path.yaml', 'marathon')
    output = mock_stdout.getvalue()
    assert SCHEMA_VALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_marathon_validate_schema_healthcheck_non_cmd(
    mock_stdout,
    mock_get_file_contents
):
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
    assert validate_schema('unused_service_path.yaml', 'marathon')
    output = mock_stdout.getvalue()
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
    mock_stdout.truncate(0)
    assert validate_schema('unused_service_path.yaml', 'marathon')
    output = mock_stdout.getvalue()
    assert SCHEMA_VALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_marathon_validate_schema_healthcheck_cmd_has_cmd(
    mock_stdout,
    mock_get_file_contents
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
    assert not validate_schema('unused_service_path.yaml', 'marathon')
    output = mock_stdout.getvalue()
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
    mock_stdout.truncate(0)
    assert validate_schema('unused_service_path.yaml', 'marathon')
    output = mock_stdout.getvalue()
    assert SCHEMA_VALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_marathon_validate_schema_keys_outside_instance_blocks_bad(
    mock_stdout,
    mock_get_file_contents
):
    mock_get_file_contents.return_value = """
{
    "main": {
        "instances": 5
    },
    "page": false
}
"""
    assert not validate_schema('unused_service_path.json', 'marathon')

    output = mock_stdout.getvalue()

    assert SCHEMA_INVALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_marathon_validate_invalid_key_bad(
    mock_stdout,
    mock_get_file_contents
):
    mock_get_file_contents.return_value = """
{
    "main": {
        "fake_key": 5
    }
}
"""
    assert not validate_schema('unused_service_path.json', 'marathon')

    output = mock_stdout.getvalue()

    assert SCHEMA_INVALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_chronos_validate_schema_list_hashes_good(
    mock_stdout,
    mock_get_file_contents
):
    mock_get_file_contents.return_value = """
{
    "daily_job": {
        "schedule": "bar"
    },
    "wheekly": {
        "schedule": "baz"
    }
}
"""
    assert validate_schema('unused_service_path.json', 'chronos')

    output = mock_stdout.getvalue()

    assert SCHEMA_VALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_chronos_validate_schema_keys_outside_instance_blocks_bad(
    mock_stdout,
    mock_get_file_contents
):
    mock_get_file_contents.return_value = """
{
    "daily_job": {
        "schedule": "bar"
    },
    "page": false
}
"""
    assert not validate_schema('unused_service_path.json', 'chronos')

    output = mock_stdout.getvalue()

    assert SCHEMA_INVALID in output


@patch('paasta_tools.cli.cmds.validate.get_services_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_clusters', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config', autospec=True)
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_failing_chronos_job_validate(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters,
    mock_get_services_for_cluster
):
    fake_service = 'fake-service'
    fake_instance = 'fake-instance'
    fake_cluster = 'penguin'

    mock_chronos_job = mock.Mock(autospec=True)
    mock_chronos_job.get_parents.return_value = None
    mock_chronos_job.validate.return_value = (False, ['something is wrong with the config'])

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', fake_service)
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_get_services_for_cluster.return_value = [(fake_service, fake_instance)]
    mock_load_chronos_job_config.return_value = mock_chronos_job

    assert not validate_chronos('fake_service_path')

    output = mock_stdout.getvalue()

    expected_output = 'something is wrong with the config'
    assert invalid_chronos_instance(fake_cluster, fake_instance, expected_output) in output


@patch('paasta_tools.cli.cmds.validate.get_services_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_clusters', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config', autospec=True)
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_failing_chronos_job_self_dependent(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters,
    mock_get_services_for_cluster
):
    fake_service = 'fake-service'
    fake_instance = 'fake-instance'
    fake_cluster = 'penguin'
    chronos_spacer = paasta_tools.chronos_tools.INTERNAL_SPACER

    mock_chronos_job = mock.Mock(autospec=True)
    mock_chronos_job.get_parents.return_value = ["%s%s%s" % (fake_service, chronos_spacer, fake_instance)]
    mock_chronos_job.validate.return_value = (True, [])

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', fake_service)
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_get_services_for_cluster.return_value = [(fake_service, fake_instance)]
    mock_load_chronos_job_config.return_value = mock_chronos_job

    assert not validate_chronos('fake_service_path')

    output = mock_stdout.getvalue()

    expected_output = 'Job fake-service.fake-instance cannot depend on itself'
    assert invalid_chronos_instance(fake_cluster, fake_instance, expected_output) in output


@patch('paasta_tools.cli.cmds.validate.get_services_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_clusters', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config', autospec=True)
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_failing_chronos_job_missing_parent(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters,
    mock_get_services_for_cluster
):
    fake_service = 'fake-service'
    fake_instance = 'fake-instance'
    fake_cluster = 'penguin'
    chronos_spacer = paasta_tools.chronos_tools.INTERNAL_SPACER

    mock_chronos_job = mock.Mock(autospec=True)
    mock_chronos_job.get_parents.return_value = ["%s%s%s" % (fake_service, chronos_spacer, 'parent-1')]
    mock_chronos_job.validate.return_value = (True, [])

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', fake_service)
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_get_services_for_cluster.return_value = [(fake_service, fake_instance)]
    mock_load_chronos_job_config.return_value = mock_chronos_job

    assert not validate_chronos('fake_service_path')

    output = mock_stdout.getvalue()

    expected_output = 'Parent job fake-service.parent-1 could not be found'
    assert invalid_chronos_instance(fake_cluster, fake_instance, expected_output) in output


@patch('paasta_tools.cli.cmds.validate.get_services_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_clusters', autospec=True)
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config', autospec=True)
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_validate_chronos_valid_instance(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters,
    mock_get_services_for_cluster
):
    fake_service = 'fake-service'
    fake_instance = 'fake-instance'
    fake_cluster = 'penguin'

    mock_chronos_job = mock.Mock(autospec=True)
    mock_chronos_job.get_parents.return_value = None
    mock_chronos_job.validate.return_value = (True, [])

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', fake_service)
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_get_services_for_cluster.return_value = [(fake_service, fake_instance)]
    mock_load_chronos_job_config.return_value = mock_chronos_job

    assert validate_chronos('fake_service_path')

    output = mock_stdout.getvalue()

    assert valid_chronos_instance(fake_cluster, fake_instance) in output


@patch("paasta_tools.chronos_tools.TMP_JOB_IDENTIFIER", 'tmp', autospec=None)
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_validate_chronos_tmp_job(
    mock_stdout,
    mock_path_to_soa_dir_service,
):
    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', 'tmp')
    assert validate_chronos('fake_path/tmp') is False
    assert ("Services using scheduled tasks cannot be named tmp, as it clashes"
            " with the identifier used for temporary jobs") in \
        mock_stdout.getvalue()


@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_check_service_path_none(
    mock_stdout
):
    service_path = None
    assert not check_service_path(service_path)

    output = mock_stdout.getvalue()
    assert "%s is not a directory" % service_path in output


@patch('sys.stdout', new_callable=StringIO, autospec=None)
@patch('paasta_tools.cli.cmds.validate.os.path.isdir', autospec=True)
def test_check_service_path_empty(
    mock_isdir,
    mock_stdout
):
    mock_isdir.return_value = True
    service_path = 'fake/path'
    assert not check_service_path(service_path)

    output = mock_stdout.getvalue()
    assert "%s does not contain any .yaml files" % service_path in output


@patch('paasta_tools.cli.cmds.validate.os.path.isdir', autospec=True)
@patch('paasta_tools.cli.cmds.validate.glob', autospec=True)
def test_check_service_path_good(
    mock_glob,
    mock_isdir
):
    mock_isdir.return_value = True
    mock_glob.return_value = True
    service_path = 'fake/path'
    assert check_service_path(service_path)
