# Copyright 2015 Yelp Inc.
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

import mock
import os

from mock import patch
from pytest import raises
from StringIO import StringIO

from paasta_tools.chronos_tools import ChronosJobConfig
from paasta_tools.cli.cmds.validate import get_schema
from paasta_tools.cli.cmds.validate import get_service_path
from paasta_tools.cli.cmds.validate import invalid_chronos_instance
from paasta_tools.cli.cmds.validate import valid_chronos_instance
from paasta_tools.cli.cmds.validate import validate_chronos
from paasta_tools.cli.cmds.validate import validate_schema
from paasta_tools.cli.cmds.validate import paasta_validate
from paasta_tools.cli.cmds.validate import SCHEMA_VALID
from paasta_tools.cli.cmds.validate import SCHEMA_INVALID
from paasta_tools.cli.cmds.validate import UNKNOWN_SERVICE


@patch('paasta_tools.cli.cmds.validate.validate_all_schemas')
@patch('paasta_tools.cli.cmds.validate.validate_chronos')
@patch('paasta_tools.cli.cmds.validate.get_service_path')
def test_paasta_validate_calls_everything(
    mock_get_service_path,
    mock_validate_chronos,
    mock_validate_all_schemas
):
    # Ensure each check in 'paasta_validate' is called

    mock_get_service_path.return_value = 'unused_path'
    mock_validate_all_schemas.return_value = 0
    mock_validate_chronos.return_value = 0

    args = mock.MagicMock()
    args.service = None
    args.soa_dir = None

    paasta_validate(args)

    assert mock_validate_all_schemas.called
    assert mock_validate_chronos.called


@patch('sys.stdout', new_callable=StringIO)
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

    with raises(SystemExit) as excinfo:
        paasta_validate(args)

    assert excinfo.value.code == 1


@patch('paasta_tools.cli.cmds.validate.os.path.isdir')
@patch('paasta_tools.cli.cmds.validate.glob')
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


@patch('paasta_tools.cli.cmds.validate.os.path.isdir')
@patch('paasta_tools.cli.cmds.validate.glob')
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


@patch('paasta_tools.cli.cmds.validate.get_file_contents')
@patch('sys.stdout', new_callable=StringIO)
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
  cmd: virtualenv_run/bin/python adindexer/adindex_worker.py
  healthcheck_mode: cmd
main_http:
  cpus: 0.1
  instances: 2
  mem: 250
"""
    mock_get_file_contents.return_value = marathon_content

    assert validate_schema('unused_service_path.yaml', 'marathon') == 0

    output = mock_stdout.getvalue()

    assert SCHEMA_VALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents')
@patch('sys.stdout', new_callable=StringIO)
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
    assert validate_schema('unused_service_path.json', 'marathon') == 1

    output = mock_stdout.getvalue()

    assert SCHEMA_INVALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents')
@patch('sys.stdout', new_callable=StringIO)
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
    assert validate_schema('unused_service_path.json', 'chronos') == 0

    output = mock_stdout.getvalue()

    assert SCHEMA_VALID in output


@patch('paasta_tools.cli.cmds.validate.get_file_contents')
@patch('sys.stdout', new_callable=StringIO)
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
    assert validate_schema('unused_service_path.json', 'chronos') == 1

    output = mock_stdout.getvalue()

    assert SCHEMA_INVALID in output


@patch('paasta_tools.cli.cmds.validate.list_clusters')
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service')
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config')
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service')
@patch('sys.stdout', new_callable=StringIO)
def test_validate_chronos_missing_schedule(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters
):
    fake_service = 'test-service'
    fake_instance = 'fake-instance'
    fake_job_name = 'test'
    fake_cluster = 'penguin'
    fake_monitoring_info = {'fake_monitoring_info': 'fake_monitoring_value'}
    fake_config_dict = {
        'bounce_method': 'graceful',
        'cmd': '/bin/sleep 40',
        'epsilon': 'PT30M',
        'retries': 5,
        'cpus': 5.5,
        'mem': 1024.4,
        'disabled': True,
        'schedule_time_zone': 'Zulu',
        'monitoring': fake_monitoring_info,
    }
    fake_branch_dict = {
        'desired_state': 'start',
        'docker_image': 'paasta-%s-%s' % (fake_service, fake_cluster),
    }
    fake_chronos_job_config = ChronosJobConfig(fake_service,
                                               fake_job_name,
                                               fake_config_dict,
                                               fake_branch_dict)

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', 'fake_service')
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_load_chronos_job_config.return_value = fake_chronos_job_config

    assert validate_chronos('fake_service_path') == 1

    output = mock_stdout.getvalue()

    expected_output = 'You must specify a "schedule" in your configuration'
    assert invalid_chronos_instance(fake_cluster, fake_instance, expected_output) in output


@patch('paasta_tools.cli.cmds.validate.list_clusters')
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service')
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config')
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service')
@patch('sys.stdout', new_callable=StringIO)
def test_validate_chronos_invalid_instance(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters
):
    fake_service = 'test-service'
    fake_instance = 'fake-instance'
    fake_job_name = 'test'
    fake_cluster = 'penguin'
    fake_monitoring_info = {'fake_monitoring_info': 'fake_monitoring_value'}
    fake_config_dict = {
        'bounce_method': 'graceful',
        'cmd': '/bin/sleep 40',
        'epsilon': 'PT30M',
        'retries': 5,
        'cpus': 5.5,
        'mem': 1024.4,
        'disabled': True,
        'schedule': 'P1DT09:00:00',
        'schedule_time_zone': 'Zulu',
        'monitoring': fake_monitoring_info,
    }
    fake_branch_dict = {
        'desired_state': 'start',
        'docker_image': 'paasta-%s-%s' % (fake_service, fake_cluster),
    }
    fake_chronos_job_config = ChronosJobConfig(fake_service,
                                               fake_job_name,
                                               fake_config_dict,
                                               fake_branch_dict)

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', 'fake_service')
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_load_chronos_job_config.return_value = fake_chronos_job_config

    assert validate_chronos('fake_service_path') == 1

    output = mock_stdout.getvalue()

    expected_output = 'The specified schedule "%s" is invalid' % \
        fake_config_dict['schedule']
    assert invalid_chronos_instance(fake_cluster, fake_instance, expected_output) in output


@patch('paasta_tools.cli.cmds.validate.list_clusters')
@patch('paasta_tools.cli.cmds.validate.list_all_instances_for_service')
@patch('paasta_tools.cli.cmds.validate.load_chronos_job_config')
@patch('paasta_tools.cli.cmds.validate.path_to_soa_dir_service')
@patch('sys.stdout', new_callable=StringIO)
def test_validate_chronos_valid_instance(
    mock_stdout,
    mock_path_to_soa_dir_service,
    mock_load_chronos_job_config,
    mock_list_all_instances_for_service,
    mock_list_clusters
):
    fake_service = 'test-service'
    fake_instance = 'fake-instance'
    fake_job_name = 'test'
    fake_cluster = 'penguin'
    fake_monitoring_info = {'fake_monitoring_info': 'fake_monitoring_value'}
    fake_config_dict = {
        'bounce_method': 'graceful',
        'cmd': '/bin/sleep 40',
        'epsilon': 'PT30M',
        'retries': 5,
        'cpus': 5.5,
        'mem': 1024.4,
        'disabled': True,
        'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
        'schedule_time_zone': 'Zulu',
        'monitoring': fake_monitoring_info,
    }
    fake_branch_dict = {
        'desired_state': 'start',
        'docker_image': 'paasta-%s-%s' % (fake_service, fake_cluster),
    }
    fake_chronos_job_config = ChronosJobConfig(fake_service,
                                               fake_job_name,
                                               fake_config_dict,
                                               fake_branch_dict)

    mock_path_to_soa_dir_service.return_value = ('fake_soa_dir', 'fake_service')
    mock_list_clusters.return_value = [fake_cluster]
    mock_list_all_instances_for_service.return_value = [fake_instance]
    mock_load_chronos_job_config.return_value = fake_chronos_job_config

    assert validate_chronos('fake_service_path') == 0

    output = mock_stdout.getvalue()

    assert valid_chronos_instance(fake_cluster, fake_instance) in output
