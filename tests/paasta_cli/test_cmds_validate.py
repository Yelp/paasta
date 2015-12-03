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

from mock import patch
from pytest import raises
from StringIO import StringIO

from paasta_tools.paasta_cli.cmds.validate import get_schema
from paasta_tools.paasta_cli.cmds.validate import validate_schema
from paasta_tools.paasta_cli.cmds.validate import paasta_validate
from paasta_tools.paasta_cli.utils import PaastaValidateMessages


@patch('paasta_tools.paasta_cli.cmds.validate.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.validate.validate_all_schemas')
def test_paasta_validate_calls_everything(
        mock_validate_all_schemas,
        mock_guess_service_name
):
    # Ensure each check in 'paasta_validate' is called

    mock_guess_service_name.return_value = 'servicedocs'
    paasta_validate(None)

    assert mock_validate_all_schemas.called


def test_get_schema_marathon_found():
    get_schema('marathon')


def test_get_schema_chronos_found():
    get_schema('chronos')


def test_get_schema_missing():
    with raises(AttributeError):
        get_schema('fake_schema')


@patch('paasta_tools.paasta_cli.cmds.validate.get_file_contents')
def test_marathon_validate_schema_list_hashes_good(
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

    validate_schema('unused_service_path.yaml', 'marathon')


@patch('paasta_tools.paasta_cli.cmds.validate.get_file_contents')
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
    validate_schema('unused_service_path.json', 'marathon')

    output = mock_stdout.getvalue()

    assert PaastaValidateMessages.SCHEMA_INVALID in output


@patch('paasta_tools.paasta_cli.cmds.validate.get_file_contents')
def test_chronos_validate_schema_list_hashes_good(
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
    validate_schema('unused_service_path.json', 'chronos')


@patch('paasta_tools.paasta_cli.cmds.validate.get_file_contents')
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
    validate_schema('unused_service_path.json', 'chronos')

    output = mock_stdout.getvalue()

    assert PaastaValidateMessages.SCHEMA_INVALID in output
