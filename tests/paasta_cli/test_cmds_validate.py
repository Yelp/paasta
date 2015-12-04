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
from StringIO import StringIO

from paasta_tools.paasta_cli.cmds.validate import get_schema
from paasta_tools.paasta_cli.cmds.validate import validate_schema
from paasta_tools.paasta_cli.cmds.validate import paasta_validate
from paasta_tools.paasta_cli.cmds.validate import SCHEMA_VALID
from paasta_tools.paasta_cli.cmds.validate import SCHEMA_INVALID


@patch('paasta_tools.paasta_cli.cmds.validate.validate_all_schemas')
def test_paasta_validate_calls_everything(
        mock_validate_all_schemas
):
    # Ensure each check in 'paasta_validate' is called

    paasta_validate(None)

    assert mock_validate_all_schemas.called


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


@patch('paasta_tools.paasta_cli.cmds.validate.get_file_contents')
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

    validate_schema('unused_service_path.yaml', 'marathon')

    output = mock_stdout.getvalue()

    assert SCHEMA_VALID in output


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

    assert SCHEMA_INVALID in output


@patch('paasta_tools.paasta_cli.cmds.validate.get_file_contents')
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
    validate_schema('unused_service_path.json', 'chronos')

    output = mock_stdout.getvalue()

    assert SCHEMA_VALID in output


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

    assert SCHEMA_INVALID in output
