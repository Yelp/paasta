#!/usr/bin/env python
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

import json
import os
import yaml

from glob import glob

from jsonschema import Draft4Validator
from jsonschema import FormatChecker
from jsonschema import ValidationError
from paasta_tools.paasta_cli.utils import guess_service_name
from paasta_tools.paasta_cli.utils import PaastaValidateMessages
from paasta_tools.paasta_cli.utils import PaastaValidateSchemas


def get_file_contents(path):
    """Open a file for reading

    :param path: path of file to read"""
    return open(path).read()


def get_schema(file_type):
    """Get the correct schema to use for validation

    :param file_type: what schema type should we validate against"""
    schema_name = '%s_SCHEMA' % file_type.upper()
    return getattr(PaastaValidateSchemas, schema_name)


def validate_schema(file_path, file_type):
    """Check if the specified config file has a valid schema

    :param file_path: path to file to validate
    :param file_type: what schema type should we validate against"""
    schema = get_schema(file_type)
    validator = Draft4Validator(schema, format_checker=FormatChecker())
    basename = os.path.basename(file_path)
    extension = os.path.splitext(basename)[1]
    config_file = get_file_contents(file_path)
    if extension == '.yaml':
        config_file_object = yaml.load(config_file)
    elif extension == '.json':
        config_file_object = json.loads(config_file)
    else:
        config_file_object = config_file
    try:
        validator.validate(config_file_object)
    except ValidationError as e:
        print '%s: %s' % (PaastaValidateMessages.SCHEMA_INVALID, file_path)
        print '  Validation Message: %s' % e.message
    else:
        print '%s: %s' % (PaastaValidateMessages.SCHEMA_VALID, file_path)


def validate_all_schemas(service_path):
    """Finds all recognized config files in service directory,
    and validates their schema.

    :param service_path: path to location of configuration files"""

    path = os.path.join(service_path, '*.yaml')

    for file_name in glob(path):
        basename = os.path.basename(file_name)
        for file_type in ['chronos', 'marathon']:
            if basename.startswith(file_type):
                validate_schema(basename, file_type)


def add_subparser(subparsers):
    validate_parser = subparsers.add_parser(
        'validate',
        description="Execute 'paasta validate' from service repo root",
        help="Validate that all paasta config files in pwd are correct")
    validate_parser.set_defaults(command=paasta_validate)


def paasta_validate(args):
    """Analyze the service in the PWD to determine if conf files are all valid
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service = guess_service_name()
    service_path = os.path.join('/nail/etc/services', service)

    validate_all_schemas(service_path)
