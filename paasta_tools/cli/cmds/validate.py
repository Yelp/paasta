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
import pkgutil
import sys
from glob import glob

import yaml
from jsonschema import Draft4Validator
from jsonschema import FormatChecker
from jsonschema import ValidationError

from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.cli.utils import failure
from paasta_tools.cli.utils import get_file_contents
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.cli.utils import success
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters


SCHEMA_VALID = success("Successfully validated schema")

SCHEMA_INVALID = failure(
    "Failed to validate schema. More info:",
    "http://paasta.readthedocs.org/en/latest/yelpsoa_configs.html")

SCHEMA_NOT_FOUND = failure(
    "Failed to find schema to validate against. More info:",
    "http://paasta.readthedocs.org/en/latest/yelpsoa_configs.html")

FAILED_READING_FILE = failure(
    "Failed to read file. More info:",
    "http://paasta.readthedocs.org/en/latest/yelpsoa_configs.html")

UNKNOWN_SERVICE = "Unable to determine service to validate.\n" \
                  "Please supply the %s name you wish to " \
                  "validate with the %s option." \
                  % (PaastaColors.cyan('SERVICE'), PaastaColors.cyan('-s'))


def invalid_chronos_instance(cluster, instance, output):
    return failure(
        'chronos-%s.yaml has an invalid instance: %s.\n  %s\n  '
        'More info:' % (cluster, instance, output),
        "http://paasta.readthedocs.org/en/latest/yelpsoa_configs.html#chronos-clustername-yaml")


def valid_chronos_instance(cluster, instance):
    return success('chronos-%s.yaml has a valid instance: %s.' % (cluster, instance))


def get_schema(file_type):
    """Get the correct schema to use for validation

    :param file_type: what schema type should we validate against
    """
    schema_path = 'schemas/%s_schema.json' % file_type
    try:
        schema = pkgutil.get_data('paasta_tools.cli', schema_path)
    except IOError:
        return None
    return json.loads(schema)


def validate_schema(file_path, file_type):
    """Check if the specified config file has a valid schema

    :param file_path: path to file to validate
    :param file_type: what schema type should we validate against
    """
    schema = get_schema(file_type)
    if (schema is None):
        print '%s: %s' % (SCHEMA_NOT_FOUND, file_path)
        return
    validator = Draft4Validator(schema, format_checker=FormatChecker())
    basename = os.path.basename(file_path)
    extension = os.path.splitext(basename)[1]
    try:
        config_file = get_file_contents(file_path)
    except IOError:
        print '%s: %s' % (FAILED_READING_FILE, file_path)
        return False
    if extension == '.yaml':
        config_file_object = yaml.load(config_file)
    elif extension == '.json':
        config_file_object = json.loads(config_file)
    else:
        config_file_object = config_file
    try:
        validator.validate(config_file_object)
    except ValidationError as e:
        print '%s: %s' % (SCHEMA_INVALID, file_path)
        print '  Validation Message: %s' % e.message
        return False
    else:
        print '%s: %s' % (SCHEMA_VALID, basename)
        return True


def validate_all_schemas(service_path):
    """Finds all recognized config files in service directory,
    and validates their schema.

    :param service_path: path to location of configuration files
    """

    path = os.path.join(service_path, '*.yaml')

    returncode = True
    for file_name in glob(path):
        if os.path.islink(file_name):
            continue
        basename = os.path.basename(file_name)
        for file_type in ['chronos', 'marathon']:
            if basename.startswith(file_type):
                if not validate_schema(file_name, file_type):
                    returncode = False
    return returncode


def add_subparser(subparsers):
    validate_parser = subparsers.add_parser(
        'validate',
        description="Execute 'paasta validate' from service repo root",
        help="Validate that all paasta config files in pwd are correct")
    validate_parser.add_argument(
        '-s', '--service',
        required=False,
        help="Service that you want to validate. Like 'example_service'.",
    ).completer = lazy_choices_completer(list_services)
    validate_parser.add_argument(
        '-y', '--yelpsoa-config-root',
        dest='yelpsoa_config_root',
        default=os.getcwd(),
        required=False,
        help="Path to root of yelpsoa-configs checkout",
    )
    validate_parser.set_defaults(command=paasta_validate)


def check_service_path(service_path):
    """Check that the specified path exists and has yaml files

    :param service_path: Path to directory that should contain yaml files
    """
    if not service_path or not os.path.isdir(service_path):
        print failure("%s is not a directory" % service_path,
                      "http://paasta.readthedocs.org/en/latest/yelpsoa_configs.html")
        return False
    if not glob(os.path.join(service_path, "*.yaml")):
        print failure("%s does not contain any .yaml files" % service_path,
                      "http://paasta.readthedocs.org/en/latest/yelpsoa_configs.html")
        return False
    return True


def get_service_path(service, soa_dir):
    """Determine the path of the directory containing the conf files

    :param service: Name of service
    :param soa_dir: Directory containing soa configs for all services
    """
    if service:
        service_path = os.path.join(soa_dir, service)
    else:
        if soa_dir == os.getcwd():
            service_path = os.getcwd()
        else:
            print UNKNOWN_SERVICE
            return None
    return service_path


def path_to_soa_dir_service(service_path):
    """Split a service_path into its soa_dir and service name components"""
    soa_dir = os.path.dirname(service_path)
    service = os.path.basename(service_path)
    return soa_dir, service


def validate_chronos(service_path):
    """Check that any chronos configurations are valid"""
    soa_dir, service = path_to_soa_dir_service(service_path)
    instance_type = 'chronos'

    returncode = True
    for cluster in list_clusters(service, soa_dir, instance_type):
        for instance in list_all_instances_for_service(
                service=service, clusters=[cluster], instance_type=instance_type,
                soa_dir=soa_dir):
            cjc = load_chronos_job_config(service, instance, cluster, False, soa_dir)
            checks_passed, check_msgs = cjc.validate()

            # Remove duplicate check_msgs
            unique_check_msgs = list(set(check_msgs))

            if not checks_passed:
                print invalid_chronos_instance(cluster, instance, "\n  ".join(unique_check_msgs))
                returncode = False
            else:
                print valid_chronos_instance(cluster, instance)
    return returncode


def paasta_validate_soa_configs(service_path):
    """Analyze the service in serivce_path to determine if the conf files are valid

    :param service_path: Path to directory containing soa conf yaml files for service
    """
    if not check_service_path(service_path):
        return False

    returncode = True

    if not validate_all_schemas(service_path):
        returncode = False

    if not validate_chronos(service_path):
        returncode = False

    return returncode


def paasta_validate(args):
    """Generate a service_path from the provided args and call paasta_validate_soa_configs

    :param args: argparse.Namespace obj created from sys.args by cli
    """
    service = args.service
    soa_dir = args.yelpsoa_config_root
    service_path = get_service_path(service, soa_dir)

    if not paasta_validate_soa_configs(service_path):
        sys.exit(1)
