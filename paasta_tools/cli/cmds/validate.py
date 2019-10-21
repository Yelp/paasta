#!/usr/bin/env python
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
import json
import os
import pkgutil
from collections import Counter
from glob import glob

import yaml
from jsonschema import Draft4Validator
from jsonschema import exceptions
from jsonschema import FormatChecker
from jsonschema import ValidationError

from paasta_tools.cli.utils import failure
from paasta_tools.cli.utils import get_file_contents
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.cli.utils import success
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.tron_tools import list_tron_clusters
from paasta_tools.tron_tools import validate_complete_config
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import paasta_print


SCHEMA_VALID = success("Successfully validated schema")

SCHEMA_ERROR = failure(
    "Failed to load schema.",
    "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
)

SCHEMA_INVALID = failure(
    "Failed to validate schema. More info:",
    "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
)

SCHEMA_NOT_FOUND = failure(
    "Failed to find schema to validate against. More info:",
    "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
)

FAILED_READING_FILE = failure(
    "Failed to read file. More info:",
    "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
)

UNKNOWN_SERVICE = (
    "Unable to determine service to validate.\n"
    "Please supply the %s name you wish to "
    "validate with the %s option."
    % (PaastaColors.cyan("SERVICE"), PaastaColors.cyan("-s"))
)


def invalid_tron_namespace(cluster, output, filename):
    return failure(
        "%s is invalid:\n  %s\n  " "More info:" % (filename, output),
        "http://tron.readthedocs.io/en/latest/jobs.html",
    )


def valid_tron_namespace(cluster, filename):
    return success(f"{filename} is valid.")


def duplicate_instance_names_message(service, cluster, instance_names):
    instance_name_list = "\n\t".join(instance_names)
    message = (
        f"Service {service} uses the following duplicate instance names for "
        f"cluster {cluster}:\n\t{instance_name_list}\n"
    )
    return failure(
        message, "https://paasta.readthedocs.io/en/latest/yelpsoa_configs.html"
    )


def no_duplicate_instance_names_message(service, cluster):
    return success(f"All {service}'s instance names in cluster {cluster} are unique")


def get_schema(file_type):
    """Get the correct schema to use for validation

    :param file_type: what schema type should we validate against
    """
    schema_path = "schemas/%s_schema.json" % file_type
    try:
        schema = pkgutil.get_data("paasta_tools.cli", schema_path).decode()
    except IOError:
        return None
    return json.loads(schema)


def validate_instance_names(config_file_object, file_path):
    errors = []
    for instance_name in config_file_object:
        if (
            not instance_name.startswith("_")
            and len(sanitise_kubernetes_name(instance_name)) > 63
        ):
            errors.append(instance_name)
    if errors:
        error_string = "\n".join(errors)
        paasta_print(
            failure(
                f"Length of instance name \n{error_string}\n should be no more than 63."
                + " Note _ is replaced with -- due to Kubernetes restriction",
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
    return len(errors) == 0


def validate_service_name(service):
    if len(sanitise_kubernetes_name(service)) > 63:
        paasta_print(
            failure(
                f"Length of service name {service} should be no more than 63."
                + " Note _ is replaced with - due to Kubernetes restriction",
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
        return False
    return True


def validate_schema(file_path, file_type):
    """Check if the specified config file has a valid schema

    :param file_path: path to file to validate
    :param file_type: what schema type should we validate against
    """
    try:
        schema = get_schema(file_type)
    except Exception as e:
        paasta_print(f"{SCHEMA_ERROR}: {file_type}, error: {e!r}")
        return

    if schema is None:
        paasta_print(f"{SCHEMA_NOT_FOUND}: {file_path}")
        return
    validator = Draft4Validator(schema, format_checker=FormatChecker())
    basename = os.path.basename(file_path)
    extension = os.path.splitext(basename)[1]
    try:
        config_file = get_file_contents(file_path)
        if extension == ".yaml":
            config_file_object = yaml.safe_load(config_file)
        elif extension == ".json":
            config_file_object = json.loads(config_file)
        else:
            config_file_object = config_file
    except Exception:
        paasta_print(f"{FAILED_READING_FILE}: {file_path}")
        raise
    try:
        validator.validate(config_file_object)
        if file_type == "kubernetes" and not validate_instance_names(
            config_file_object, file_path
        ):
            return
    except ValidationError:
        paasta_print(f"{SCHEMA_INVALID}: {file_path}")

        errors = validator.iter_errors(config_file_object)
        paasta_print("  Validation Message: %s" % exceptions.best_match(errors).message)
    except Exception as e:
        paasta_print(f"{SCHEMA_ERROR}: {file_type}, error: {e!r}")
        return
    else:
        paasta_print(f"{SCHEMA_VALID}: {basename}")
        return True


def validate_all_schemas(service_path):
    """Finds all recognized config files in service directory,
    and validates their schema.

    :param service_path: path to location of configuration files
    """

    path = os.path.join(service_path, "*.yaml")

    returncode = True
    for file_name in glob(path):
        if os.path.islink(file_name):
            continue
        basename = os.path.basename(file_name)
        for file_type in ["marathon", "adhoc", "tron", "kubernetes"]:
            if basename.startswith(file_type):
                if not validate_schema(file_name, file_type):
                    returncode = False
    return returncode


def add_subparser(subparsers):
    validate_parser = subparsers.add_parser(
        "validate",
        description="Execute 'paasta validate' from service repo root",
        help="Validate that all paasta config files in pwd are correct",
    )
    validate_parser.add_argument(
        "-s",
        "--service",
        required=False,
        help="Service that you want to validate. Like 'example_service'.",
    ).completer = lazy_choices_completer(list_services)
    validate_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
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
        paasta_print(
            failure(
                "%s is not a directory" % service_path,
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
        return False
    if not glob(os.path.join(service_path, "*.yaml")):
        paasta_print(
            failure(
                "%s does not contain any .yaml files" % service_path,
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
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
            paasta_print(UNKNOWN_SERVICE)
            return None
    return service_path


def path_to_soa_dir_service(service_path):
    """Split a service_path into its soa_dir and service name components"""
    soa_dir = os.path.dirname(service_path)
    service = os.path.basename(service_path)
    return soa_dir, service


def validate_tron(service_path):
    soa_dir, service = path_to_soa_dir_service(service_path)
    returncode = True

    if soa_dir.endswith("/tron"):
        # Makes it possible to validate files in tron/ rather than service directories
        # TODO: Clean up after migration to services is complete
        cluster = service
        soa_dir = soa_dir[:-5]
        filenames = [
            filename
            for filename in os.listdir(service_path)
            if filename.endswith(".yaml")
        ]
        for filename in filenames:
            namespace = os.path.splitext(filename)[0]
            file_path = os.path.join(service_path, filename)
            if not validate_schema(file_path, "tron"):
                returncode = False
            if not validate_tron_namespace(namespace, cluster, soa_dir, tron_dir=True):
                returncode = False
    else:
        # Normal service directory
        for cluster in list_tron_clusters(service, soa_dir):
            if not validate_tron_namespace(service, cluster, soa_dir):
                returncode = False

    return returncode


def validate_tron_namespace(service, cluster, soa_dir, tron_dir=False):
    if tron_dir:
        display_name = f"{cluster}/{service}.yaml"
    else:
        display_name = f"tron-{cluster}.yaml"

    messages = validate_complete_config(service, cluster, soa_dir)
    returncode = len(messages) == 0

    if messages:
        paasta_print(
            invalid_tron_namespace(cluster, "\n  ".join(messages), display_name)
        )
    else:
        paasta_print(valid_tron_namespace(cluster, display_name))

    return returncode


def validate_paasta_objects(service_path):
    soa_dir, service = path_to_soa_dir_service(service_path)

    returncode = True
    messages = []
    for cluster in list_clusters(service, soa_dir):
        for instance in list_all_instances_for_service(
            service=service, clusters=[cluster], soa_dir=soa_dir
        ):
            instance_config = get_instance_config(
                service=service,
                instance=instance,
                cluster=cluster,
                load_deployments=False,
                soa_dir=soa_dir,
            )
            messages.extend(instance_config.validate())
    returncode = len(messages) == 0

    if messages:
        errors = "\n".join(messages)
        paasta_print(
            failure((f"There were failures validating {service}: {errors}"), "")
        )
    else:
        paasta_print(success(f"All PaaSTA Instances for are valid for all clusters"))

    return returncode


def validate_unique_instance_names(service_path):
    """Check that the service does not use the same instance name more than once"""
    soa_dir, service = path_to_soa_dir_service(service_path)
    check_passed = True

    for cluster in list_clusters(service, soa_dir):
        service_instances = get_service_instance_list(
            service=service, cluster=cluster, soa_dir=soa_dir
        )
        instance_names = [service_instance[1] for service_instance in service_instances]
        instance_name_to_count = Counter(instance_names)
        duplicate_instance_names = [
            instance_name
            for instance_name, count in instance_name_to_count.items()
            if count > 1
        ]
        if duplicate_instance_names:
            check_passed = False
            paasta_print(
                duplicate_instance_names_message(
                    service, cluster, duplicate_instance_names
                )
            )
        else:
            paasta_print(no_duplicate_instance_names_message(service, cluster))

    return check_passed


def paasta_validate_soa_configs(service, service_path):
    """Analyze the service in service_path to determine if the conf files are valid

    :param service_path: Path to directory containing soa conf yaml files for service
    """
    if not check_service_path(service_path):
        return False

    if not validate_service_name(service):
        return False

    returncode = True

    if not validate_all_schemas(service_path):
        returncode = False

    if not validate_tron(service_path):
        returncode = False

    if not validate_paasta_objects(service_path):
        returncode = False

    if not validate_unique_instance_names(service_path):
        returncode = False

    return returncode


def paasta_validate(args):
    """Generate a service_path from the provided args and call paasta_validate_soa_configs

    :param args: argparse.Namespace obj created from sys.args by cli
    """
    service = args.service
    soa_dir = args.yelpsoa_config_root
    service_path = get_service_path(service, soa_dir)
    if not paasta_validate_soa_configs(service, service_path):
        return 1
