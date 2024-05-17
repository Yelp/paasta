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
import functools
import json
import os
import pkgutil
import re
from collections import Counter
from datetime import datetime
from functools import lru_cache
from functools import partial
from glob import glob
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

import pytz
import yaml
from croniter import croniter
from jsonschema import Draft4Validator
from jsonschema import exceptions
from jsonschema import FormatChecker
from jsonschema import RefResolver
from jsonschema import ValidationError
from mypy_extensions import TypedDict
from ruamel.yaml import SafeConstructor
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from paasta_tools.autoscaling.utils import MetricsProviderDict
from paasta_tools.cli.utils import failure
from paasta_tools.cli.utils import get_file_contents
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import guess_service_name
from paasta_tools.cli.utils import info_message
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.cli.utils import success
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.long_running_service_tools import DEFAULT_AUTOSCALING_SETPOINT
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_ACTIVE_REQUESTS
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_CPU
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_GUNICORN
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_PISCINA
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_PROMQL
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_UWSGI
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.secret_tools import is_shared_secret
from paasta_tools.tron_tools import DEFAULT_TZ
from paasta_tools.tron_tools import list_tron_clusters
from paasta_tools.tron_tools import load_tron_service_config
from paasta_tools.tron_tools import TronJobConfig
from paasta_tools.tron_tools import validate_complete_config
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InstanceConfigDict
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config


class SoaValidationError(Exception):
    pass


class AutoscalingValidationError(SoaValidationError):
    pass


SCHEMA_VALID = success("Successfully validated schema")

SCHEMA_ERROR = failure(
    "Failed to load schema.",
    "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
)

SCHEMA_INVALID = failure(
    "Failed to validate schema. More info:",
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

SCHEMA_TYPES = {
    "service",  # service metadata
    "adhoc",
    "kubernetes",  # long-running services
    "rollback",  # automatic rollbacks during deployments
    "tron",  # batch workloads
    "eks",  # eks workloads
    "autotuned_defaults/kubernetes",
    "autotuned_defaults/cassandracluster",
}
# we expect a comment that looks like # override-cpu-setting PROJ-1234
# but we don't have a $ anchor in case users want to add an additional
# comment
OVERRIDE_CPU_AUTOTUNE_ACK_PATTERN = r"#\s*override-cpu-setting\s+\(.+[A-Z]+-[0-9]+.+\)"

# we expect a comment that looks like # override-cpu-burst PROJ-1234
# but we don't have a $ anchor in case users want to add an additional
# comment
OVERRIDE_CPU_BURST_ACK_PATTERN = r"#\s*override-cpu-burst\s+\(.+[A-Z]+-[0-9]+.+\)"
# for now, double the autotune cap to give people the benefit of the doubt
# if we see that people are still misusing this configuration, we can lower
# this to the autotune cap (i.e., 1)
CPU_BURST_THRESHOLD = 2

K8S_TYPES = {"eks", "kubernetes"}

INVALID_AUTOSCALING_FIELDS = {
    # setpoint isn't included here because we need to confirm that setpoint = 0.8
    # (since it's auto-added at parse-time)
    METRICS_PROVIDER_ACTIVE_REQUESTS: {"prometheus-adapter-config"},
    METRICS_PROVIDER_CPU: {
        "desired_active_requests_per_replica",
        "prometheus-adapter-config",
    },
    METRICS_PROVIDER_GUNICORN: {
        "desired_active_requests_per_replica",
        "prometheus-adapter-config",
    },
    METRICS_PROVIDER_PISCINA: {
        "desired_active_requests_per_replica",
        "prometheus-adapter-config",
    },
    METRICS_PROVIDER_UWSGI: {
        "desired_active_requests_per_replica",
        "prometheus-adapter-config",
    },
    METRICS_PROVIDER_PROMQL: {"desired_active_requests_per_replica"},
}


class ConditionConfig(TypedDict, total=False):
    """
    Common config options for all Conditions
    """

    # for now, this is the only key required by the schema
    query: str
    # and only one of these needs to be present (enforced in code, not schema)
    upper_bound: Optional[Union[int, float]]
    lower_bound: Optional[Union[int, float]]

    # truly optional
    dry_run: bool


@functools.lru_cache()
def load_all_instance_configs_for_service(
    service: str, cluster: str, soa_dir: str
) -> Tuple[Tuple[str, InstanceConfig], ...]:
    ret = []
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
        ret.append((instance, instance_config))

    return tuple(ret)


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


def get_schema_validator(file_type: str) -> Draft4Validator:
    """Get the correct schema to use for validation

    :param file_type: what schema type should we validate against
    """
    schema_path = f"schemas/{file_type}_schema.json"
    autoscaling_path = "schemas/autoscaling_schema.json"
    schema = pkgutil.get_data("paasta_tools.cli", schema_path).decode()
    autoscaling_ref = pkgutil.get_data("paasta_tools.cli", autoscaling_path).decode()

    # This bit of code loads the base schemas and any relevant "referenced" schemas
    # into a shared "store" -- so that you can reference the shared schema without
    # having to find the exact right path on disk in your schema file.  If you want
    # to reference one schema from another, you still have to include a
    # {"$ref": "<schema_id>#field"} section in your JsonSchema
    #
    # (see https://python-jsonschema.readthedocs.io/en/v2.6.0/references/ and this
    # stack overflow answer https://stackoverflow.com/a/65150457 for details)
    #
    # Also note that this functionality has changed significantly in modern versions
    # of python-jsonschema, so if we ever update we'll need to do some work here.
    base_schema = json.loads(schema)
    autoscaling_schema = json.loads(autoscaling_ref)
    store = {
        "base": base_schema,
        autoscaling_schema["$id"]: json.loads(autoscaling_ref),
    }

    resolver = RefResolver.from_schema(base_schema, store=store)
    return Draft4Validator(
        json.loads(schema),
        resolver=resolver,
        format_checker=FormatChecker(),
    )


def validate_rollback_bounds(
    config: Dict[str, List[ConditionConfig]], file_loc: str
) -> bool:
    """
    Ensure that at least one of upper_bound or lower_bound is set (and set to non-null values)
    """
    errors = []

    for source, queries in config.items():
        for query in queries:
            if not any(
                (
                    query.get("lower_bound"),
                    query.get("upper_bound"),
                ),
            ):
                errors.append(
                    f"{file_loc}:{source}: {query['query']} needs one of lower_bound OR upper_bound set."
                )

    for error in errors:
        print(
            failure(error, link=""),  # TODO: point to actual docs once they exist
        )

    return len(errors) == 0


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
        print(
            failure(
                f"Length of instance name \n{error_string}\n should be no more than 63."
                + " Note _ is replaced with -- due to Kubernetes restriction",
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
    return len(errors) == 0


def validate_service_name(service):
    if len(sanitise_kubernetes_name(service)) > 63:
        print(
            failure(
                f"Length of service name {service} should be no more than 63."
                + " Note _ is replaced with - due to Kubernetes restriction",
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
        return False
    return True


@lru_cache()
def get_config_file_dict(file_path: str, use_ruamel: bool = False) -> Dict[Any, Any]:
    basename = os.path.basename(file_path)
    extension = os.path.splitext(basename)[1]
    try:
        config_file = get_file_contents(file_path)
        if extension == ".yaml":
            if use_ruamel:
                ruamel_loader = YAML(typ="rt")
                # there are templates that define keys that are later overwritten
                # when those templates are actually used (e.g., a template that
                # sets disk: 100 -> an instance uses that template and overwrites
                # it with disk: 1000)
                ruamel_loader.allow_duplicate_keys = True
                # we want to actually expand out all anchors so that we still get
                # comments from the original block
                ruamel_loader.Constructor.flatten_mapping = (
                    SafeConstructor.flatten_mapping
                )
                return ruamel_loader.load(config_file)
            else:
                return yaml.safe_load(config_file)
        elif extension == ".json":
            return json.loads(config_file)
        else:
            return config_file
    except Exception:
        print(f"{FAILED_READING_FILE}: {file_path}")
        raise


def validate_schema(file_path: str, file_type: str) -> bool:
    """Check if the specified config file has a valid schema

    :param file_path: path to file to validate
    :param file_type: what schema type should we validate against
    """
    try:
        validator = get_schema_validator(file_type)
    except Exception as e:
        print(f"{SCHEMA_ERROR}: {file_type}, error: {e!r}")
        return False

    basename = os.path.basename(file_path)
    config_file_object = get_config_file_dict(file_path)
    try:
        validator.validate(config_file_object)
        if file_type in K8S_TYPES and not validate_instance_names(
            config_file_object, file_path
        ):
            return False

        if file_type == "rollback" and not validate_rollback_bounds(
            config_file_object["conditions"],
            file_path,
        ):
            return False

    except ValidationError:
        print(f"{SCHEMA_INVALID}: {file_path}")

        errors = validator.iter_errors(config_file_object)
        print("  Validation Message: %s" % exceptions.best_match(errors).message)
        return False
    except Exception as e:
        print(f"{SCHEMA_ERROR}: {file_type}, error: {e!r}")
        return False
    else:
        print(f"{SCHEMA_VALID}: {basename}")
        return True


def validate_all_schemas(service_path: str) -> bool:
    """Finds all recognized config files in service directory,
    and validates their schema.

    :param service_path: path to location of configuration files
    """

    path = os.path.join(service_path, "**/*.yaml")

    returncode = True
    for file_name in glob(path, recursive=True):
        if os.path.islink(file_name):
            continue

        filename_without_service_path = os.path.relpath(file_name, start=service_path)
        for file_type in SCHEMA_TYPES:
            if filename_without_service_path.startswith(file_type):
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
        "-v",
        "--verbose",
        action="store_true",
        required=False,
        help="Toggle to display additional validation messages for humans.",
    )
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
        print(
            failure(
                "%s is not a directory" % service_path,
                "http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html",
            )
        )
        return False
    if not glob(os.path.join(service_path, "*.yaml")):
        print(
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
            print(UNKNOWN_SERVICE)
            return None
    return service_path


def path_to_soa_dir_service(service_path):
    """Split a service_path into its soa_dir and service name components"""
    soa_dir = os.path.dirname(service_path)
    service = os.path.basename(service_path)
    return soa_dir, service


def validate_tron(service_path: str, verbose: bool = False) -> bool:
    soa_dir, service = path_to_soa_dir_service(service_path)
    returncode = True

    for cluster in list_tron_clusters(service, soa_dir):
        if not validate_tron_namespace(service, cluster, soa_dir):
            returncode = False
        elif verbose:
            # service config has been validated and cron schedules should be safe to parse

            # TODO(TRON-1761): unify tron/paasta validate cron syntax validation
            service_config = load_tron_service_config(
                service=service, cluster=cluster, soa_dir=soa_dir
            )
            for config in service_config:
                cron_expression = config.get_cron_expression()
                if cron_expression:
                    print_upcoming_runs(config, cron_expression)

    return returncode


def print_upcoming_runs(config: TronJobConfig, cron_expression: str) -> None:
    print(info_message(f"Upcoming runs for {config.get_name()}:"))

    config_tz = config.get_time_zone() or DEFAULT_TZ

    next_cron_runs = list_upcoming_runs(
        cron_schedule=cron_expression,
        starting_from=pytz.timezone(config_tz).localize(datetime.today()),
    )

    for run in next_cron_runs:
        print(f"\t{run}")


def validate_tron_namespace(service, cluster, soa_dir, tron_dir=False):
    if tron_dir:
        display_name = f"{cluster}/{service}.yaml"
    else:
        display_name = f"tron-{cluster}.yaml"

    messages = validate_complete_config(service, cluster, soa_dir)
    returncode = len(messages) == 0

    if messages:
        print(invalid_tron_namespace(cluster, "\n  ".join(messages), display_name))
    else:
        print(valid_tron_namespace(cluster, display_name))

    return returncode


def validate_paasta_objects(service_path):
    soa_dir, service = path_to_soa_dir_service(service_path)

    returncode = True
    messages = []
    for cluster in list_clusters(service, soa_dir):
        for instance, instance_config in load_all_instance_configs_for_service(
            service=service, cluster=cluster, soa_dir=soa_dir
        ):
            messages.extend(instance_config.validate())
    returncode = len(messages) == 0

    if messages:
        errors = "\n".join(messages)
        print(failure((f"There were failures validating {service}: {errors}"), ""))
    else:
        print(success(f"All PaaSTA Instances for are valid for all clusters"))

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
            print(
                duplicate_instance_names_message(
                    service, cluster, duplicate_instance_names
                )
            )
        else:
            print(no_duplicate_instance_names_message(service, cluster))

    return check_passed


def _get_comments_for_key(data: CommentedMap, key: Any) -> Optional[str]:
    # this is a little weird, but ruamel is returning a list that looks like:
    # [None, None, CommentToken(...), None] for some reason instead of just a
    # single string
    # Sometimes ruamel returns a recursive list of CommentTokens as well that looks like
    # [None, None, [CommentToken(...),CommentToken(...),None], CommentToken(...), None]
    def _flatten_comments(comments):
        for comment in comments:
            if comment is None:
                continue
            if isinstance(comment, list):
                yield from _flatten_comments(comment)
            else:
                yield comment.value

    raw_comments = [*_flatten_comments(data.ca.items.get(key, []))]
    if not raw_comments:
        # return None so that we don't return an empty string below if there really aren't
        # any comments
        return None
    # joining all comments together before returning them
    comment = "".join(raw_comments)
    return comment


def __is_templated(service: str, soa_dir: str, cluster: str, workload: str) -> bool:
    return os.path.exists(
        os.path.join(os.path.abspath(soa_dir), service, f"{workload}-{cluster}.in")
    )


def _validate_active_requests_autoscaling_configs(
    instance_config: LongRunningServiceConfig,
    metrics_provider_config: MetricsProviderDict,
) -> None:
    if len(instance_config.get_registrations()) > 1:
        raise AutoscalingValidationError(
            "active-requests metrics provider doesn't support instances with multiple registrations"
        )
    # This is a bit incorrect, since the default autoscaling setpoint is 0.8,
    # someone could theoretically bypass this by setting setpoint: 0.8 in their
    # soaconfigs; but I think it's approximately fine for now
    if (
        metrics_provider_config.get("setpoint", DEFAULT_AUTOSCALING_SETPOINT)
        != DEFAULT_AUTOSCALING_SETPOINT
    ):
        raise AutoscalingValidationError(
            "setpoint is not supported for active-requests; use desired_active_requests_per_replica instead"
        )


def _validate_arbitrary_promql_autoscaling_configs(
    metrics_provider_config: MetricsProviderDict,
) -> None:
    # This is a bit incorrect, since the default autoscaling setpoint is 0.8,
    # someone could theoretically bypass this by setting setpoint: 0.8 in their
    # soaconfigs; but I think it's approximately fine for now
    if (
        metrics_provider_config.get("setpoint", DEFAULT_AUTOSCALING_SETPOINT)
        != DEFAULT_AUTOSCALING_SETPOINT
    ):
        raise AutoscalingValidationError(
            "setpoint is not supported for arbitrary PromQL"
        )
    if not metrics_provider_config.get("prometheus_adapter_config"):
        raise AutoscalingValidationError(
            "arbitrary promql metrics provider requires prometheus_adapter_config to be set"
        )


def _validate_autoscaling_config(metrics_provider_config: MetricsProviderDict) -> None:
    metrics_provider_type = metrics_provider_config["type"]
    for field in INVALID_AUTOSCALING_FIELDS[metrics_provider_type]:
        if field in metrics_provider_config:
            raise AutoscalingValidationError(
                f"metric provider {metrics_provider_type} does not support {field}"
            )


def validate_autoscaling_configs(service_path: str) -> bool:
    """Validate new autoscaling configurations that are not validated by jsonschema for the service of interest.

    :param service_path: Path to directory containing soa conf yaml files for service
    """
    soa_dir, service = path_to_soa_dir_service(service_path)
    returncode = True
    link = ""
    skip_cpu_override_validation_list = (
        load_system_paasta_config().get_skip_cpu_override_validation_services()
    )

    for cluster in list_clusters(service, soa_dir):
        for instance, instance_config in load_all_instance_configs_for_service(
            service=service, cluster=cluster, soa_dir=soa_dir
        ):
            if instance_config.get_instance_type() not in K8S_TYPES:
                continue

            instance_config = cast(LongRunningServiceConfig, instance_config)
            if (
                # instance_config is an `InstanceConfig` object, which doesn't have an `is_autoscaling_enabled()`
                # method, but by asserting that the type is in K8S_TYPES, we know we're dealing with either
                # a KubernetesDeploymentConfig or an EksDeploymentConfig, so the cast is safe.
                instance_config.is_autoscaling_enabled()
                # we should eventually make the python templates add the override comment
                # to the correspoding YAML line, but until then we just opt these out of that validation
                and __is_templated(
                    service,
                    soa_dir,
                    cluster,
                    workload=instance_config.get_instance_type(),
                )
                is False
            ):
                autoscaling_params = instance_config.get_autoscaling_params()
                should_skip_cpu_override_validation = (
                    service in skip_cpu_override_validation_list
                )
                seen_provider_types: Set[str] = set()
                configured_provider_count = len(autoscaling_params["metrics_providers"])

                for metrics_provider in autoscaling_params["metrics_providers"]:
                    try:
                        # Generic validation of the config
                        _validate_autoscaling_config(metrics_provider)

                        # Multi-metrics specific validation:
                        # 1. Bespoke policies cannot use multi-metrics scaling
                        # 2. Can't set the same metrics provider multiple times
                        if (
                            metrics_provider.get("decision_policy") == "bespoke"
                            and configured_provider_count > 1
                        ):
                            raise AutoscalingValidationError(
                                f"cannot use bespoke autoscaling with HPA autoscaling"
                            )
                        if metrics_provider["type"] in seen_provider_types:
                            raise AutoscalingValidationError(
                                f"cannot set the same metrics provider multiple times: {metrics_provider['type']}"
                            )
                        seen_provider_types.add(metrics_provider["type"])

                        # Metrics-provider specific validations
                        if metrics_provider["type"] == METRICS_PROVIDER_ACTIVE_REQUESTS:
                            _validate_active_requests_autoscaling_configs(
                                instance_config, metrics_provider
                            )

                        elif metrics_provider["type"] == METRICS_PROVIDER_PROMQL:
                            _validate_arbitrary_promql_autoscaling_configs(
                                metrics_provider
                            )

                        elif (
                            metrics_provider["type"] == METRICS_PROVIDER_CPU
                            # to enable kew autoscaling we just set a decision policy of "bespoke", but
                            # the metrics_provider is (confusingly) left as "cpu"
                            and metrics_provider.get("decision_policy") != "bespoke"
                            and not should_skip_cpu_override_validation
                        ):
                            # Do some extra validation below: we don't abstract that into the above function
                            # call because it needs a lot of extra information

                            # we need access to the comments, so we need to read the config with ruamel to be able
                            # to actually get them in a "nice" automated fashion
                            config = get_config_file_dict(
                                os.path.join(
                                    soa_dir,
                                    service,
                                    f"{instance_config.get_instance_type()}-{cluster}.yaml",
                                ),
                                use_ruamel=True,
                            )
                            if config[instance].get("cpus") is None:
                                # If we're using multiple scaling metrics and one of them is CPU, we must
                                # opt out of CPU autotuning
                                if configured_provider_count > 1:
                                    link = "y/override-cpu-autotune"
                                    raise AutoscalingValidationError(
                                        "using CPU-based scaling with multiple scaling metrics requires explicit "
                                        "'cpus' setting; see the following link for more info:"
                                    )
                                # cpu autoscaled, but using autotuned values - can skip
                                continue

                            cpu_comment = _get_comments_for_key(
                                data=config[instance], key="cpus"
                            )
                            # we could probably have a separate error message if there's a comment that doesn't match
                            # the ack pattern, but that seems like overkill - especially for something that could cause
                            # a DAR if people aren't being careful.
                            if (
                                cpu_comment is None
                                or re.search(
                                    pattern=OVERRIDE_CPU_AUTOTUNE_ACK_PATTERN,
                                    string=cpu_comment,
                                )
                                is None
                            ):
                                link = "y/override-cpu-autotune"
                                raise AutoscalingValidationError(
                                    f"CPU override detected for a CPU-autoscaled instance; "
                                    "see the following link for next steps:"
                                )
                    except AutoscalingValidationError as e:
                        returncode = False
                        print(
                            failure(
                                msg=f"Autoscaling validation failed for {service}.{instance} in {cluster}: {str(e)}",
                                link=link,
                            )
                        )

    return returncode


def validate_min_max_instances(service_path):
    soa_dir, service = path_to_soa_dir_service(service_path)
    returncode = True

    for cluster in list_clusters(service, soa_dir):
        for instance, instance_config in load_all_instance_configs_for_service(
            service=service, cluster=cluster, soa_dir=soa_dir
        ):
            if instance_config.get_instance_type() != "tron":
                min_instances = instance_config.get_min_instances()
                max_instances = instance_config.get_max_instances()
                if min_instances is not None and max_instances is not None:
                    if max_instances < min_instances:
                        returncode = False
                        print(
                            failure(
                                f"Instance {instance} on cluster {cluster} has a greater number of min_instances than max_instances."
                                + f"The number of min_instances ({min_instances}) cannot be greater than the max_instances ({max_instances}).",
                                "",
                            )
                        )

    return returncode


def check_secrets_for_instance(
    instance_config_dict: InstanceConfigDict, soa_dir: str, service: str, vault_env: str
) -> bool:
    return_value = True
    # If the service: directive is used, look for the secret there, rather than where the instance config is defined.
    service_containing_secret = instance_config_dict.get("service", service)
    for env_value in instance_config_dict.get("env", {}).values():
        if is_secret_ref(env_value):
            secret_name = get_secret_name_from_ref(env_value)
            if is_shared_secret(env_value):
                secret_file_name = f"{soa_dir}/_shared/secrets/{secret_name}.json"
            else:
                secret_file_name = (
                    f"{soa_dir}/{service_containing_secret}/secrets/{secret_name}.json"
                )
            if os.path.isfile(secret_file_name):
                secret_json = get_config_file_dict(secret_file_name)
                if "ciphertext" not in secret_json["environments"].get(vault_env, {}):
                    print(
                        failure(
                            f"Secret {secret_name} not defined for ecosystem {vault_env} on secret file {secret_file_name}",
                            "",
                        )
                    )
                    return_value = False
            else:
                print(failure(f"Secret file {secret_file_name} not defined", ""))
                return_value = False
    return return_value


def list_upcoming_runs(
    cron_schedule: str, starting_from: datetime, num_runs: int = 5
) -> List[str]:
    iter = croniter(cron_schedule, starting_from)
    return [iter.get_next(datetime) for _ in range(num_runs)]


def validate_secrets(service_path):
    soa_dir, service = path_to_soa_dir_service(service_path)
    system_paasta_config = load_system_paasta_config()
    vault_cluster_map = system_paasta_config.get_vault_cluster_config()
    return_value = True
    for cluster in list_clusters(service, soa_dir):
        vault_env = vault_cluster_map.get(cluster)
        if not vault_env:
            print(failure(f"{cluster} not found on vault_cluster_map", ""))
            return_value = False
            continue

        for instance, instance_config in load_all_instance_configs_for_service(
            service=service, cluster=cluster, soa_dir=soa_dir
        ):
            if not check_secrets_for_instance(
                instance_config.config_dict, soa_dir, service, vault_env
            ):
                return_value = False
    if return_value:
        print(success("No orphan secrets found"))
    return return_value


def validate_cpu_burst(service_path: str) -> bool:
    soa_dir, service = path_to_soa_dir_service(service_path)
    skip_cpu_burst_validation_list = (
        load_system_paasta_config().get_skip_cpu_burst_validation_services()
    )

    returncode = True
    for cluster in list_clusters(service, soa_dir):
        if __is_templated(
            service, soa_dir, cluster, workload="kubernetes"
        ) or __is_templated(service, soa_dir, cluster, workload="eks"):
            # we should eventually make the python templates add the override comment
            # to the correspoding YAML line, but until then we just opt these out of that validation
            continue
        for instance, instance_config in load_all_instance_configs_for_service(
            service=service, cluster=cluster, soa_dir=soa_dir
        ):
            is_k8s_service = (
                instance_config.get_instance_type() == "kubernetes"
                or instance_config.get_instance_type() == "eks"
            )
            should_skip_cpu_burst_validation = service in skip_cpu_burst_validation_list
            if is_k8s_service and not should_skip_cpu_burst_validation:
                # we need access to the comments, so we need to read the config with ruamel to be able
                # to actually get them in a "nice" automated fashion
                config = get_config_file_dict(
                    os.path.join(
                        soa_dir,
                        service,
                        f"{instance_config.get_instance_type()}-{cluster}.yaml",
                    ),
                    use_ruamel=True,
                )

                if config[instance].get("cpu_burst_add") is None:
                    # using autotuned values - can skip
                    continue
                if config[instance]["cpu_burst_add"] <= CPU_BURST_THRESHOLD:
                    # under the threshold - can also skip
                    continue

                burst_comment = _get_comments_for_key(
                    data=config[instance], key="cpu_burst_add"
                )
                # we could probably have a separate error message if there's a comment that doesn't match
                # the ack pattern, but that seems like overkill - especially for something that could cause
                # a DAR if people aren't being careful.
                if (
                    burst_comment is None
                    or re.search(
                        pattern=OVERRIDE_CPU_BURST_ACK_PATTERN,
                        string=burst_comment,
                    )
                    is None
                ):
                    returncode = False
                    print(
                        failure(
                            msg=f"Potentially excessive CPU burst (cpu_burst_add: {config[instance]['cpu_burst_add']} "
                            f"higher than current threshold of {CPU_BURST_THRESHOLD} cores) detected in {cluster}: {service}.{instance}."
                            " Please read the following link for next steps:",
                            link="y/high-cpu-burst",
                        )
                    )

    return returncode


def paasta_validate_soa_configs(
    service: str, service_path: str, verbose: bool = False
) -> bool:
    """Analyze the service in service_path to determine if the conf files are valid

    :param service_path: Path to directory containing soa conf yaml files for service
    """
    if not check_service_path(service_path):
        return False

    if not validate_service_name(service):
        return False

    checks: List[Callable[[str], bool]] = [
        validate_all_schemas,
        partial(validate_tron, verbose=verbose),
        validate_paasta_objects,
        validate_unique_instance_names,
        validate_autoscaling_configs,
        validate_secrets,
        validate_min_max_instances,
        validate_cpu_burst,
    ]

    # NOTE: we're explicitly passing a list comprehension to all()
    # instead of a generator expression so that we run all checks
    # no matter what
    return all([check(service_path) for check in checks])


def paasta_validate(args):
    """Generate a service_path from the provided args and call paasta_validate_soa_configs

    :param args: argparse.Namespace obj created from sys.args by cli
    """
    service_path = get_service_path(args.service, args.yelpsoa_config_root)
    service = args.service or guess_service_name()
    if not paasta_validate_soa_configs(service, service_path, args.verbose):
        return 1
