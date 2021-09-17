# Copyright 2015-2018 Yelp Inc.
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
import difflib
import glob
import hashlib
import json
import logging
import os
import pkgutil
import re
import subprocess
import traceback
from string import Formatter
from typing import List
from typing import Mapping
from typing import Tuple
from typing import Union

import yaml
from service_configuration_lib import read_extra_service_information
from service_configuration_lib import read_yaml_file
from service_configuration_lib.spark_config import generate_clusterman_metrics_entries
from service_configuration_lib.spark_config import get_aws_credentials
from service_configuration_lib.spark_config import get_resources_requested
from service_configuration_lib.spark_config import get_spark_conf
from service_configuration_lib.spark_config import stringify_spark_env

from paasta_tools.mesos_tools import mesos_services_running_here

try:
    from yaml.cyaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Dumper = yaml.SafeDumper  # type: ignore

from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.tron.client import TronClient
from paasta_tools.tron import tron_command_context
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DockerParameter
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import time_cache
from paasta_tools.utils import filter_templates_from_config
from paasta_tools.kubernetes_tools import (
    allowlist_denylist_to_requirements,
    limit_size_with_hash,
    raw_selectors_to_requirements,
    sanitise_kubernetes_name,
    to_node_label,
)
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.secret_tools import is_shared_secret
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.spark_tools import get_webui_url
from paasta_tools.spark_tools import inject_spark_conf_str

from paasta_tools import monitoring_tools
from paasta_tools.monitoring_tools import list_teams
from typing import Optional
from typing import Dict
from typing import Any

log = logging.getLogger(__name__)
logging.getLogger("tron").setLevel(logging.WARNING)

MASTER_NAMESPACE = "MASTER"
SPACER = "."
VALID_MONITORING_KEYS = set(
    json.loads(
        pkgutil.get_data("paasta_tools.cli", "schemas/tron_schema.json").decode()
    )["definitions"]["job"]["properties"]["monitoring"]["properties"].keys()
)
MESOS_EXECUTOR_NAMES = ("paasta", "spark")
KUBERNETES_EXECUTOR_NAMES = ("paasta",)
DEFAULT_AWS_REGION = "us-west-2"
clusterman_metrics, _ = get_clusterman_metrics()


class TronNotConfigured(Exception):
    pass


class InvalidTronConfig(Exception):
    pass


class TronConfig(dict):
    """System-level configuration for Tron."""

    def __init__(self, config):
        super().__init__(config)

    def get_cluster_name(self):
        """:returns The name of the Tron cluster"""
        try:
            return self["cluster_name"]
        except KeyError:
            raise TronNotConfigured(
                "Could not find name of Tron cluster in system Tron config"
            )

    def get_url(self):
        """:returns The URL for the Tron master's API"""
        try:
            return self["url"]
        except KeyError:
            raise TronNotConfigured(
                "Could not find URL of Tron master in system Tron config"
            )


def get_tronfig_folder(cluster, soa_dir):
    return os.path.join(soa_dir, "tron", cluster)


def load_tron_config():
    return TronConfig(load_system_paasta_config().get_tron_config())


def get_tron_client():
    return TronClient(load_tron_config().get_url())


def compose_instance(job, action):
    return f"{job}{SPACER}{action}"


def decompose_instance(instance):
    """Get (job_name, action_name) from an instance."""
    decomposed = instance.split(SPACER)
    if len(decomposed) != 2:
        raise InvalidInstanceConfig("Invalid instance name: %s" % instance)
    return (decomposed[0], decomposed[1])


def decompose_executor_id(executor_id) -> Tuple[str, str, int, str]:
    """(service, job, run_number, action)"""
    service, job, str_run_number, action, _ = executor_id.split(SPACER)
    return (service, job, int(str_run_number), action)


class StringFormatter(Formatter):
    def __init__(self, context=None):
        Formatter.__init__(self)
        self.context = context

    def get_value(self, key, args, kwds):
        if isinstance(key, str):
            try:
                return kwds[key]
            except KeyError:
                return self.context[key]
            else:
                return Formatter.get_value(key, args, kwds)


def parse_time_variables(command: str, parse_time: datetime.datetime = None) -> str:
    """Parses an input string and uses the Tron-style dateparsing
    to replace time variables. Currently supports only the date/time
    variables listed in the tron documentation:
    http://tron.readthedocs.io/en/latest/command_context.html#built-in-cc

    :param input_string: input string to be parsed
    :param parse_time: Reference Datetime object to parse the date and time strings, defaults to now.
    :returns: A string with the date and time variables replaced
    """
    if parse_time is None:
        parse_time = datetime.datetime.now()
    # We build up a tron context object that has the right
    # methods to parse tron-style time syntax
    job_context = tron_command_context.JobRunContext(
        tron_command_context.CommandContext()
    )
    # The tron context object needs the run_time attribute set so it knows
    # how to interpret the date strings
    job_context.job_run.run_time = parse_time
    return StringFormatter(job_context).format(command)


def pick_spark_ui_port(service, instance):
    # We don't know what ports will be available on the agent that the driver
    # will be scheduled on, so we just try to make them unique per service / instance.
    hash_key = f"{service} {instance}".encode()
    hash_number = int(hashlib.sha1(hash_key).hexdigest(), 16)
    preferred_port = 33000 + (hash_number % 25000)
    return preferred_port


class TronActionConfig(InstanceConfig):
    config_filename_prefix = "tron"

    def __init__(
        self,
        service,
        instance,
        cluster,
        config_dict,
        branch_dict,
        soa_dir=DEFAULT_SOA_DIR,
        for_validation=False,
    ):
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )
        self.job, self.action = decompose_instance(instance)
        # Indicate whether this config object is created for validation
        self.for_validation = for_validation

    def get_spark_config_dict(self):
        spark_config_dict = getattr(self, "_spark_config_dict", None)
        # cached the created dict, so that we don't need to process it multiple
        # times, and having inconsistent result
        if spark_config_dict is not None:
            return spark_config_dict

        if self.get_spark_cluster_manager() == "mesos":
            mesos_leader = (
                f"zk://{load_system_paasta_config().get_zk_hosts()}"
                if not self.for_validation
                else "N/A"
            )
        else:
            mesos_leader = None

        aws_creds = get_aws_credentials(
            aws_credentials_yaml=self.config_dict.get("aws_credentials_yaml")
        )
        self._spark_config_dict = get_spark_conf(
            cluster_manager=self.get_spark_cluster_manager(),
            spark_app_base_name=f"tron_spark_{self.get_service()}_{self.get_instance()}",
            user_spark_opts=self.config_dict.get("spark_args", {}),
            paasta_cluster=self.get_spark_paasta_cluster(),
            paasta_pool=self.get_spark_paasta_pool(),
            paasta_service=self.get_service(),
            paasta_instance=self.get_instance(),
            docker_img=self.get_docker_url(),
            aws_creds=aws_creds,
            extra_volumes=self.get_volumes(load_system_paasta_config().get_volumes()),
            # tron is using environment variable to load the required creds
            with_secret=False,
            mesos_leader=mesos_leader,
            # load_system_paasta already load the default volumes
            load_paasta_default_volumes=False,
        )
        return self._spark_config_dict

    def get_job_name(self):
        return self.job

    def get_action_name(self):
        return self.action

    def get_deploy_group(self) -> Optional[str]:
        return self.config_dict.get("deploy_group", None)

    def get_docker_url(
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
    ) -> str:
        # It's okay for tronfig to contain things that aren't deployed yet - it's normal for developers to
        # push tronfig well before the job is scheduled to run, and either they'll deploy the service before
        # or get notified when the job fails.
        #
        # This logic ensures that we can still pass validation and run setup_tron_namespace even if
        # there's nothing in deployments.json yet.
        return (
            ""
            if not self.get_docker_image()
            else super().get_docker_url(system_paasta_config=system_paasta_config)
        )

    def get_cmd(self):
        command = self.config_dict.get("command")
        if self.get_executor() == "spark":
            # Spark expects to be able to write to MESOS_SANDBOX if it is set
            # but the default value (/mnt/mesos/sandbox) doesn't get mounted in
            # our Docker containers, so we unset it here.  (Un-setting is fine,
            # since Spark will just write to /tmp instead).
            command = "unset MESOS_DIRECTORY MESOS_SANDBOX; " + inject_spark_conf_str(
                command, stringify_spark_env(self.get_spark_config_dict())
            )
        return command

    def get_spark_paasta_cluster(self):
        return self.config_dict.get("spark_paasta_cluster", self.get_cluster())

    def get_spark_paasta_pool(self):
        return self.config_dict.get("spark_paasta_pool", "batch")

    def get_spark_cluster_manager(self):
        return self.config_dict.get("spark_cluster_manager", "mesos")

    def get_env(self):
        env = super().get_env()
        if self.get_executor() == "spark":
            spark_config_dict = self.get_spark_config_dict()
            env["EXECUTOR_CLUSTER"] = self.get_spark_paasta_cluster()
            env["EXECUTOR_POOL"] = self.get_spark_paasta_pool()
            env["SPARK_OPTS"] = stringify_spark_env(spark_config_dict)
            # The actual mesos secret will be decrypted and injected on mesos master when assigning
            # tasks.
            env["SPARK_MESOS_SECRET"] = "SHARED_SECRET(SPARK_MESOS_SECRET)"
            if clusterman_metrics:
                env["CLUSTERMAN_RESOURCES"] = json.dumps(
                    generate_clusterman_metrics_entries(
                        clusterman_metrics,
                        get_resources_requested(spark_config_dict),
                        spark_config_dict["spark.app.name"],
                        get_webui_url(spark_config_dict["spark.ui.port"]),
                    )
                )
            else:
                env["CLUSTERMAN_RESOURCES"] = "{}"

            if "AWS_ACCESS_KEY_ID" not in env or "AWS_SECRET_ACCESS_KEY" not in env:
                try:
                    access_key, secret_key, session_token = get_aws_credentials(
                        service=self.get_service(),
                        aws_credentials_yaml=self.config_dict.get(
                            "aws_credentials_yaml"
                        ),
                    )
                    env["AWS_ACCESS_KEY_ID"] = access_key
                    env["AWS_SECRET_ACCESS_KEY"] = secret_key
                except Exception:
                    log.warning(
                        f"Cannot set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment "
                        f"variables for tron action {self.get_instance()} of service "
                        f"{self.get_service()} via credentail file. Traceback:\n"
                        f"{traceback.format_exc()}"
                    )
            if "AWS_DEFAULT_REGION" not in env:
                env["AWS_DEFAULT_REGION"] = DEFAULT_AWS_REGION

        return env

    def get_secret_env(self) -> Mapping[str, dict]:
        base_env = self.config_dict.get("env", {})
        secret_env = {}
        for k, v in base_env.items():
            if is_secret_ref(v):
                secret = get_secret_name_from_ref(v)
                sanitised_secret = sanitise_kubernetes_name(secret)
                service = (
                    self.service if not is_shared_secret(v) else SHARED_SECRET_SERVICE
                )
                sanitised_service = sanitise_kubernetes_name(service)
                secret_env[k] = {
                    "secret_name": f"tron-secret-{sanitised_service}-{sanitised_secret}",
                    "key": secret,
                }
        return secret_env

    def get_cpu_burst_add(self) -> float:
        """ For Tron jobs, we don't let them burst by default, because they
        don't represent "real-time" workloads, and should not impact
        neighbors """
        return self.config_dict.get("cpu_burst_add", 0)

    def get_executor(self):
        return self.config_dict.get("executor", "paasta")

    def get_healthcheck_mode(self, _) -> None:
        return None

    def get_node(self):
        return self.config_dict.get("node")

    def get_retries(self):
        return self.config_dict.get("retries")

    def get_retries_delay(self):
        return self.config_dict.get("retries_delay")

    def get_requires(self):
        return self.config_dict.get("requires")

    def get_expected_runtime(self):
        return self.config_dict.get("expected_runtime")

    def get_triggered_by(self):
        return self.config_dict.get("triggered_by", None)

    def get_trigger_downstreams(self):
        return self.config_dict.get("trigger_downstreams", None)

    def get_on_upstream_rerun(self):
        return self.config_dict.get("on_upstream_rerun", None)

    def get_trigger_timeout(self):
        return self.config_dict.get("trigger_timeout", None)

    def get_use_k8s(self):
        return self.config_dict.get("use_k8s", False)

    def get_node_selectors(self) -> Dict[str, str]:
        raw_selectors: Dict[str, Any] = self.config_dict.get("node_selectors", {})  # type: ignore
        node_selectors = {
            to_node_label(label): value
            for label, value in raw_selectors.items()
            if isinstance(value, str)
        }
        node_selectors["yelp.com/pool"] = self.get_pool()
        return node_selectors

    def get_node_affinities(self) -> Optional[List[Dict[str, Union[str, List[str]]]]]:
        """Converts deploy_whitelist and deploy_blacklist in node affinities.

        note: At the time of writing, `kubectl describe` does not show affinities,
        only selectors. To see affinities, use `kubectl get pod -o json` instead.
        """
        requirements = allowlist_denylist_to_requirements(
            allowlist=self.get_deploy_whitelist(), denylist=self.get_deploy_blacklist(),
        )
        requirements.extend(
            raw_selectors_to_requirements(
                raw_selectors=self.config_dict.get("node_selectors", {}),  # type: ignore
            )
        )
        if not requirements:
            return None

        return [
            {"key": key, "operator": op, "value": value}
            for key, op, value in requirements
        ]

    def get_calculated_constraints(self):
        """Combine all configured Mesos constraints."""
        constraints = self.get_constraints()
        if constraints is not None:
            return constraints
        else:
            constraints = self.get_extra_constraints()
            constraints.extend(
                self.get_deploy_constraints(
                    blacklist=self.get_deploy_blacklist(),
                    whitelist=self.get_deploy_whitelist(),
                    # Don't have configs for the paasta cluster
                    system_deploy_blacklist=[],
                    system_deploy_whitelist=None,
                )
            )
            constraints.extend(self.get_pool_constraints())
            return constraints

    def get_nerve_namespace(self) -> None:
        return None

    def validate(self):
        error_msgs = []
        error_msgs.extend(super().validate())
        # Tron is a little special, because it can *not* have a deploy group
        # But only if an action is running via ssh and not via paasta
        if (
            self.get_deploy_group() is None
            and self.get_executor() in MESOS_EXECUTOR_NAMES
        ):
            error_msgs.append(
                f"{self.get_job_name()}.{self.get_action_name()} must have a deploy_group set"
            )
        return error_msgs

    def format_docker_parameters(
        self,
        with_labels: bool = True,
        system_paasta_config: Optional[SystemPaastaConfig] = None,
    ) -> List[DockerParameter]:
        """Formats extra flags for running docker.  Will be added in the format
        `["--%s=%s" % (e['key'], e['value']) for e in list]` to the `docker run` command
        Note: values must be strings"""
        parameters = super().format_docker_parameters(
            with_labels=with_labels, system_paasta_config=system_paasta_config
        )
        if self.get_executor() == "spark":
            parameters.append({"key": "net", "value": "host"})
        return parameters


class TronJobConfig:
    """Represents a job in Tron, consisting of action(s) and job-level configuration values."""

    def __init__(
        self,
        name: str,
        config_dict: Dict[str, Any],
        cluster: str,
        service: Optional[str] = None,
        load_deployments: bool = True,
        soa_dir: str = DEFAULT_SOA_DIR,
        for_validation: bool = False,
    ) -> None:
        self.name = name
        self.config_dict = config_dict
        self.cluster = cluster
        self.service = service
        self.load_deployments = load_deployments
        self.soa_dir = soa_dir
        # Indicate whether this config object is created for validation
        self.for_validation = for_validation

    def get_use_k8s(self) -> bool:
        return self.config_dict.get("use_k8s", False)

    def get_name(self):
        return self.name

    def get_node(self):
        return self.config_dict.get("node", "paasta")

    def get_schedule(self):
        return self.config_dict.get("schedule")

    def get_monitoring(self):
        srv_monitoring = dict(
            monitoring_tools.read_monitoring_config(self.service, soa_dir=self.soa_dir)
        )
        tron_monitoring = self.config_dict.get("monitoring", {})
        srv_monitoring.update(tron_monitoring)
        # filter out non-tron monitoring keys
        srv_monitoring = {
            k: v for k, v in srv_monitoring.items() if k in VALID_MONITORING_KEYS
        }
        return srv_monitoring

    def get_queueing(self):
        return self.config_dict.get("queueing")

    def get_run_limit(self):
        return self.config_dict.get("run_limit")

    def get_all_nodes(self):
        return self.config_dict.get("all_nodes")

    def get_enabled(self):
        return self.config_dict.get("enabled")

    def get_allow_overlap(self):
        return self.config_dict.get("allow_overlap")

    def get_max_runtime(self):
        return self.config_dict.get("max_runtime")

    def get_time_zone(self):
        return self.config_dict.get("time_zone")

    def get_service(self) -> Optional[str]:
        return self.service or self.config_dict.get("service")

    def get_deploy_group(self) -> Optional[str]:
        return self.config_dict.get("deploy_group", None)

    def get_cluster(self):
        return self.cluster

    def get_expected_runtime(self):
        return self.config_dict.get("expected_runtime")

    def _get_action_config(self, action_name, action_dict):
        action_service = action_dict.setdefault("service", self.get_service())
        action_deploy_group = action_dict.setdefault(
            "deploy_group", self.get_deploy_group()
        )
        if action_service and action_deploy_group and self.load_deployments:
            try:
                deployments_json = load_v2_deployments_json(
                    service=action_service, soa_dir=self.soa_dir
                )
                branch_dict = {
                    "docker_image": deployments_json.get_docker_image_for_deploy_group(
                        action_deploy_group
                    ),
                    "git_sha": deployments_json.get_git_sha_for_deploy_group(
                        action_deploy_group
                    ),
                    # TODO: add Tron instances when generating deployments json
                    "desired_state": "start",
                    "force_bounce": None,
                }
            except NoDeploymentsAvailable:
                log.warning(
                    f'Docker image unavailable for {action_service}.{self.get_name()}.{action_dict.get("name")}'
                    " is it deployed yet?"
                )
                branch_dict = None
        else:
            branch_dict = None
        action_dict["monitoring"] = self.get_monitoring()

        return TronActionConfig(
            service=action_service,
            instance=compose_instance(self.get_name(), action_name),
            cluster=self.get_cluster(),
            config_dict=action_dict,
            branch_dict=branch_dict,
            soa_dir=self.soa_dir,
            for_validation=self.for_validation,
        )

    def get_actions(self):
        actions = self.config_dict.get("actions")
        return [
            self._get_action_config(name, action_dict)
            for name, action_dict in actions.items()
        ]

    def get_cleanup_action(self):
        action_dict = self.config_dict.get("cleanup_action")
        if not action_dict:
            return None

        # TODO: we should keep this trickery outside paasta repo
        return self._get_action_config("cleanup", action_dict)

    def check_monitoring(self) -> Tuple[bool, str]:
        monitoring = self.get_monitoring()
        valid_teams = list_teams()
        if monitoring is not None:
            team_name = monitoring.get("team", None)
            if team_name is None:
                return False, "Team name is required for monitoring"
            elif team_name not in valid_teams:
                suggest_teams = difflib.get_close_matches(
                    word=team_name, possibilities=valid_teams
                )
                return (
                    False,
                    f"Invalid team name: {team_name}. Do you mean one of these: {suggest_teams}",
                )
        return True, ""

    def check_actions(self) -> Tuple[bool, List[str]]:
        actions = self.get_actions()
        cleanup_action = self.get_cleanup_action()
        if cleanup_action:
            actions.append(cleanup_action)

        checks_passed = True
        msgs: List[str] = []
        for action in actions:
            action_msgs = action.validate()
            if action_msgs:
                checks_passed = False
                msgs.extend(action_msgs)
        return checks_passed, msgs

    def validate(self) -> List[str]:
        _, error_msgs = self.check_actions()
        checks = ["check_monitoring"]
        for check in checks:
            check_passed, check_msg = getattr(self, check)()
            if not check_passed:
                error_msgs.append(check_msg)
        return error_msgs

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.config_dict == other.config_dict
        return False


def format_volumes(paasta_volume_list):
    return [
        {
            "container_path": v["containerPath"],
            "host_path": v["hostPath"],
            "mode": v["mode"],
        }
        for v in paasta_volume_list
    ]


def format_master_config(master_config, default_volumes, dockercfg_location):
    mesos_options = master_config.get("mesos_options", {})
    mesos_options.update(
        {
            "default_volumes": format_volumes(default_volumes),
            "dockercfg_location": dockercfg_location,
        }
    )
    master_config["mesos_options"] = mesos_options

    k8s_options = master_config.get("k8s_options", {})
    if k8s_options:
        # Only add default volumes if we already have k8s_options
        k8s_options.update(
            {"default_volumes": format_volumes(default_volumes),}
        )
        master_config["k8s_options"] = k8s_options
    return master_config


def format_tron_action_dict(action_config: TronActionConfig, use_k8s: bool = False):
    """Generate a dict of tronfig for an action, from the TronActionConfig.

    :param job_config: TronActionConfig
    """
    executor = action_config.get_executor()
    result = {
        "command": action_config.get_cmd(),
        "executor": executor,
        "requires": action_config.get_requires(),
        "node": action_config.get_node(),
        "retries": action_config.get_retries(),
        "retries_delay": action_config.get_retries_delay(),
        "expected_runtime": action_config.get_expected_runtime(),
        "trigger_downstreams": action_config.get_trigger_downstreams(),
        "triggered_by": action_config.get_triggered_by(),
        "on_upstream_rerun": action_config.get_on_upstream_rerun(),
        "trigger_timeout": action_config.get_trigger_timeout(),
    }

    # while we're tranisitioning, we want to be able to cleanly fallback to Mesos
    # so we'll default to Mesos unless k8s usage is enabled for both the cluster
    # and job.
    # there are slight differences between k8s and Mesos configs, so we'll translate
    # whatever is in soaconfigs to the k8s equivalent here as well.
    if executor in KUBERNETES_EXECUTOR_NAMES and use_k8s:
        result["executor"] = "kubernetes"

        result["secret_env"] = action_config.get_secret_env()
        all_env = action_config.get_env()
        # For k8s, we do not want secret envvars to be duplicated in both `env` and `secret_env`
        result["env"] = {k: v for k, v in all_env.items() if not is_secret_ref(v)}
        # for Tron-on-K8s, we want to ship tronjob output through logspout
        # such that this output eventually makes it into our per-instance
        # log streams automatically
        result["env"]["ENABLE_PER_INSTANCE_LOGSPOUT"] = "1"
        result["node_selectors"] = action_config.get_node_selectors()
        result["node_affinities"] = action_config.get_node_affinities()

        # XXX: once we're off mesos we can make get_cap_* return just the cap names as a list
        result["cap_add"] = [cap["value"] for cap in action_config.get_cap_add()]
        result["cap_drop"] = [cap["value"] for cap in action_config.get_cap_drop()]

        result["labels"] = {
            "paasta.yelp.com/cluster": action_config.get_cluster(),
            "paasta.yelp.com/pool": action_config.get_pool(),
            "paasta.yelp.com/service": action_config.get_service(),
            "paasta.yelp.com/instance": limit_size_with_hash(
                action_config.get_instance(), limit=63, suffix=4,
            ),
        }

        if action_config.get_team() is not None:
            result["labels"]["yelp.com/owner"] = action_config.get_team()

    elif executor in MESOS_EXECUTOR_NAMES:
        result["executor"] = "mesos"
        constraint_labels = ["attribute", "operator", "value"]
        result["constraints"] = [
            dict(zip(constraint_labels, constraint))
            for constraint in action_config.get_calculated_constraints()
        ]
        result["docker_parameters"] = [
            {"key": param["key"], "value": param["value"]}
            for param in action_config.format_docker_parameters()
        ]
        result["env"] = action_config.get_env()

    # the following config is only valid for k8s/Mesos since we're not running SSH actions
    # in a containerized fashion
    if executor in (KUBERNETES_EXECUTOR_NAMES + MESOS_EXECUTOR_NAMES):
        result["cpus"] = action_config.get_cpus()
        result["mem"] = action_config.get_mem()
        result["disk"] = action_config.get_disk()
        result["extra_volumes"] = format_volumes(action_config.get_extra_volumes())
        result["docker_image"] = action_config.get_docker_url()

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def format_tron_job_dict(job_config: TronJobConfig, k8s_enabled: bool = False):
    """Generate a dict of tronfig for a job, from the TronJobConfig.

    :param job_config: TronJobConfig
    """

    # TODO: this use_k8s flag should be removed once we've fully migrated off of mesos
    use_k8s = job_config.get_use_k8s() and k8s_enabled
    action_dict = {
        action_config.get_action_name(): format_tron_action_dict(
            action_config=action_config, use_k8s=use_k8s
        )
        for action_config in job_config.get_actions()
    }

    result = {
        "node": job_config.get_node(),
        "schedule": job_config.get_schedule(),
        "actions": action_dict,
        "monitoring": job_config.get_monitoring(),
        "queueing": job_config.get_queueing(),
        "run_limit": job_config.get_run_limit(),
        "all_nodes": job_config.get_all_nodes(),
        "enabled": job_config.get_enabled(),
        "allow_overlap": job_config.get_allow_overlap(),
        "max_runtime": job_config.get_max_runtime(),
        "time_zone": job_config.get_time_zone(),
        "expected_runtime": job_config.get_expected_runtime(),
    }
    # TODO: this should be directly inlined, but we need to update tron everywhere first so it'll
    # be slightly less tedious to just conditionally send this now until we clean things up on the
    # removal of all the Mesos code
    if job_config.get_use_k8s():
        result["use_k8s"] = job_config.get_use_k8s()

    cleanup_config = job_config.get_cleanup_action()
    if cleanup_config:
        cleanup_action = format_tron_action_dict(
            action_config=cleanup_config, use_k8s=use_k8s
        )
        result["cleanup_action"] = cleanup_action

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def load_tron_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> TronActionConfig:
    jobs = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir,
    )
    requested_job, requested_action = instance.split(".")
    for job in jobs:
        if job.get_name() == requested_job:
            for action in job.get_actions():
                if action.get_action_name() == requested_action:
                    return action
    raise NoConfigurationForServiceError(
        f"No tron configuration found for {service} {instance}"
    )


@time_cache(ttl=5)
def load_tron_service_config(
    service,
    cluster,
    load_deployments=True,
    soa_dir=DEFAULT_SOA_DIR,
    for_validation=False,
):
    return load_tron_service_config_no_cache(
        service, cluster, load_deployments, soa_dir, for_validation,
    )


def load_tron_service_config_no_cache(
    service,
    cluster,
    load_deployments=True,
    soa_dir=DEFAULT_SOA_DIR,
    for_validation=False,
):
    """Load all configured jobs for a service, and any additional config values."""
    config = read_extra_service_information(
        service_name=service, extra_info=f"tron-{cluster}", soa_dir=soa_dir
    )
    jobs = filter_templates_from_config(config)
    job_configs = [
        TronJobConfig(
            name=name,
            service=service,
            cluster=cluster,
            config_dict=job,
            load_deployments=load_deployments,
            soa_dir=soa_dir,
            for_validation=for_validation,
        )
        for name, job in jobs.items()
    ]
    return job_configs


def create_complete_master_config(cluster, soa_dir=DEFAULT_SOA_DIR):
    system_paasta_config = load_system_paasta_config()
    tronfig_folder = get_tronfig_folder(soa_dir=soa_dir, cluster=cluster)
    config = read_yaml_file(os.path.join(tronfig_folder, f"MASTER.yaml"))
    master_config = format_master_config(
        config,
        system_paasta_config.get_volumes(),
        system_paasta_config.get_dockercfg_location(),
    )
    return yaml.dump(master_config, Dumper=Dumper, default_flow_style=False)


def create_complete_config(
    service: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
    k8s_enabled: bool = False,
):
    """Generate a namespace configuration file for Tron, for a service."""
    job_configs = load_tron_service_config(
        service=service, cluster=cluster, load_deployments=True, soa_dir=soa_dir,
    )
    preproccessed_config = {}
    preproccessed_config["jobs"] = {
        job_config.get_name(): format_tron_job_dict(
            job_config=job_config, k8s_enabled=k8s_enabled
        )
        for job_config in job_configs
    }
    return yaml.dump(preproccessed_config, Dumper=Dumper, default_flow_style=False)


def validate_complete_config(
    service: str, cluster: str, soa_dir: str = DEFAULT_SOA_DIR
) -> List[str]:
    job_configs = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=False,
        soa_dir=soa_dir,
        for_validation=True,
    )

    # PaaSTA-specific validation
    for job_config in job_configs:
        check_msgs = job_config.validate()
        if check_msgs:
            return check_msgs

    master_config_path = os.path.join(
        os.path.abspath(soa_dir), "tron", cluster, MASTER_NAMESPACE + ".yaml"
    )

    # TODO: remove creating the master config here once we're fully off of mesos
    # since we only have it here to verify that the generated tronfig will be valid
    # given that the kill-switch will affect PaaSTA's setup_tron_namespace script (we're
    # not reading the kill-switch in Tron since it's not easily accessible at the point
    # at which we'd like to fallback to Mesos if toggled)
    master_config = yaml.safe_load(
        create_complete_master_config(cluster=cluster, soa_dir=soa_dir)
    )
    k8s_enabled_for_cluster = master_config.get("k8s_options", {}).get("enabled", False)

    preproccessed_config = {}
    # Use Tronfig on generated config from PaaSTA to validate the rest
    preproccessed_config["jobs"] = {
        job_config.get_name(): format_tron_job_dict(
            job_config=job_config, k8s_enabled=k8s_enabled_for_cluster
        )
        for job_config in job_configs
    }

    complete_config = yaml.dump(preproccessed_config, Dumper=Dumper)

    proc = subprocess.run(
        ["tronfig", "-", "-V", "-n", service, "-m", master_config_path],
        input=complete_config,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )

    if proc.returncode != 0:
        process_errors = proc.stderr.strip()
        if process_errors:  # Error running tronfig
            print(proc.stderr)
        return [proc.stdout.strip()]

    return []


def get_tron_namespaces(cluster, soa_dir):
    tron_config_file = f"tron-{cluster}.yaml"
    config_dirs = [
        _dir[0]
        for _dir in os.walk(os.path.abspath(soa_dir))
        if tron_config_file in _dir[2]
    ]
    namespaces = [os.path.split(config_dir)[1] for config_dir in config_dirs]
    return namespaces


def list_tron_clusters(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> List[str]:
    """Returns the Tron clusters a service is configured to deploy to."""
    search_re = r"/tron-([0-9a-z-_]*)\.yaml$"
    service_dir = os.path.join(soa_dir, service)
    clusters = []
    for filename in glob.glob(f"{service_dir}/*.yaml"):
        cluster_re_match = re.search(search_re, filename)
        if cluster_re_match is not None:
            clusters.append(cluster_re_match.group(1))
    return clusters


def get_tron_dashboard_for_cluster(cluster: str):
    dashboards = load_system_paasta_config().get_dashboard_links()[cluster]
    if "Tron" not in dashboards:
        raise Exception(f"tron api endpoint is not defined for cluster {cluster}")
    return dashboards["Tron"]


def tron_jobs_running_here() -> List[Tuple[str, str, int]]:
    return mesos_services_running_here(
        framework_filter=lambda fw: fw["name"].startswith("tron"),
        parse_service_instance_from_executor_id=parse_service_instance_from_executor_id,
    )


def parse_service_instance_from_executor_id(task_id: str) -> Tuple[str, str]:
    """Parses tron mesos task ids, like schematizer.traffic_generator.28414.turnstyle.46da87d7-6092-4ed4-b926-ffa7b21c7785"""
    try:
        service, job, job_run, action, uuid = task_id.split(".")
    except Exception as e:
        log.warning(
            f"Couldn't parse the mesos task id into a valid tron job: {task_id}: {e}"
        )
        service, job, action = "unknown_service", "unknown_job", "unknown_action"
    return service, f"{job}.{action}"
