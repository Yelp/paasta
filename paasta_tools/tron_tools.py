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
import json
import logging
import os
import pkgutil
import re
import subprocess
from string import Formatter
from typing import List
from typing import Mapping
from typing import Tuple
from typing import Union

import yaml
from mypy_extensions import TypedDict
from service_configuration_lib import read_extra_service_information
from service_configuration_lib import read_yaml_file
from service_configuration_lib.spark_config import get_total_driver_memory_mb
from service_configuration_lib.spark_config import SparkConfBuilder

from paasta_tools.mesos_tools import mesos_services_running_here

try:
    from yaml.cyaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Dumper = yaml.SafeDumper  # type: ignore

from paasta_tools.clusterman import get_clusterman_metrics
from paasta_tools.tron.client import TronClient
from paasta_tools.tron import tron_command_context
from paasta_tools.utils import DEFAULT_SOA_DIR, InstanceConfigDict
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import time_cache
from paasta_tools.utils import filter_templates_from_config
from paasta_tools.utils import TronSecretVolume
from paasta_tools.utils import get_k8s_url_for_cluster
from paasta_tools.utils import validate_pool
from paasta_tools.utils import PoolsNotConfiguredError
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import ProjectedSAVolume

from paasta_tools import spark_tools

from paasta_tools.kubernetes_tools import (
    allowlist_denylist_to_requirements,
    get_service_account_name,
    limit_size_with_hash,
    raw_selectors_to_requirements,
    to_node_label,
)
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.secret_tools import is_shared_secret
from paasta_tools.secret_tools import is_shared_secret_from_secret_name
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.kubernetes_tools import get_paasta_secret_name
from paasta_tools.kubernetes_tools import add_volumes_for_authenticating_services
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE

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
MESOS_EXECUTOR_NAMES = ("paasta",)
KUBERNETES_EXECUTOR_NAMES = ("paasta", "spark")
EXECUTOR_NAME_TO_TRON_EXECUTOR_TYPE = {"paasta": "kubernetes", "spark": "spark"}
KUBERNETES_NAMESPACE = "tron"
DEFAULT_AWS_REGION = "us-west-2"
EXECUTOR_TYPE_TO_NAMESPACE = {
    "paasta": "tron",
    "spark": "tron",
}
DEFAULT_TZ = "US/Pacific"
clusterman_metrics, _ = get_clusterman_metrics()
EXECUTOR_TYPES = ["paasta", "ssh", "spark"]
DEFAULT_SPARK_EXECUTOR_POOL = "batch"


class FieldSelectorConfig(TypedDict):
    field_path: str


class TronNotConfigured(Exception):
    pass


class InvalidTronConfig(Exception):
    pass


class InvalidPoolError(Exception):
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


def _get_tron_k8s_cluster_override(cluster: str) -> Optional[str]:
    """
    Return the name of a compute cluster if there's a different compute cluster that should be used to run a Tronjob.
    Will return None if no override mapping is present

    We have certain Tron masters that are named differently from the compute cluster that should actually be used (
    e.g., we might have tron-XYZ-test-prod, but instead of scheduling on XYZ-test-prod, we'd like to schedule jobs
    on test-prod).

    To control this, we have an optional config item that we'll puppet onto Tron masters that need this type of
    tron master -> compute cluster override which this function will read.
    """
    return (
        load_system_paasta_config()
        .get_tron_k8s_cluster_overrides()
        .get(
            cluster,
            None,
        )
    )


def _spark_k8s_role() -> str:
    return load_system_paasta_config().get_spark_k8s_role()


class TronActionConfigDict(InstanceConfigDict, total=False):
    # this is kinda confusing: long-running stuff is currently using cmd
    # ...but tron are using command - this is going to require a little
    # maneuvering to unify
    command: str
    service_account_name: str

    # the values for this dict can be anything since it's whatever
    # spark accepts
    spark_args: Dict[str, Any]
    force_spark_resource_configs: bool
    # TODO: TRON-2145: use this to implement timeout for non-spark actions in tron
    max_runtime: str
    mrjob: bool


class TronActionConfig(InstanceConfig):
    config_dict: TronActionConfigDict
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

        self.action_spark_config = None
        if self.get_executor() == "spark":
            # build the complete Spark configuration
            # TODO: add conditional check for Spark specific commands spark-submit, pyspark etc ?
            self.action_spark_config = self.build_spark_config()

    def get_cpus(self) -> float:
        # set Spark driver pod CPU if it is specified by Spark arguments
        if (
            self.action_spark_config
            and "spark.driver.cores" in self.action_spark_config
        ):
            return float(self.action_spark_config["spark.driver.cores"])
        # we fall back to this default if there's no spark.driver.cores config
        return super().get_cpus()

    def get_mem(self) -> float:
        # get Spark driver pod memory specified by Spark arguments
        if self.action_spark_config:
            return get_total_driver_memory_mb(self.action_spark_config)
        # we fall back to this default if there's no Spark config
        return super().get_mem()

    def get_disk(self, default: float = 1024) -> float:
        # increase default threshold for Spark driver pod memory because 1G is too low
        if self.action_spark_config and "disk" not in self.config_dict:
            return spark_tools.SPARK_DRIVER_DEFAULT_DISK_MB
        # we fall back to this default if there's no Spark config
        return super().get_disk()

    def build_spark_config(self) -> Dict[str, str]:
        system_paasta_config = load_system_paasta_config()
        resolved_cluster = system_paasta_config.get_eks_cluster_aliases().get(
            self.get_cluster(), self.get_cluster()
        )
        pool = self.get_spark_executor_pool()
        try:
            if not validate_pool(resolved_cluster, pool, system_paasta_config):
                raise InvalidPoolError(
                    f"Job {self.get_service()}.{self.get_instance()}: "
                    f"pool '{pool}' is invalid for cluster '{resolved_cluster}'"
                )
        except PoolsNotConfiguredError:
            log.warning(
                f"Could not fetch allowed_pools for `{resolved_cluster}`. Skipping pool validation.\n"
            )

        spark_args = self.config_dict.get("spark_args", {})
        # most of the service_configuration_lib function expected string values only
        # so let's go ahead and convert the values now instead of once per-wrapper
        stringified_spark_args = {
            k: (str(v) if not isinstance(v, bool) else str(v).lower())
            for k, v in spark_args.items()
        }

        spark_app_name = stringified_spark_args.get(
            "spark.app.name",
            f"tron_spark_{self.get_service()}_{self.get_instance()}",
        )

        docker_img_url = self.get_docker_url(system_paasta_config)

        spark_conf_builder = SparkConfBuilder()
        spark_conf = spark_conf_builder.get_spark_conf(
            cluster_manager="kubernetes",
            spark_app_base_name=spark_app_name,
            user_spark_opts=stringified_spark_args,
            paasta_cluster=resolved_cluster,
            paasta_pool=self.get_spark_executor_pool(),
            paasta_service=self.get_service(),
            paasta_instance=self.get_instance(),
            docker_img=docker_img_url,
            extra_volumes=self.get_volumes(
                system_paasta_config.get_volumes(),
                uses_bulkdata_default=system_paasta_config.get_uses_bulkdata_default(),
            ),
            use_eks=True,
            k8s_server_address=get_k8s_url_for_cluster(self.get_cluster()),
            force_spark_resource_configs=self.config_dict.get(
                "force_spark_resource_configs", False
            ),
            user=spark_tools.SPARK_JOB_USER,
        )
        # delete the dynamically generated spark.app.id to prevent frequent config updates in Tron.
        # spark.app.id will be generated later by yelp spark-submit wrapper or Spark itself.
        spark_conf.pop("spark.app.id", None)
        # use a static spark.app.name to prevent frequent config updates in Tron.
        # md5 and base64 will always generate the same encoding for a string.
        # This spark.app.name might be overridden by yelp spark-submit wrapper.
        if "spark.app.name" in spark_conf:
            spark_conf["spark.app.name"] = limit_size_with_hash(
                f"tron_spark_{self.get_service()}_{self.get_instance()}_{self.get_action_name()}"
                if "spark.app.name" not in stringified_spark_args
                else stringified_spark_args["spark.app.name"]
            )
        # TODO: Remove this once dynamic pod template is generated inside the driver using spark-submit wrapper
        if "spark.kubernetes.executor.podTemplateFile" in spark_conf:
            print(
                f"Replacing spark.kubernetes.executor.podTemplateFile="
                f"{spark_conf['spark.kubernetes.executor.podTemplateFile']} with "
                f"spark.kubernetes.executor.podTemplateFile={spark_tools.SPARK_DNS_POD_TEMPLATE}"
            )
            spark_conf[
                "spark.kubernetes.executor.podTemplateFile"
            ] = spark_tools.SPARK_DNS_POD_TEMPLATE

        spark_conf.update(
            {
                "spark.hadoop.fs.s3a.aws.credentials.provider": spark_tools.SPARK_AWS_CREDS_PROVIDER,
                "spark.driver.host": "$PAASTA_POD_IP",
            }
        )
        spark_conf.setdefault(
            "spark.kubernetes.executor.label.yelp.com/owner", self.get_team()
        )

        # We are using the Service Account created using the provided or default IAM role.
        spark_conf[
            "spark.kubernetes.authenticate.executor.serviceAccountName"
        ] = get_service_account_name(
            iam_role=self.get_spark_executor_iam_role(),
        )

        return spark_conf

    def get_cmd(self):
        command = self.config_dict.get("command")
        return command

    def get_job_name(self):
        return self.job

    def get_action_name(self):
        return self.action

    # mypy does not like the SecretVolume -> TronSecretVolume conversion, because TypedDict inheritence is broken.
    # Until this is fixed, let's ignore this issue.
    def get_secret_volumes(self) -> List[TronSecretVolume]:  # type: ignore
        """Adds the secret_volume_name to the object so tron/task_processing can load it downstream without replicating code."""
        secret_volumes = super().get_secret_volumes()
        tron_secret_volumes = []
        for secret_volume in secret_volumes:
            tron_secret_volume = TronSecretVolume(
                secret_volume_name=self.get_secret_volume_name(
                    secret_volume["secret_name"]
                ),
                secret_name=secret_volume["secret_name"],
                container_path=secret_volume["container_path"],
                items=secret_volume.get("items", []),
            )
            # we have a different place where the default can come from (tron) and we don't want to insert the wrong default here
            if "default_mode" in secret_volume:
                tron_secret_volume["default_mode"] = secret_volume["default_mode"]

            tron_secret_volumes.append(tron_secret_volume)
        return tron_secret_volumes

    def get_namespace(self) -> str:
        """Get namespace from config, default to 'paasta'"""
        return self.config_dict.get("namespace", KUBERNETES_NAMESPACE)

    def get_secret_volume_name(self, secret_name: str) -> str:
        service = (
            self.service
            if not is_shared_secret_from_secret_name(
                soa_dir=self.soa_dir, secret_name=secret_name
            )
            else SHARED_SECRET_SERVICE
        )
        return get_paasta_secret_name(
            self.get_namespace(),
            service,
            secret_name,
        )

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

    def get_env(
        self,
        system_paasta_config: Optional["SystemPaastaConfig"] = None,
    ) -> Dict[str, str]:
        env = super().get_env(system_paasta_config=system_paasta_config)
        if self.get_executor() == "spark":
            # Required by some sdks like boto3 client. Throws NoRegionError otherwise.
            # AWS_REGION takes precedence if set.
            env["AWS_DEFAULT_REGION"] = DEFAULT_AWS_REGION
            env["PAASTA_INSTANCE_TYPE"] = "spark"
            # XXX: is this actually necessary? every PR that's added this hasn't really mentioned why,
            # and Chesterton's Fence makes me very wary about removing it
            env["SPARK_USER"] = "root"
            # XXX: we were adding the commandline we were starting the Spark driver with to SPARK_OPTS
            # before, but that doesn't really seem necessary from my testing (driver starts just fine)
            # if this changes and we do need it - please add a comment about *why* we need it!
            # XXX: update PAASTA_RESOURCE_* env vars to use the correct value from spark_args and set
            # these to the correct values for the executors as part of the driver commandline

        return env

    def get_iam_role(self) -> str:
        iam_role = super().get_iam_role()

        if not iam_role and self.get_executor() == "spark":
            iam_role = load_system_paasta_config().get_spark_driver_iam_role()

        return iam_role

    def get_spark_executor_iam_role(self) -> str:
        return (
            self.get_iam_role()
            or load_system_paasta_config().get_spark_executor_iam_role()
        )

    def get_secret_env(self) -> Mapping[str, dict]:
        base_env = self.config_dict.get("env", {})
        secret_env = {}
        for k, v in base_env.items():
            if is_secret_ref(v):
                secret = get_secret_name_from_ref(v)
                service = (
                    self.service if not is_shared_secret(v) else SHARED_SECRET_SERVICE
                )
                secret_env[k] = {
                    "secret_name": get_paasta_secret_name(
                        self.get_namespace(),
                        service,
                        secret,
                    ),
                    "key": secret,
                }
        return secret_env

    def get_field_selector_env(self) -> Dict[str, FieldSelectorConfig]:
        # we're not expecting users to need to add any of these themselves, so for now
        # we'll just hardcode the env vars we want to add by default
        return {
            "PAASTA_POD_IP": {
                "field_path": "status.podIP",
            }
        }

    def get_cpu_burst_add(self) -> float:
        """For Tron jobs, we don't let them burst by default, because they
        don't represent "real-time" workloads, and should not impact
        neighbors"""
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
            allowlist=self.get_deploy_whitelist(),
            denylist=self.get_deploy_blacklist(),
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
        # We are not allowing users to specify `cpus` and `mem` configuration if the action is a Spark job
        # with driver running on k8s (executor: spark), because we derive these values from `spark.driver.cores`
        # and `spark.driver.memory` in order to avoid confusion.
        if self.get_executor() == "spark":
            if "cpus" in self.config_dict:
                error_msgs.append(
                    f"{self.get_job_name()}.{self.get_action_name()} is a Spark job. `cpus` config is not allowed. "
                    f"Please specify the driver cores using `spark.driver.cores`."
                )
            if "mem" in self.config_dict:
                error_msgs.append(
                    f"{self.get_job_name()}.{self.get_action_name()} is a Spark job. `mem` config is not allowed. "
                    f"Please specify the driver memory using `spark.driver.memory`."
                )
        return error_msgs

    def get_pool(self) -> str:
        """
        Returns the default pool override if pool is not defined in the action configuration.

        This is useful for environments like spam to allow us to default the pool to spam but allow users to
        override this value. To control this, we have an optional config item that we'll puppet onto Tron masters
        which this function will read.
        """
        return (
            self.config_dict.get(
                "pool", load_system_paasta_config().get_tron_default_pool_override()
            )
            if not self.get_executor() == "spark"
            else spark_tools.SPARK_DRIVER_POOL
        )

    def get_spark_executor_pool(self) -> str:
        return self.config_dict.get("pool", DEFAULT_SPARK_EXECUTOR_POOL)

    def get_service_account_name(self) -> Optional[str]:
        return self.config_dict.get("service_account_name")

    def get_projected_sa_volumes(self) -> Optional[List[ProjectedSAVolume]]:
        projected_volumes = add_volumes_for_authenticating_services(
            service_name=self.service,
            config_volumes=super().get_projected_sa_volumes(),
            soa_dir=self.soa_dir,
        )
        return projected_volumes if projected_volumes else None


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

    def get_name(self):
        return self.name

    def get_node(self):
        return self.config_dict.get("node", "paasta")

    def get_schedule(self):
        return self.config_dict.get("schedule")

    def get_cron_expression(self) -> Optional[str]:
        schedule = self.config_dict.get("schedule")
        # TODO(TRON-1746): once we simplify this format, we can clean this code up
        if (
            isinstance(schedule, dict)
            and "type" in schedule
            and schedule["type"] == "cron"
        ):
            return schedule["value"]
        elif isinstance(schedule, str) and schedule.startswith("cron"):
            # most cron parsers won't understand our schedule tag, so we need to strip
            # that off before passing it to anything else
            return schedule.replace("cron", "")

        return None

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

    def _get_action_config(self, action_name, action_dict) -> TronActionConfig:
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
                    "image_version": deployments_json.get_image_version_for_deploy_group(
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

                if self.soa_dir != DEFAULT_SOA_DIR:
                    log.warning(
                        f"Error: No deployments.json found in {self.soa_dir}/{action_service}. "
                        "You can generate this by running: "
                        f"generate_deployments_for_service -d {self.soa_dir} -s {action_service}"
                    )

                branch_dict = None
        else:
            branch_dict = None
        action_dict["monitoring"] = self.get_monitoring()

        cluster_override = _get_tron_k8s_cluster_override(self.get_cluster())
        return TronActionConfig(
            service=action_service,
            instance=compose_instance(self.get_name(), action_name),
            cluster=cluster_override or self.get_cluster(),
            config_dict=action_dict,
            branch_dict=branch_dict,
            soa_dir=self.soa_dir,
            for_validation=self.for_validation,
        )

    def get_actions(self) -> List[TronActionConfig]:
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
            {
                "default_volumes": format_volumes(default_volumes),
            }
        )
        master_config["k8s_options"] = k8s_options
    return master_config


def format_tron_action_dict(action_config: TronActionConfig):
    """Generate a dict of tronfig for an action, from the TronActionConfig.

    :param action_config: TronActionConfig
    """
    executor = action_config.get_executor()
    result = {
        "command": action_config.get_cmd(),
        "executor": executor,
        "requires": action_config.get_requires(),
        "node": action_config.get_node(),
        "retries": action_config.get_retries(),
        "retries_delay": action_config.get_retries_delay(),
        "secret_volumes": action_config.get_secret_volumes(),
        "expected_runtime": action_config.get_expected_runtime(),
        "trigger_downstreams": action_config.get_trigger_downstreams(),
        "triggered_by": action_config.get_triggered_by(),
        "on_upstream_rerun": action_config.get_on_upstream_rerun(),
        "trigger_timeout": action_config.get_trigger_timeout(),
        # outside of Spark use-cases, we also allow users to specify an expected-to-exist Service Account name
        # in the Tron namespace in case an action needs specific k8s permissions (e.g., a Jolt batch may need
        # k8s permissions to list Jolt pods in the jolt namespace to do science™ to them).
        # if the provided Service Account does not exist, Tron should simply fail to create the Podspec and report
        # a failure
        # NOTE: this will get overridden if an action specifies Pod Identity configs
        "service_account_name": action_config.get_service_account_name(),
    }

    if executor in KUBERNETES_EXECUTOR_NAMES:
        # we'd like Tron to be able to distinguish between spark and normal actions
        # even though they both run on k8s
        result["executor"] = EXECUTOR_NAME_TO_TRON_EXECUTOR_TYPE.get(
            executor, "kubernetes"
        )

        result["secret_env"] = action_config.get_secret_env()
        result["field_selector_env"] = action_config.get_field_selector_env()
        all_env = action_config.get_env()
        # For k8s, we do not want secret envvars to be duplicated in both `env` and `secret_env`
        # or for field selector env vars to be overwritten
        result["env"] = {
            k: v
            for k, v in all_env.items()
            if not is_secret_ref(v) and k not in result["field_selector_env"]
        }
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
                action_config.get_instance(),
                limit=63,
                suffix=4,
            ),
            # XXX: should this be different for Spark drivers launched by Tron?
            "app.kubernetes.io/managed-by": "tron",
        }

        # we can hardcode this for now as batches really shouldn't
        # need routable IPs and we know that Spark probably does.
        result["annotations"] = {
            "paasta.yelp.com/routable_ip": "true" if executor == "spark" else "false",
        }

        result["labels"]["yelp.com/owner"] = "compute_infra_platform_experience"

        if (
            action_config.get_iam_role_provider() == "aws"
            and action_config.get_iam_role()
        ):
            # this service account will be used for normal Tron batches as well as for Spark drivers
            result["service_account_name"] = get_service_account_name(
                iam_role=action_config.get_iam_role(),
                k8s_role=None,
            )

        # service account token volumes for service authentication
        result["projected_sa_volumes"] = action_config.get_projected_sa_volumes()

        # XXX: now that we're actually passing through extra_volumes correctly (e.g., using get_volumes()),
        # we can get rid of the default_volumes from the Tron master config
        system_paasta_config = load_system_paasta_config()
        extra_volumes = action_config.get_volumes(
            system_paasta_config.get_volumes(),
            uses_bulkdata_default=system_paasta_config.get_uses_bulkdata_default(),
        )
        if executor == "spark":
            is_mrjob = action_config.config_dict.get("mrjob", False)
            system_paasta_config = load_system_paasta_config()
            # inject additional Spark configs in case of Spark commands
            result["command"] = spark_tools.build_spark_command(
                result["command"],
                action_config.action_spark_config,
                is_mrjob,
                action_config.config_dict.get(
                    "max_runtime", spark_tools.DEFAULT_SPARK_RUNTIME_TIMEOUT
                ),
            )
            # point to the KUBECONFIG needed by Spark driver
            result["env"]["KUBECONFIG"] = system_paasta_config.get_spark_kubeconfig()

            # spark, unlike normal batches, needs to expose several ports for things like the spark
            # ui and for executor->driver communication
            result["ports"] = list(
                set(
                    spark_tools.get_spark_ports_from_config(
                        action_config.action_spark_config
                    )
                )
            )
            # mount KUBECONFIG file for Spark drivers to communicate with EKS cluster
            extra_volumes.append(
                DockerVolume(
                    {
                        "containerPath": system_paasta_config.get_spark_kubeconfig(),
                        "hostPath": system_paasta_config.get_spark_kubeconfig(),
                        "mode": "RO",
                    }
                )
            )
            # Add pod annotations and labels for Spark monitoring metrics
            monitoring_annotations = (
                spark_tools.get_spark_driver_monitoring_annotations(
                    action_config.action_spark_config
                )
            )
            monitoring_labels = spark_tools.get_spark_driver_monitoring_labels(
                action_config.action_spark_config
            )
            result["annotations"].update(monitoring_annotations)
            result["labels"].update(monitoring_labels)

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
        result["extra_volumes"] = format_volumes(extra_volumes)
        result["docker_image"] = action_config.get_docker_url()

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def format_tron_job_dict(job_config: TronJobConfig, k8s_enabled: bool = False):
    """Generate a dict of tronfig for a job, from the TronJobConfig.

    :param job_config: TronJobConfig
    """
    action_dict = {
        action_config.get_action_name(): format_tron_action_dict(
            action_config=action_config,
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

    cleanup_config = job_config.get_cleanup_action()
    if cleanup_config:
        cleanup_action = format_tron_action_dict(
            action_config=cleanup_config,
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
    for action in load_tron_instance_configs(
        service=service,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir,
    ):
        if action.get_instance() == instance:
            return action
    raise NoConfigurationForServiceError(
        f"No tron configuration found for {service} {instance}"
    )


@time_cache(ttl=5)
def load_tron_instance_configs(
    service: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Tuple[TronActionConfig, ...]:
    ret: List[TronActionConfig] = []

    jobs = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir,
    )

    for job in jobs:
        ret.extend(job.get_actions())

    return tuple(ret)


@time_cache(ttl=5)
def load_tron_service_config(
    service,
    cluster,
    load_deployments=True,
    soa_dir=DEFAULT_SOA_DIR,
    for_validation=False,
):
    return load_tron_service_config_no_cache(
        service,
        cluster,
        load_deployments,
        soa_dir,
        for_validation,
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
    dry_run: bool = False,
):
    """Generate a namespace configuration file for Tron, for a service."""
    job_configs = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=True,
        soa_dir=soa_dir,
        for_validation=dry_run,
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


def _is_valid_namespace(job: Any, tron_executors: List[str]) -> bool:
    for action_info in job.get("actions", {}).values():
        if action_info.get("executor", "paasta") in tron_executors:
            return True
    return False


def get_tron_namespaces(
    cluster: str,
    soa_dir: str,
    tron_executors: List[str] = EXECUTOR_TYPES,
) -> List[str]:
    tron_config_file = f"tron-{cluster}.yaml"
    config_dirs = [
        _dir[0]
        for _dir in os.walk(os.path.abspath(soa_dir))
        if tron_config_file in _dir[2]
    ]
    namespaces = [os.path.split(config_dir)[1] for config_dir in config_dirs]
    tron_namespaces = set()
    for namespace in namespaces:
        config = filter_templates_from_config(
            read_extra_service_information(
                namespace,
                extra_info=f"tron-{cluster}",
                soa_dir=soa_dir,
                deepcopy=False,
            )
        )
        for job in config.values():
            if _is_valid_namespace(job, tron_executors):
                tron_namespaces.add(namespace)
                break
    return list(tron_namespaces)


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
