#!/usr/bin/env python
import argparse
import asyncio
import logging
from typing import Type

import pysensu_yelp

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.instance import kubernetes as pik
from paasta_tools.kubernetes_tools import get_kubernetes_app_name
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.metrics.metastatus_lib import suffixed_number_value
from paasta_tools.monitoring_tools import send_event
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Check all autoscaled services to see if they're at their max_instances. If"
            " so, send an alert if their utilization is above"
            " max_instances_alert_threshold."
        )
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=DEFAULT_SOA_DIR,
        help="Use a different soa config directory",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Print Sensu alert events instead of sending them",
    )
    return parser.parse_args()


async def check_max_instances(
    soa_dir: str,
    cluster: str,
    instance_type_class: Type[KubernetesDeploymentConfig],
    system_paasta_config: SystemPaastaConfig,
    dry_run: bool = False,
):
    kube_client = KubeClient()
    for service in list_services(soa_dir=soa_dir):
        service_config = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
        for job_config in service_config.instance_configs(
            cluster=cluster, instance_type_class=instance_type_class
        ):
            instance = job_config.get_instance()
            if not job_config.get_autoscaling_metric_spec(
                name=get_kubernetes_app_name(service, instance),
                cluster=cluster,
                kube_client=kube_client,
                namespace=job_config.get_namespace(),
            ):
                # Not an instance that uses HPA, don't check.
                # TODO: should we send status=0 here, in case someone disables autoscaling for their service / changes
                # to bespoke autoscaler?
                continue

            if not job_config.get_docker_image():
                # skip services that haven't been marked for deployment yet.
                continue

            autoscaling_status = await pik.autoscaling_status(
                kube_client=kube_client,
                job_config=job_config,
                namespace=job_config.get_namespace(),
            )
            if autoscaling_status["min_instances"] == -1:
                log.warning(
                    f"HPA {job_config.get_sanitised_deployment_name()} not found."
                )
                continue

            if (
                autoscaling_status["min_instances"]
                == autoscaling_status["max_instances"]
            ) and "canary" in instance:
                status = pysensu_yelp.Status.OK
                output = (
                    f"Not checking {service}.{instance} as the instance name contains"
                    ' "canary" and min_instances == max_instances.'
                )
            elif (
                autoscaling_status["desired_replicas"]
                >= autoscaling_status["max_instances"]
            ):

                metrics_provider_configs = job_config.get_autoscaling_params()[
                    "metrics_providers"
                ]

                status = pysensu_yelp.Status.UNKNOWN
                output = "how are there no metrics for this thing?"

                # This makes an assumption that the metrics currently used by the HPA are exactly the same order (and
                # length) as the list of metrics_providers dictionaries. This should generally be true, but between
                # yelpsoa-configs being pushed and the HPA actually being updated it may not be true. This might cause
                # spurious alerts, but hopefully the frequency is low. We can add some safeguards if it's a problem.
                # (E.g. smarter matching between the status dicts and the config dicts, or bailing/not alerting if the
                # lists aren't the same lengths.)
                for metric, metrics_provider_config in zip(
                    autoscaling_status["metrics"], metrics_provider_configs
                ):

                    setpoint = metrics_provider_config["setpoint"]
                    threshold = metrics_provider_config.get(
                        "max_instances_alert_threshold",
                        setpoint,
                    )

                    metric_threshold_target_ratio = threshold / setpoint
                    try:
                        current_value = suffixed_number_value(metric["current_value"])
                        target_value = suffixed_number_value(metric["target_value"])

                        if current_value / target_value > metric_threshold_target_ratio:
                            status = pysensu_yelp.Status.CRITICAL
                            output = (
                                f"{service}.{instance}: Service is at max_instances, and"
                                " ratio of current value to target value"
                                f" ({current_value} / {target_value}) is greater than the"
                                " ratio of max_instances_alert_threshold to setpoint"
                                f" ({threshold} / {setpoint})"
                            )
                        else:
                            status = pysensu_yelp.Status.OK
                            output = (
                                f"{service}.{instance}: Service is at max_instances, but"
                                " ratio of current value to target value"
                                f" ({current_value} / {target_value}) is below the ratio of"
                                f" max_instances_alert_threshold to setpoint ({threshold} /"
                                f" {setpoint})"
                            )
                    except KeyError:
                        # we likely couldn't find values for the current metric from autoscaling status
                        # if this is the only metric, we will return UNKNOWN+this error
                        # suggest fixing their autoscaling config
                        output = f'{service}.{instance}: Service is at max_instances, and there is an error fetching your {metrics_provider_config["type"]} metric. Check your autoscaling configs or reach out to #paasta.'
            else:
                status = pysensu_yelp.Status.OK
                output = f"{service}.{instance} is below max_instances."

            monitoring_overrides = job_config.get_monitoring()
            monitoring_overrides.update(
                {
                    "page": False,  # TODO: remove this line once this alert has been deployed for a little while.
                    "runbook": "y/check-autoscaler-max-instances",
                    "realert_every": 60,  # The check runs once a minute, so this would realert every hour.
                    "tip": (
                        "The autoscaler wants to scale up to handle additional load"
                        " because your service is overloaded, but cannot scale any"
                        " higher because of max_instances. You may want to bump"
                        " max_instances. To make this alert quieter, adjust"
                        " autoscaling.metrics_providers[n].max_instances_alert_threshold in yelpsoa-configs."
                    ),
                }
            )
            send_event(
                service,
                check_name=f"check_autoscaler_max_instances.{service}.{instance}",
                overrides=monitoring_overrides,
                status=status,
                output=output,
                soa_dir=soa_dir,
                ttl=None,
                cluster=cluster,
                system_paasta_config=system_paasta_config,
                dry_run=dry_run,
            )


def main():
    args = parse_args()
    system_paasta_config = load_system_paasta_config()

    for instance_type_class in [KubernetesDeploymentConfig, EksDeploymentConfig]:
        asyncio.run(
            check_max_instances(
                soa_dir=args.soa_dir,
                cluster=system_paasta_config.get_cluster(),
                instance_type_class=instance_type_class,
                system_paasta_config=system_paasta_config,
                dry_run=args.dry_run,
            )
        )


if __name__ == "__main__":
    main()
