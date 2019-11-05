import logging
import threading
from abc import ABC
from abc import abstractmethod
from time import sleep
from typing import Optional
from typing import Union

from kubernetes.client import V1beta1PodDisruptionBudget
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import create_pod_disruption_budget
from paasta_tools.kubernetes_tools import create_stateful_set
from paasta_tools.kubernetes_tools import force_delete_pods
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import pod_disruption_budget_for_service_instance
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.kubernetes_tools import update_stateful_set
from paasta_tools.utils import SystemPaastaConfig


class Application(ABC):
    def __init__(
        self,
        item: Union[V1Deployment, V1StatefulSet],
        logging=logging.getLogger(__name__),
    ) -> None:
        """
        This Application wrapper is an interface for creating/deleting k8s deployments and statefulsets
        soa_config is KubernetesDeploymentConfig. It is not loaded in init because it is not always required.
        :param item: Kubernetes Object(V1Deployment/V1StatefulSet) that has already been filled up.
        :param logging: where logs go
        """
        if not item.metadata.namespace:
            item.metadata.namespace = "paasta"
        self.kube_deployment = KubeDeployment(
            service=item.metadata.labels["yelp.com/paasta_service"],
            instance=item.metadata.labels["yelp.com/paasta_instance"],
            git_sha=item.metadata.labels["yelp.com/paasta_git_sha"],
            config_sha=item.metadata.labels["yelp.com/paasta_config_sha"],
            replicas=item.spec.replicas,
        )
        self.item = item
        self.soa_config = None  # type: KubernetesDeploymentConfig
        self.logging = logging

    def load_local_config(
        self, soa_dir: str, system_paasta_config: SystemPaastaConfig
    ) -> Optional[KubernetesDeploymentConfig]:
        if not self.soa_config:
            self.soa_config = load_kubernetes_service_config_no_cache(
                service=self.kube_deployment.service,
                instance=self.kube_deployment.instance,
                cluster=system_paasta_config.get_cluster(),
                soa_dir=soa_dir,
            )
        return self.soa_config

    def __str__(self):
        service = self.kube_deployment.service
        instance = self.kube_deployment.instance
        git_sha = self.kube_deployment.git_sha
        config_sha = self.kube_deployment.config_sha
        return f"{service}-{instance}-{git_sha}-{config_sha}"

    @abstractmethod
    def deep_delete(self, kube_client: KubeClient) -> None:
        """
        Remove all controllers, pods, and pod disruption budgets related to this application
        :param kube_client:
        """
        pass

    def create(self, kube_client: KubeClient):
        """
        Create all controllers, HPA, and pod disruption budgets related to this application
        :param kube_client:
        """
        pass

    def update(self, kube_client: KubeClient):
        """
        Update all controllers, HPA, and pod disruption budgets related to this application
        :param kube_client:
        """
        pass

    def delete_pod_disruption_budget(self, kube_client: KubeClient) -> None:
        try:
            kube_client.policy.delete_namespaced_pod_disruption_budget(
                name=self.item.metadata.name,
                namespace=self.item.metadata.namespace,
                body=V1DeleteOptions(),
            )
        except ApiException as e:
            if e.status == 404:
                # Deployment does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    "not deleting nonexistent pod disruption budget/{} from namespace/{}".format(
                        self.item.metadata.name, self.item.metadata.namespace
                    )
                )
            else:
                raise
        else:
            self.logging.info(
                "deleted pod disruption budget/{} from namespace/{}".format(
                    self.item.metadata.name, self.item.metadata.namespace
                )
            )

    def ensure_pod_disruption_budget(
        self, kube_client: KubeClient
    ) -> V1beta1PodDisruptionBudget:
        pdr = pod_disruption_budget_for_service_instance(
            service=self.kube_deployment.service,
            instance=self.kube_deployment.instance,
            max_unavailable="{}%".format(
                int((1 - self.soa_config.get_bounce_margin_factor()) * 100)
            ),
        )
        try:
            existing_pdr = kube_client.policy.read_namespaced_pod_disruption_budget(
                name=pdr.metadata.name, namespace=pdr.metadata.namespace
            )
        except ApiException as e:
            if e.status == 404:
                existing_pdr = None
            else:
                raise

        if existing_pdr:
            if existing_pdr.spec.max_unavailable != pdr.spec.max_unavailable:
                logging.debug(f"Updating poddisruptionbudget {pdr.metadata.name}")
                return kube_client.policy.patch_namespaced_pod_disruption_budget(
                    name=pdr.metadata.name, namespace=pdr.metadata.namespace, body=pdr
                )
            else:
                logging.debug(f"poddisruptionbudget {pdr.metadata.name} up to date")
        else:
            logging.debug(f"creating poddisruptionbudget {pdr.metadata.name}")
            return create_pod_disruption_budget(
                kube_client=kube_client, pod_disruption_budget=pdr
            )


class DeploymentWrapper(Application):
    def deep_delete(self, kube_client: KubeClient) -> None:
        """
        Remove all controllers, pods, and pod disruption budgets related to this application
        :param kube_client:
        """
        delete_options = V1DeleteOptions(propagation_policy="Foreground")
        try:
            kube_client.deployments.delete_namespaced_deployment(
                self.item.metadata.name,
                self.item.metadata.namespace,
                body=delete_options,
            )
        except ApiException as e:
            if e.status == 404:
                # Deployment does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    "not deleting nonexistent deploy/{} from namespace/{}".format(
                        self.item.metadata.name, self.item.metadata.namespace
                    )
                )
            else:
                raise
        else:
            self.logging.info(
                "deleted deploy/{} from namespace/{}".format(
                    self.item.metadata.name, self.item.metadata.namespace
                )
            )
        self.delete_pod_disruption_budget(kube_client)
        self.delete_horizontal_pod_autoscaler(kube_client)

    def get_existing_app(self, kube_client: KubeClient):
        return kube_client.deployments.read_namespaced_deployment(
            name=self.item.metadata.name, namespace=self.item.metadata.namespace
        )

    def create(self, kube_client: KubeClient) -> None:
        create_deployment(kube_client=kube_client, formatted_deployment=self.item)
        self.ensure_pod_disruption_budget(kube_client)
        self.sync_horizontal_pod_autoscaler(kube_client)

    def deep_delete_and_create(self, kube_client: KubeClient) -> None:
        self.deep_delete(kube_client)
        timer = 0
        while (
            self.kube_deployment in set(list_all_deployments(kube_client))
            and timer < 60
        ):
            sleep(1)
            timer += 1

        if timer >= 60 and self.kube_deployment in set(
            list_all_deployments(kube_client)
        ):
            try:
                force_delete_pods(
                    self.item.metadata.name,
                    self.kube_deployment.service,
                    self.kube_deployment.instance,
                    self.item.metadata.namespace,
                    kube_client,
                )
            except ApiException as e:
                if e.status == 404:
                    # Deployment does not exist, nothing to delete but
                    # we can consider this a success.
                    self.logging.debug(
                        "not deleting nonexistent deploy/{} from namespace/{}".format(
                            self.kube_deployment.service, self.item.metadata.namespace
                        )
                    )
                else:
                    raise
        else:
            self.logging.info(
                "deleted deploy/{} from namespace/{}".format(
                    self.kube_deployment.service, self.item.metadata.namespace
                )
            )
        self.create(kube_client=kube_client)

    def update(self, kube_client: KubeClient) -> None:
        # If HPA is enabled, do not update replicas.
        # In all other cases, replica is set to max(instances, min_instances)
        if self.soa_config.config_dict.get("bounce_method", "") == "brutal":
            threading.Thread(
                target=self.deep_delete_and_create, args=[KubeClient()]
            ).start()
            return
        if self.should_have_hpa() is None:
            self.item.spec.replicas = self.get_existing_app(kube_client).spec.replicas
        update_deployment(kube_client=kube_client, formatted_deployment=self.item)
        self.ensure_pod_disruption_budget(kube_client)
        self.sync_horizontal_pod_autoscaler(kube_client)

    def should_have_hpa(self):
        return (
            self.soa_config.get_max_instances() is not None
            # with bespoke autoscaler, setup_kubernetes_job sets the number of instances directly; no HPA is required.
            and self.soa_config.get_autoscaling_params()["decision_policy"] != "bespoke"
        )

    def sync_horizontal_pod_autoscaler(self, kube_client: KubeClient) -> None:
        """
        In order for autoscaling to work, there needs to be at least two configurations
        min_instnace, max_instance, and there cannot be instance.
        """
        self.logging.info(
            f"Syncing HPA setting for {self.item.metadata.name}/name in {self.item.metadata.namespace}"
        )
        hpa_exists = self.exists_hpa(kube_client)
        # NO autoscaling
        if not self.should_have_hpa():
            # Remove HPA if autoscaling is disabled
            if hpa_exists:
                self.delete_horizontal_pod_autoscaler(kube_client)
            return

        body = self.soa_config.get_autoscaling_metric_spec(
            name=self.item.metadata.name, namespace=self.item.metadata.namespace
        )
        if not body:
            raise Exception(
                f"CRIT: autoscaling misconfigured for {self.kube_deployment.service}."
                + f"{self.kube_deployment.instance}.Please correct the configuration and update pre-commit hook."
            )
        self.logging.debug(body)
        if not hpa_exists:
            self.logging.info(
                f"Creating new HPA for {self.item.metadata.name}/name in {self.item.metadata.namespace}"
            )
            kube_client.autoscaling.create_namespaced_horizontal_pod_autoscaler(
                namespace=self.item.metadata.namespace, body=body, pretty=True
            )
        else:
            self.logging.info(
                f"Updating new HPA for {self.item.metadata.name}/name in {self.item.metadata.namespace}/namespace"
            )
            kube_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler(
                name=self.item.metadata.name,
                namespace=self.item.metadata.namespace,
                body=body,
                pretty=True,
            )

    def exists_hpa(self, kube_client: KubeClient) -> bool:
        return (
            len(
                kube_client.autoscaling.list_namespaced_horizontal_pod_autoscaler(
                    field_selector=f"metadata.name={self.item.metadata.name}",
                    namespace=self.item.metadata.namespace,
                ).items
            )
            > 0
        )

    def delete_horizontal_pod_autoscaler(self, kube_client: KubeClient) -> None:
        try:
            kube_client.autoscaling.delete_namespaced_horizontal_pod_autoscaler(
                name=self.item.metadata.name,
                namespace=self.item.metadata.namespace,
                body=V1DeleteOptions(),
            )
        except ApiException as e:
            if e.status == 404:
                # Deployment does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    f"not deleting nonexistent HPA/{self.item.metadata.name} from namespace/{self.item.metadata.namespace}"
                )
            else:
                raise
        else:
            self.logging.info(
                "deleted HPA/{} from namespace/{}".format(
                    self.item.metadata.name, self.item.metadata.namespace
                )
            )


class StatefulSetWrapper(Application):
    def deep_delete(self, kube_client: KubeClient) -> None:
        """
        Remove all controllers, pods, and pod disruption budgets related to this application
        :param kube_client:
        """
        delete_options = V1DeleteOptions(propagation_policy="Foreground")
        try:
            kube_client.deployments.delete_namespaced_stateful_set(
                self.item.metadata.name,
                self.item.metadata.namespace,
                body=delete_options,
            )
        except ApiException as e:
            if e.status == 404:
                # StatefulSet does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    "not deleting nonexistent statefulset/{} from namespace/{}".format(
                        self.item.metadata.name, self.item.metadata.namespace
                    )
                )
            else:
                raise
        else:
            self.logging.info(
                "deleted statefulset/{} from namespace/{}".format(
                    self.item.metadata.name, self.item.metadata.namespace
                )
            )
        self.delete_pod_disruption_budget(kube_client)

    def create(self, kube_client: KubeClient):
        create_stateful_set(kube_client=kube_client, formatted_stateful_set=self.item)
        self.ensure_pod_disruption_budget(kube_client)

    def update(self, kube_client: KubeClient):
        update_stateful_set(kube_client=kube_client, formatted_stateful_set=self.item)
        self.ensure_pod_disruption_budget(kube_client)
