import logging
from abc import ABC
from abc import abstractmethod
from typing import Optional
from typing import Union

from kubernetes.client import V1beta1PodDisruptionBudget
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from kubernetes.client.rest import ApiException

from paasta_tools.autoscaling.autoscaling_service_lib import autoscaling_is_paused
from paasta_tools.eks_tools import load_eks_service_config_no_cache
from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import create_pod_disruption_budget
from paasta_tools.kubernetes_tools import create_stateful_set
from paasta_tools.kubernetes_tools import ensure_service_account
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import pod_disruption_budget_for_service_instance
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.kubernetes_tools import update_stateful_set
from paasta_tools.utils import load_system_paasta_config


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
        attrs = {
            attr: item.metadata.labels.get(paasta_prefixed(attr))
            for attr in [
                "service",
                "instance",
                "git_sha",
                "image_version",
                "config_sha",
            ]
        }

        replicas = (
            item.spec.replicas
            if item.metadata.labels.get(paasta_prefixed("autoscaled"), "false")
            == "false"
            else None
        )
        self.kube_deployment = KubeDeployment(
            replicas=replicas, namespace=item.metadata.namespace, **attrs
        )
        self.item = item
        self.soa_config = None  # type: KubernetesDeploymentConfig
        self.logging = logging

    def load_local_config(
        self, soa_dir: str, cluster: str, eks: bool = False
    ) -> Optional[KubernetesDeploymentConfig]:
        if not self.soa_config:
            if eks:
                self.soa_config = load_eks_service_config_no_cache(
                    service=self.kube_deployment.service,
                    instance=self.kube_deployment.instance,
                    cluster=cluster,
                    soa_dir=soa_dir,
                )
            else:
                self.soa_config = load_kubernetes_service_config_no_cache(
                    service=self.kube_deployment.service,
                    instance=self.kube_deployment.instance,
                    cluster=cluster,
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

    def update_related_api_objects(self, kube_client: KubeClient) -> None:
        """
        Update related Kubernetes API objects such as HPAs and Pod Disruption Budgets
        :param kube_client:
        """
        self.ensure_pod_disruption_budget(kube_client, self.soa_config.get_namespace())

    def update_dependency_api_objects(self, kube_client: KubeClient) -> None:
        """
        Update related Kubernetes API objects that should be updated before the main object,
        such as service accounts.
        :param kube_client:
        """
        self.ensure_service_account(kube_client)

    def ensure_service_account(self, kube_client: KubeClient) -> None:
        """
        Ensure that the service account for this application exists
        :param kube_client:
        """
        if self.soa_config.get_iam_role():
            ensure_service_account(
                iam_role=self.soa_config.get_iam_role(),
                namespace=self.soa_config.get_namespace(),
                kube_client=kube_client,
            )

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
        self, kube_client: KubeClient, namespace: str
    ) -> V1beta1PodDisruptionBudget:
        max_unavailable: Union[str, int]
        if "bounce_margin_factor" in self.soa_config.config_dict:
            max_unavailable = (
                f"{int((1 - self.soa_config.get_bounce_margin_factor()) * 100)}%"
            )
        else:
            system_paasta_config = load_system_paasta_config()
            max_unavailable = system_paasta_config.get_pdb_max_unavailable()

        pdr = pod_disruption_budget_for_service_instance(
            service=self.kube_deployment.service,
            instance=self.kube_deployment.instance,
            max_unavailable=max_unavailable,
            namespace=namespace,
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
            if existing_pdr.spec.min_available is not None:
                logging.info(
                    "Not updating poddisruptionbudget: can't have both "
                    "min_available and max_unavailable"
                )
            elif existing_pdr.spec.max_unavailable != pdr.spec.max_unavailable:
                logging.info(f"Updating poddisruptionbudget {pdr.metadata.name}")
                return kube_client.policy.patch_namespaced_pod_disruption_budget(
                    name=pdr.metadata.name, namespace=pdr.metadata.namespace, body=pdr
                )
            else:
                logging.info(f"poddisruptionbudget {pdr.metadata.name} up to date")
        else:
            logging.info(f"creating poddisruptionbudget {pdr.metadata.name}")
            return create_pod_disruption_budget(
                kube_client=kube_client,
                pod_disruption_budget=pdr,
                namespace=pdr.metadata.namespace,
            )


class DeploymentWrapper(Application):
    def deep_delete(
        self, kube_client: KubeClient, propagation_policy="Foreground"
    ) -> None:
        """
        Remove all controllers, pods, and pod disruption budgets related to this application
        :param kube_client:
        """
        delete_options = V1DeleteOptions(propagation_policy=propagation_policy)
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
        create_deployment(
            kube_client=kube_client,
            formatted_deployment=self.item,
            namespace=self.soa_config.get_namespace(),
        )
        self.ensure_pod_disruption_budget(kube_client, self.soa_config.get_namespace())
        self.sync_horizontal_pod_autoscaler(kube_client)

    def update(self, kube_client: KubeClient) -> None:
        # If HPA is enabled, do not update replicas.
        # In all other cases, replica is set to max(instances, min_instances)
        update_deployment(
            kube_client=kube_client,
            formatted_deployment=self.item,
            namespace=self.soa_config.get_namespace(),
        )

    def update_related_api_objects(self, kube_client: KubeClient) -> None:
        super().update_related_api_objects(kube_client)
        self.sync_horizontal_pod_autoscaler(kube_client)

    def sync_horizontal_pod_autoscaler(self, kube_client: KubeClient) -> None:
        """
        In order for autoscaling to work, there needs to be at least two configurations
        min_instnace, max_instance, and there cannot be instance.
        """
        desired_hpa_spec = self.soa_config.get_autoscaling_metric_spec(
            name=self.item.metadata.name,
            cluster=self.soa_config.cluster,
            kube_client=kube_client,
            namespace=self.item.metadata.namespace,
        )

        hpa_exists = self.exists_hpa(kube_client)
        should_have_hpa = desired_hpa_spec and not autoscaling_is_paused()

        if not should_have_hpa:
            self.logging.info(
                f"No HPA required for {self.item.metadata.name}/name in {self.item.metadata.namespace}"
            )
            if hpa_exists:
                self.logging.info(
                    f"Deleting HPA for {self.item.metadata.name}/name in {self.item.metadata.namespace}"
                )
                self.delete_horizontal_pod_autoscaler(kube_client)
            return

        self.logging.info(
            f"Syncing HPA setting for {self.item.metadata.name}/name in {self.item.metadata.namespace}"
        )
        self.logging.debug(desired_hpa_spec)
        if not hpa_exists:
            self.logging.info(
                f"Creating new HPA for {self.item.metadata.name}/name in {self.item.metadata.namespace}"
            )
            kube_client.autoscaling.create_namespaced_horizontal_pod_autoscaler(
                namespace=self.item.metadata.namespace,
                body=desired_hpa_spec,
                pretty=True,
            )
        else:
            self.logging.info(
                f"Updating new HPA for {self.item.metadata.name}/name in {self.item.metadata.namespace}/namespace"
            )
            kube_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler(
                name=self.item.metadata.name,
                namespace=self.item.metadata.namespace,
                body=desired_hpa_spec,
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
        create_stateful_set(
            kube_client=kube_client,
            formatted_stateful_set=self.item,
            namespace=self.soa_config.get_namespace(),
        )
        self.ensure_pod_disruption_budget(kube_client, self.soa_config.get_namespace())

    def update(self, kube_client: KubeClient):
        update_stateful_set(
            kube_client=kube_client,
            formatted_stateful_set=self.item,
            namespace=self.soa_config.get_namespace(),
        )


def get_application_wrapper(
    formatted_application: Union[V1Deployment, V1StatefulSet]
) -> Application:
    app: Application
    if isinstance(formatted_application, V1Deployment):
        app = DeploymentWrapper(formatted_application)
    elif isinstance(formatted_application, V1StatefulSet):
        app = StatefulSetWrapper(formatted_application)
    else:
        raise Exception("Unknown kubernetes object to update")

    return app
