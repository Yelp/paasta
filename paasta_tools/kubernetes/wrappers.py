import logging
from abc import ABC
from abc import abstractmethod

from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import get_deployment_config
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig


class Application(ABC):
    def __init__(
            self,
            item: KubeDeployment,
            kube_client: KubeClient,
            logging=logging.getLogger(__name__),
    ) -> None:
        self.kube_deployment = KubeDeployment(
            service=item.metadata.labels['yelp.com/paasta_service'],
            instance=item.metadata.labels['yelp.com/paasta_instance'],
            git_sha=item.metadata.labels['yelp.com/paasta_git_sha'],
            config_sha=item.metadata.labels['yelp.com/paasta_config_sha'],
            replicas=item.spec.replicas,
        )
        self.item = item
        self.kube_client = kube_client
        self.soa_config = None
        self.logging = logging

    def load_local_config(self, soa_dir, system_paasta_config) -> "KubernetesDeploymentConfig":
        if not self.soa_config:
            self.soa_config = get_deployment_config(self.item, soa_dir, system_paasta_config.get_cluster())
        return self.soa_config

    def __str__(self):
        return self.kube_deployment.__str__()

    @abstractmethod
    def deep_delete(self) -> None:
        pass

    def delete_pod_disruption_budget(self) -> None:
        try:
            self.kube_client.policy.delete_namespaced_pod_disruption_budget(
                name=f'{self.kube_deployment.service}-{self.kube_deployment.instance}',
                namespace=self.item.metadata.namespace,
                body=V1DeleteOptions(),
            )
        except ApiException as e:
            if e.status == 404:
                # Deployment does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    'not deleting nonexistent pod disruption budget/{} from namespace/{}'.format(
                        self.item.metadata.name, self.item.metadata.namespace,
                    ),
                )
            else:
                raise e
        else:
            self.logging.info('deleted pod disruption budget/{} from namespace/{}'.format(
                self.item.metadata.name, self.item.metadata.namespace,
            ))


class DeploymentWrapper(Application):
    def deep_delete(self) -> None:
        delete_options = V1DeleteOptions(
            propagation_policy='Foreground',
        )
        try:
            self.kube_client.deployments.delete_namespaced_deployment(
                self.item.metadata.name, self.item.metadata.namespace, delete_options,
            )
        except ApiException as e:
            if e.status == 404:
                # Deployment does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    'not deleting nonexistent deploy/{} from namespace/{}'.format(
                        self.item.metadata.name, self.item.metadata.namespace,
                    ),
                )
            else:
                raise e
        else:
            self.logging.info('deleted deploy/{} from namespace/{}'.format(
                self.item.metadata.name, self.item.metadata.namespace,
            ))
        self.delete_pod_disruption_budget()


class StatefulSetWrapper(Application):
    def deep_delete(self) -> None:
        delete_options = V1DeleteOptions(
            propagation_policy='Foreground',
        )
        try:
            self.kube_client.deployments.delete_namespaced_stateful_set(
                self.item.metadata.name, self.item.metadata.namespace, delete_options,
            )
        except ApiException as e:
            if e.status == 404:
                # StatefulSet does not exist, nothing to delete but
                # we can consider this a success.
                self.logging.debug(
                    'not deleting nonexistent statefulset/{} from namespace/{}'.format(
                        self.item.metadata.name, self.item.metadata.namespace,
                    ),
                )
            else:
                raise e
        else:
            self.logging.info('deleted statefulset/{} from namespace/{}'.format(
                self.item.metadata.name, self.item.metadata.namespace,
            ))
        self.delete_pod_disruption_budget()
