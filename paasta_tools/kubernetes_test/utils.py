from collections import defaultdict

from kubernetes.client import V1Deployment
from kubernetes.client import V1DeploymentList
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetList
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import KubeClient


class MockApi():
    """
    This is a very very simple object that mocks Kubernetes client.
    Be sure to add/update it before using it.
    """
    deployments = defaultdict(dict)
    stateful_sets = defaultdict(dict)
    pod_disruption_budgets = defaultdict(dict)

    def list_namespaced_deployment(self, namespace, *args):
        res = V1DeploymentList(items=[v for k, v in self.deployments[namespace].items()])
        return res

    def list_namespaced_stateful_set(self, namespace, *args):
        res = V1StatefulSetList(items=[v for k, v in self.stateful_sets[namespace].items()])
        return res

    def create_namespaced_deployment(self, namespace, body, *args):
        if not isinstance(body, V1Deployment):
            raise Exception('body should be V1Deployment')
        self.deployments[namespace][body.metadata.name] = body

    def create_namespaced_stateful_set(self, namespace, body, *args):
        if not isinstance(body, V1StatefulSet):
            raise Exception('body should be V1Deployment')
        self.stateful_sets[namespace][body.metadata.name] = body

    def delete_namespaced_deployment(self, name, namespace, *args):
        if namespace not in self.deployments or name not in self.deployments[namespace]:
            raise ApiException(status=404, reason='name or namespace does not exist')
        self.deployments[namespace].pop(name, None)

    def delete_namespaced_stateful_set(self, name, namespace, *args):
        if namespace not in self.stateful_sets or name not in self.stateful_sets[namespace]:
            raise ApiException(status=404, reason='name or namespace does not exist')
        self.stateful_sets[namespace].pop(name, None)

    def delete_namespaced_pod_disruption_budget(self, name, namespace, *args, **kwargs):
        if namespace not in self.pod_disruption_budgets or name not in self.pod_disruption_budgets[namespace]:
            raise ApiException(status=404, reason='name or namespace does not exist')
        self.pod_disruption_budgets[namespace].pop(name, None)


class MockKubeClient(KubeClient):
    """
    This is a very very simple object that mocks KubeClient
    Be sure to add/update it before using it.
    """

    def __init__(self):
        self.deployments = MockApi()
        self.core = MockApi()
        self.policy = MockApi()
        self.apiextensions = MockApi()
        self.custom = MockApi()
