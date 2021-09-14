import mock

from paasta_tools.kubernetes_tools import registration_label
from paasta_tools.setup_istio_mesh import sanitise_kubernetes_service_name
from paasta_tools.setup_istio_mesh import setup_paasta_namespace_services
from paasta_tools.setup_istio_mesh import setup_unified_service
from paasta_tools.setup_istio_mesh import UNIFIED_SVC_PORT

MOCK_PORT_NUMBER = 20508


def test_setup_kube_service():
    mock_client = mock.Mock()
    service_name = "compute-infra-test-service.main"
    mock_paasta_namespaces = {service_name: {"port": 20508}}
    sanitized_service_name = sanitise_kubernetes_service_name(service_name)

    (k8s_fn, (_, k8s_svc)), *rest = setup_paasta_namespace_services(
        kube_client=mock_client, paasta_namespaces=mock_paasta_namespaces
    )

    assert len(rest) == 0
    assert k8s_fn is mock_client.core.create_namespaced_service
    assert k8s_svc.metadata.name == sanitized_service_name
    assert k8s_svc.spec.selector == {registration_label(service_name): "true"}


def test_setup_unified_service():
    mock_client = mock.Mock()

    mock_port_list = [MOCK_PORT_NUMBER]

    (k8s_fn, (_, k8s_svc)), *rest = setup_unified_service(
        kube_client=mock_client, port_list=mock_port_list
    )

    assert len(rest) == 0
    assert k8s_fn is mock_client.core.create_namespaced_service
    assert len(k8s_svc.spec.ports) == 2
    assert k8s_svc.spec.ports[0].port == UNIFIED_SVC_PORT
    assert k8s_svc.spec.ports[1].port == MOCK_PORT_NUMBER
