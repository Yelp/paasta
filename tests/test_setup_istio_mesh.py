import mock

from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.setup_istio_mesh import PAATA_REGISTRATION_PREFIX
from paasta_tools.setup_istio_mesh import setup_paasta_namespace_service
from paasta_tools.setup_istio_mesh import setup_unified_service
from paasta_tools.setup_istio_mesh import UNIFIED_SVC_PORT

MOCK_PORT_NUMBER = 20508


def test_setup_kube_service():
    mock_client = mock.Mock()

    mock_paasta_namespaces = {"20508": "compute-infra-test-service"}

    sanitized_service_name = sanitise_kubernetes_name(mock_paasta_namespaces["20508"])

    setup_paasta_namespace_service(
        kube_client=mock_client, paasta_namespaces=mock_paasta_namespaces
    )

    assert mock_client.core.create_namespaced_service.call_count == 1
    assert (
        mock_client.core.create_namespaced_service.call_args[0][1].metadata.name
        == sanitized_service_name
    )
    assert mock_client.core.create_namespaced_service.call_args[0][1].spec.selector == {
        f"{PAATA_REGISTRATION_PREFIX}/{sanitized_service_name}": "true"
    }


def test_setup_unified_service():
    mock_client = mock.Mock()

    mock_port_list = [MOCK_PORT_NUMBER]

    setup_unified_service(kube_client=mock_client, port_list=mock_port_list)

    assert mock_client.core.create_namespaced_service.call_count == 1
    assert (
        len(mock_client.core.create_namespaced_service.call_args[0][1].spec.ports) == 2
    )
    assert (
        mock_client.core.create_namespaced_service.call_args[0][1].spec.ports[0].port
        == UNIFIED_SVC_PORT
    )
    assert (
        mock_client.core.create_namespaced_service.call_args[0][1].spec.ports[1].port
        == MOCK_PORT_NUMBER
    )
