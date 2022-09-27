import mock

from paasta_tools.kubernetes_tools import registration_label
from paasta_tools.setup_istio_mesh import cleanup_paasta_namespace_services
from paasta_tools.setup_istio_mesh import sanitise_kubernetes_service_name
from paasta_tools.setup_istio_mesh import setup_istio_mesh
from paasta_tools.setup_istio_mesh import setup_paasta_namespace_services
from paasta_tools.setup_istio_mesh import setup_paasta_routing
from paasta_tools.setup_istio_mesh import UNIFIED_K8S_SVC_NAME
from paasta_tools.setup_istio_mesh import UNIFIED_SVC_PORT


MOCK_PORT_NUMBER = 20508


def test_setup_kube_service():
    mock_client = mock.Mock()
    service_name = "compute-infra-test-service.main"
    mock_paasta_namespaces = {service_name: {"port": MOCK_PORT_NUMBER}}
    sanitized_service_name = sanitise_kubernetes_service_name(service_name)

    fn, *rest = setup_paasta_namespace_services(
        kube_client=mock_client,
        paasta_namespaces=mock_paasta_namespaces,
        existing_namespace_services={},
        existing_virtual_services={},
    )

    k8s_svc = fn.args[1]

    assert len(rest) == 1
    assert fn.func is mock_client.core.create_namespaced_service
    assert k8s_svc.metadata.name == sanitized_service_name
    assert k8s_svc.spec.selector == {registration_label(service_name): "true"}


def test_setup_paasta_routing():
    mock_client = mock.Mock()
    mock_namespaces = dict(foo=dict(proxy_port=MOCK_PORT_NUMBER))

    fn, *rest = setup_paasta_routing(
        kube_client=mock_client,
        namespaces=mock_namespaces,
    )
    k8s_svc = fn.args[1]

    assert len(rest) == 1
    assert fn.func is mock_client.core.create_namespaced_service
    assert len(k8s_svc.spec.ports) == 2
    assert k8s_svc.spec.ports[0].port == UNIFIED_SVC_PORT
    assert k8s_svc.spec.ports[1].port == MOCK_PORT_NUMBER


def test_cleanup_paasta_namespace_services_does_not_remove_unified_svc():
    mock_client = mock.Mock()
    mock_paasta_namespaces = {"svc1", "svc2"}

    mock_existing_namespace_services = {"svc1", "svc2", "svc3", UNIFIED_K8S_SVC_NAME}
    calls = list(
        cleanup_paasta_namespace_services(
            mock_client,
            mock_paasta_namespaces,
            mock_existing_namespace_services,
            mock_existing_namespace_services,
        )
    )
    funcs = {fn.func for fn in calls}
    services = {calls[0].args[0], calls[1].args[-1]}

    assert {
        mock_client.core.delete_namespaced_service,
        mock_client.custom.delete_namespaced_custom_object,
    } == funcs
    assert {"svc3"} == services


def test_cleanup_paasta_namespace_services_does_not_remove_svc_while_running_first_time():
    mock_client = mock.Mock()
    mock_paasta_namespaces = {"svc1", "svc2"}

    mock_existing_namespace_services = {}
    calls = cleanup_paasta_namespace_services(
        mock_client,
        mock_paasta_namespaces,
        mock_existing_namespace_services,
        mock_existing_namespace_services,
    )
    assert len(list(calls)) == 0


@mock.patch("time.sleep", autospec=True)
@mock.patch("paasta_tools.setup_istio_mesh.process_kube_services", autospec=True)
def test_setup_istio_mesh_iterates(mock_process_kube_services, mock_sleep):
    mock_yielded = mock.Mock()
    mock_process_kube_services.return_value = iter([mock_yielded])
    setup_istio_mesh(mock.Mock())
    assert len(mock_yielded.mock_calls) == 1
