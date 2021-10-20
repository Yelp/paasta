import mock

from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import registration_label
from paasta_tools.setup_istio_mesh import cleanup_paasta_namespace_services
from paasta_tools.setup_istio_mesh import PAASTA_NAMESPACE
from paasta_tools.setup_istio_mesh import sanitise_kubernetes_service_name
from paasta_tools.setup_istio_mesh import setup_istio_mesh
from paasta_tools.setup_istio_mesh import setup_paasta_namespace_services
from paasta_tools.setup_istio_mesh import setup_paasta_routing
from paasta_tools.setup_istio_mesh import UNIFIED_K8S_SVC_NAME
from paasta_tools.setup_istio_mesh import UNIFIED_SVC_PORT


MOCK_SVC_NAME = "foo"
MOCK_PORT_NUMBER = 20508


def test_setup_kube_service():
    mock_client = mock.Mock()
    service_name = "compute-infra-test-service.main"
    mock_paasta_namespaces = {service_name: {"port": MOCK_PORT_NUMBER}}
    sanitized_service_name = sanitise_kubernetes_service_name(service_name)

    fn, *rest = setup_paasta_namespace_services(
        kube_client=mock_client,
        paasta_namespaces=mock_paasta_namespaces,
        existing_kubernetes_services={},
        existing_virtual_services={},
    )

    k8s_svc = fn.args[1]

    assert len(rest) == 1
    assert fn.func is mock_client.core.create_namespaced_service
    assert k8s_svc.metadata.name == sanitized_service_name
    assert k8s_svc.spec.selector == {registration_label(service_name): "true"}


def test_setup_paasta_routing_create_both():
    mock_client = mock.Mock()
    mock_namespaces = {MOCK_SVC_NAME: dict(proxy_port=MOCK_PORT_NUMBER)}

    fn1, fn2, *rest = setup_paasta_routing(mock_client, mock_namespaces, {}, {})
    assert len(rest) == 0, "only 2 yielded calls are expected"

    assert fn1.func is mock_client.core.create_namespaced_service
    k8s_svc = fn1.args[1]
    assert (
        k8s_svc.metadata.annotations[paasta_prefixed("config_hash")]
        == "LN/dOllAx6ey4Z4V26RS/Q=="
    ), "config_hash annotation value has changed, update it in the test if it's intentional"
    assert len(k8s_svc.spec.ports) == 2
    assert k8s_svc.spec.ports[0].port == UNIFIED_SVC_PORT
    assert k8s_svc.spec.ports[1].port == MOCK_PORT_NUMBER

    assert fn2.func is mock_client.custom.create_namespaced_custom_object
    istio_vs = fn2.args[4]
    assert (
        istio_vs["metadata"]["annotations"][paasta_prefixed("config_hash")]
        == "KP05UYQYZC8KHiCDj8lCXQ=="
    ), "config_hash annotation value has changed, update it in the test if it's intentional"
    assert istio_vs["spec"]["hosts"] == [
        "paasta-routing",
        "169.254.255.254",
    ], "unified service name or yocalhost address changed, please update it in the test if it's intentional (IT PROBABLY IS NOT!)"
    assert len(istio_vs["spec"]["http"]) == 2
    assert istio_vs["spec"]["http"][0] == {
        "delegate": {"name": MOCK_SVC_NAME, "namespace": PAASTA_NAMESPACE},
        "match": [{"headers": {"x-yelp-svc": {"exact": MOCK_SVC_NAME}}}],
    }
    assert istio_vs["spec"]["http"][1] == {
        "delegate": {"name": MOCK_SVC_NAME, "namespace": PAASTA_NAMESPACE},
        "match": [{"port": MOCK_PORT_NUMBER}],
    }


def test_setup_paasta_routing_create_only_k8s_svc():
    mock_client = mock.Mock()
    mock_namespaces = {MOCK_SVC_NAME: dict(proxy_port=MOCK_PORT_NUMBER)}
    mock_vs = dict(
        metadata=dict(
            annotations={"paasta.yelp.com/config_hash": "KP05UYQYZC8KHiCDj8lCXQ=="}
        )
    )
    fn, *rest = setup_paasta_routing(
        mock_client, mock_namespaces, {}, {UNIFIED_K8S_SVC_NAME: mock_vs},
    )
    assert len(rest) == 0, "only 1 yielded call is expected"
    assert fn.func is mock_client.core.create_namespaced_service


def test_setup_paasta_routing_create_only_istio_vs():
    mock_client = mock.Mock()
    mock_namespaces = {MOCK_SVC_NAME: dict(proxy_port=MOCK_PORT_NUMBER)}
    mock_svc = mock.Mock()
    mock_svc.metadata.annotations = {
        "paasta.yelp.com/config_hash": "LN/dOllAx6ey4Z4V26RS/Q=="
    }
    fn, *rest = setup_paasta_routing(
        mock_client, mock_namespaces, {UNIFIED_K8S_SVC_NAME: mock_svc}, {},
    )
    assert len(rest) == 0, "only 1 yielded call is expected"
    assert fn.func is mock_client.custom.create_namespaced_custom_object


def test_setup_paasta_routing_replace_when_config_hash_differs():
    mock_client = mock.Mock()
    mock_namespaces = {MOCK_SVC_NAME: dict(proxy_port=MOCK_PORT_NUMBER)}
    mock_svc = mock.Mock()
    mock_svc.metadata.annotations = {"paasta.yelp.com/config_hash": "wrong"}
    mock_vs = dict(metadata=dict(annotations={"paasta.yelp.com/config_hash": "wrong"}))
    fn1, fn2, *rest = setup_paasta_routing(
        mock_client,
        mock_namespaces,
        {UNIFIED_K8S_SVC_NAME: mock_svc},
        {UNIFIED_K8S_SVC_NAME: mock_vs},
    )
    assert len(rest) == 0, "only 2 yielded calls are expected"
    assert fn1.func is mock_client.core.replace_namespaced_service
    assert fn2.func is mock_client.custom.replace_namespaced_custom_object


def test_setup_paasta_routing_noop_when_config_hash_same():
    mock_client = mock.Mock()
    mock_namespaces = {MOCK_SVC_NAME: dict(proxy_port=MOCK_PORT_NUMBER)}
    mock_svc = mock.Mock()
    mock_svc.metadata.annotations = {
        "paasta.yelp.com/config_hash": "LN/dOllAx6ey4Z4V26RS/Q=="
    }
    mock_vs = dict(
        metadata=dict(
            annotations={"paasta.yelp.com/config_hash": "KP05UYQYZC8KHiCDj8lCXQ=="}
        )
    )
    calls = setup_paasta_routing(
        mock_client,
        mock_namespaces,
        {UNIFIED_K8S_SVC_NAME: mock_svc},
        {UNIFIED_K8S_SVC_NAME: mock_vs},
    )
    assert len(list(calls)) == 0, "no calls are expected"


def test_cleanup_paasta_namespace_services_does_not_remove_unified_svc():
    mock_client = mock.Mock()
    mock_paasta_namespaces = {"svc1", "svc2"}

    mock_existing_kubernetes_services = {"svc1", "svc2", "svc3", UNIFIED_K8S_SVC_NAME}
    calls = list(
        cleanup_paasta_namespace_services(
            mock_client,
            mock_paasta_namespaces,
            mock_existing_kubernetes_services,
            mock_existing_kubernetes_services,
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

    mock_existing_kubernetes_services = {}
    calls = cleanup_paasta_namespace_services(
        mock_client,
        mock_paasta_namespaces,
        mock_existing_kubernetes_services,
        mock_existing_kubernetes_services,
    )
    assert len(list(calls)) == 0


@mock.patch("time.sleep", autospec=True)
@mock.patch("paasta_tools.setup_istio_mesh.process_kube_services", autospec=True)
def test_setup_istio_mesh_iterates(mock_process_kube_services, mock_sleep):
    mock_yielded = mock.Mock()
    mock_process_kube_services.return_value = iter([mock_yielded])
    setup_istio_mesh(mock.Mock())
    assert len(mock_yielded.mock_calls) == 1
