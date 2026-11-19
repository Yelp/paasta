from unittest import mock

import pytest
from kubernetes.client import V1IPBlock
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1NetworkPolicy
from kubernetes.client import V1NetworkPolicyEgressRule
from kubernetes.client import V1NetworkPolicyIngressRule
from kubernetes.client import V1NetworkPolicyPeer
from kubernetes.client import V1NetworkPolicyPort
from kubernetes.client import V1NetworkPolicySpec
from kubernetes.client import V1ObjectMeta

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.firewall_tools import FirewallConfigDict
from paasta_tools.firewall_tools import FirewallPortDict
from paasta_tools.firewall_tools import FirewallRuleDict
from paasta_tools.firewall_tools import _egress_rule_for_firewall_rule
from paasta_tools.firewall_tools import _ingress_rule_for_firewall_rule
from paasta_tools.firewall_tools import _network_policy_ports
from paasta_tools.firewall_tools import _peer_for_cidr
from paasta_tools.firewall_tools import _peers_for_firewall_rule
from paasta_tools.firewall_tools import _peers_for_service_ref
from paasta_tools.firewall_tools import create_network_policy
from paasta_tools.firewall_tools import network_policy_for_service_instance
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import NoConfigurationForServiceError


def test_create_network_policy():
    mock_client = mock.Mock()
    mock_policy = V1NetworkPolicy()
    create_network_policy(mock_client, mock_policy, "paasta")
    mock_client.networking.create_namespaced_network_policy.assert_called_with(
        namespace="paasta", body=mock_policy
    )


def mock_instance_config(instance: str, namespace: str) -> mock.Mock:
    config = mock.Mock(spec=EksDeploymentConfig)
    config.get_instance.return_value = instance
    config.get_namespace.return_value = namespace
    return config


def mock_paasta_service_config_loader(configs_by_service):
    def make_loader(service=None, soa_dir=None):
        loader = mock.create_autospec(PaastaServiceConfigLoader, instance=True)
        loader.instance_configs.return_value = configs_by_service.get(service, [])
        return loader

    return mock.patch(
        "paasta_tools.firewall_tools.PaastaServiceConfigLoader",
        side_effect=make_loader,
        autospec=True,
    )


def mock_eks_service_config_loader(instance_config):
    return mock.patch(
        "paasta_tools.firewall_tools.load_eks_service_config",
        return_value=instance_config,
        autospec=True,
    )


@pytest.mark.parametrize(
    "service_ref, pscl_configs, direct_config, expected_peers",
    (
        pytest.param(
            "some-service",
            {
                "some-service": [
                    mock_instance_config("main", "paastasvc-some-service"),
                    mock_instance_config("worker", "paastasvc-some-service"),
                ]
            },
            mock_instance_config("main", "paastasvc-some-service"),
            [
                V1NetworkPolicyPeer(
                    namespace_selector=V1LabelSelector(
                        match_labels={
                            "kubernetes.io/metadata.name": "paastasvc-some-service"
                        }
                    ),
                    pod_selector=V1LabelSelector(
                        match_labels={"paasta.yelp.com/service": "some-service"}
                    ),
                )
            ],
            id="bare-name-all-instances",
        ),
        pytest.param(
            "some-service.main",
            {"some-service": []},
            mock_instance_config("main", "paastasvc-some-service"),
            [
                V1NetworkPolicyPeer(
                    namespace_selector=V1LabelSelector(
                        match_labels={
                            "kubernetes.io/metadata.name": "paastasvc-some-service"
                        }
                    ),
                    pod_selector=V1LabelSelector(
                        match_labels={
                            "paasta.yelp.com/service": "some-service",
                            "paasta.yelp.com/instance": "main",
                        }
                    ),
                )
            ],
            id="dotted-name-one-instance",
        ),
        pytest.param(
            "some-service.main",
            {"some-service": []},
            mock_instance_config("main", "custom-namespace"),
            [
                V1NetworkPolicyPeer(
                    namespace_selector=V1LabelSelector(
                        match_labels={"kubernetes.io/metadata.name": "custom-namespace"}
                    ),
                    pod_selector=V1LabelSelector(
                        match_labels={
                            "paasta.yelp.com/service": "some-service",
                            "paasta.yelp.com/instance": "main",
                        }
                    ),
                )
            ],
            id="custom-namespace",
        ),
        pytest.param(
            "service_with_underscores.main",
            {"service_with_underscores": []},
            mock_instance_config("main", "paastasvc-service--with--underscores"),
            [
                V1NetworkPolicyPeer(
                    namespace_selector=V1LabelSelector(
                        match_labels={
                            "kubernetes.io/metadata.name": (
                                "paastasvc-service--with--underscores"
                            )
                        }
                    ),
                    pod_selector=V1LabelSelector(
                        match_labels={
                            "paasta.yelp.com/service": "service_with_underscores",
                            "paasta.yelp.com/instance": "main",
                        }
                    ),
                )
            ],
            id="raw-service-name-in-pod-label",
        ),
        pytest.param(
            "some-service",
            {
                "some-service": [
                    mock_instance_config("main", "paastasvc-some-service"),
                    mock_instance_config("worker", "custom-namespace"),
                ]
            },
            mock_instance_config("main", "paastasvc-some-service"),
            [
                V1NetworkPolicyPeer(
                    namespace_selector=V1LabelSelector(
                        match_labels={"kubernetes.io/metadata.name": "custom-namespace"}
                    ),
                    pod_selector=V1LabelSelector(
                        match_labels={"paasta.yelp.com/service": "some-service"}
                    ),
                ),
                V1NetworkPolicyPeer(
                    namespace_selector=V1LabelSelector(
                        match_labels={
                            "kubernetes.io/metadata.name": "paastasvc-some-service"
                        }
                    ),
                    pod_selector=V1LabelSelector(
                        match_labels={"paasta.yelp.com/service": "some-service"}
                    ),
                ),
            ],
            id="instances-span-namespaces",
        ),
    ),
)
def test_peers_for_service_ref(
    service_ref, pscl_configs, direct_config, expected_peers
):
    with mock_paasta_service_config_loader(
        pscl_configs
    ), mock_eks_service_config_loader(direct_config):
        peers = _peers_for_service_ref(service_ref, "fake-cluster")
    assert peers == expected_peers


def test_peers_for_service_ref_instance_not_found():
    with mock.patch(
        "paasta_tools.firewall_tools.load_eks_service_config",
        side_effect=NoConfigurationForServiceError("nope"),
        autospec=True,
    ):
        with pytest.raises(NoConfigurationForServiceError):
            _peers_for_service_ref("some-service.worker", "fake-cluster")


def test_peers_for_service_ref_service_not_on_cluster():
    with mock_paasta_service_config_loader({"some-service": []}):
        with pytest.raises(ValueError):
            _peers_for_service_ref("some-service", "fake-cluster")


def test_peer_for_cidr():
    peer = _peer_for_cidr("52.94.0.0/16")
    assert peer.ip_block.cidr == "52.94.0.0/16"


def test_peer_for_cidr_invalid():
    with pytest.raises(ValueError):
        _peer_for_cidr("not-a-cidr")


def test_network_policy_ports():
    rule = {"to_services": ["x"], "ports": [{"protocol": "TCP", "port": 443}]}
    assert _network_policy_ports(rule) == [
        V1NetworkPolicyPort(protocol="TCP", port=443)
    ]


def test_network_policy_ports_empty():
    assert _network_policy_ports({"to_services": ["x"]}) == []


def test_egress_rule_for_firewall_rule():
    rule = {
        "to_services": ["some-service"],
        "ports": [{"protocol": "TCP", "port": 3306}],
    }
    with mock_paasta_service_config_loader(
        {"some-service": [mock_instance_config("main", "paastasvc-some-service")]}
    ):
        egress_rule = _egress_rule_for_firewall_rule(rule, "fake-cluster")
    assert egress_rule == V1NetworkPolicyEgressRule(
        to=[
            V1NetworkPolicyPeer(
                namespace_selector=V1LabelSelector(
                    match_labels={
                        "kubernetes.io/metadata.name": "paastasvc-some-service"
                    }
                ),
                pod_selector=V1LabelSelector(
                    match_labels={"paasta.yelp.com/service": "some-service"}
                ),
            )
        ],
        ports=[V1NetworkPolicyPort(protocol="TCP", port=3306)],
    )


def test_ingress_rule_for_firewall_rule():
    rule = {"from_services": ["some-api.main"]}
    with mock_eks_service_config_loader(
        mock_instance_config("main", "paastasvc-some-api")
    ):
        ingress_rule = _ingress_rule_for_firewall_rule(rule, "fake-cluster")
    assert ingress_rule == V1NetworkPolicyIngressRule(
        _from=[
            V1NetworkPolicyPeer(
                namespace_selector=V1LabelSelector(
                    match_labels={"kubernetes.io/metadata.name": "paastasvc-some-api"}
                ),
                pod_selector=V1LabelSelector(
                    match_labels={
                        "paasta.yelp.com/service": "some-api",
                        "paasta.yelp.com/instance": "main",
                    }
                ),
            )
        ],
        ports=[],
    )


def test_firewall_rule_with_no_destinations_raises():
    # The fail-closed guard: an empty peer list would mean "allow to
    # anywhere", so it must raise instead of building a rule
    with pytest.raises(ValueError):
        _peers_for_firewall_rule(
            "egress", {"ports": [{"protocol": "TCP", "port": 443}]}, "fake-cluster"
        )
    with pytest.raises(ValueError):
        _peers_for_firewall_rule("ingress", {"to_services": []}, "fake-cluster")
    with pytest.raises(ValueError):
        _peers_for_firewall_rule("egress", {}, "fake-cluster")


def test_network_policy_for_service_instance():
    firewall_config = FirewallConfigDict(
        egress=[
            FirewallRuleDict(
                to_services=["some-service"],
                ports=[FirewallPortDict(protocol="TCP", port=3306)],
            )
        ],
        ingress=[
            FirewallRuleDict(
                from_services=["some-api.main"],
                ports=[FirewallPortDict(protocol="TCP", port=8080)],
            )
        ],
    )
    baseline_egress = [
        FirewallRuleDict(
            to_cidrs=["10.0.0.0/8"], ports=[FirewallPortDict(protocol="UDP", port=53)]
        )
    ]
    baseline_ingress = [
        FirewallRuleDict(
            from_cidrs=["10.0.0.0/8"],
            ports=[FirewallPortDict(protocol="TCP", port=8080)],
        )
    ]

    with mock_paasta_service_config_loader(
        {"some-service": [mock_instance_config("main", "paastasvc-some-service")]}
    ), mock_eks_service_config_loader(
        mock_instance_config("main", "paastasvc-some-api")
    ):
        policy = network_policy_for_service_instance(
            service="some-service",
            instance="main",
            cluster="fake-cluster",
            namespace="paastasvc-some-service",
            firewall_config=firewall_config,
            baseline_egress=baseline_egress,
            baseline_ingress=baseline_ingress,
        )

    expected_policy = V1NetworkPolicy(
        api_version="networking.k8s.io/v1",
        kind="NetworkPolicy",
        metadata=V1ObjectMeta(
            name="some-service-main",
            namespace="paastasvc-some-service",
            labels={
                "paasta.yelp.com/managed": "true",
                "paasta.yelp.com/service": "some-service",
                "paasta.yelp.com/instance": "main",
            },
        ),
        spec=V1NetworkPolicySpec(
            pod_selector=V1LabelSelector(
                match_labels={
                    "paasta.yelp.com/service": "some-service",
                    "paasta.yelp.com/instance": "main",
                }
            ),
            policy_types=["Egress", "Ingress"],
            egress=[
                V1NetworkPolicyEgressRule(
                    to=[
                        V1NetworkPolicyPeer(
                            namespace_selector=V1LabelSelector(
                                match_labels={
                                    "kubernetes.io/metadata.name": "paastasvc-some-service"
                                }
                            ),
                            pod_selector=V1LabelSelector(
                                match_labels={"paasta.yelp.com/service": "some-service"}
                            ),
                        )
                    ],
                    ports=[V1NetworkPolicyPort(protocol="TCP", port=3306)],
                ),
                V1NetworkPolicyEgressRule(
                    to=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="10.0.0.0/8"))],
                    ports=[V1NetworkPolicyPort(protocol="UDP", port=53)],
                ),
            ],
            ingress=[
                V1NetworkPolicyIngressRule(
                    _from=[
                        V1NetworkPolicyPeer(
                            namespace_selector=V1LabelSelector(
                                match_labels={
                                    "kubernetes.io/metadata.name": "paastasvc-some-api"
                                }
                            ),
                            pod_selector=V1LabelSelector(
                                match_labels={
                                    "paasta.yelp.com/service": "some-api",
                                    "paasta.yelp.com/instance": "main",
                                }
                            ),
                        )
                    ],
                    ports=[V1NetworkPolicyPort(protocol="TCP", port=8080)],
                ),
                V1NetworkPolicyIngressRule(
                    _from=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="10.0.0.0/8"))],
                    ports=[V1NetworkPolicyPort(protocol="TCP", port=8080)],
                ),
            ],
        ),
    )
    # owner rules first, then baselines
    assert policy == expected_policy


def test_network_policy_for_service_instance_baselines():
    # An egress-only config still gets the ingress baseline appended
    firewall_config = FirewallConfigDict(
        egress=[FirewallRuleDict(to_services=["some-service"])]
    )
    baseline_ingress = [FirewallRuleDict(from_cidrs=["10.0.0.0/8"])]

    with mock_paasta_service_config_loader(
        {"some-service": [mock_instance_config("main", "paastasvc-some-service")]}
    ):
        policy = network_policy_for_service_instance(
            service="some-service",
            instance="main",
            cluster="fake-cluster",
            namespace="paastasvc-some-service",
            firewall_config=firewall_config,
            baseline_egress=[],
            baseline_ingress=baseline_ingress,
        )

    expected_policy = V1NetworkPolicy(
        api_version="networking.k8s.io/v1",
        kind="NetworkPolicy",
        metadata=V1ObjectMeta(
            name="some-service-main",
            namespace="paastasvc-some-service",
            labels={
                "paasta.yelp.com/managed": "true",
                "paasta.yelp.com/service": "some-service",
                "paasta.yelp.com/instance": "main",
            },
        ),
        spec=V1NetworkPolicySpec(
            pod_selector=V1LabelSelector(
                match_labels={
                    "paasta.yelp.com/service": "some-service",
                    "paasta.yelp.com/instance": "main",
                }
            ),
            policy_types=["Egress", "Ingress"],
            egress=[
                V1NetworkPolicyEgressRule(
                    to=[
                        V1NetworkPolicyPeer(
                            namespace_selector=V1LabelSelector(
                                match_labels={
                                    "kubernetes.io/metadata.name": "paastasvc-some-service"
                                }
                            ),
                            pod_selector=V1LabelSelector(
                                match_labels={"paasta.yelp.com/service": "some-service"}
                            ),
                        )
                    ],
                    ports=[],
                ),
            ],
            ingress=[
                V1NetworkPolicyIngressRule(
                    _from=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="10.0.0.0/8"))],
                    ports=[],
                ),
            ],
        ),
    )
    assert policy == expected_policy


def test_network_policy_for_service_instance_invalid_rule_raises():
    # A destinationless rule triggers the fail-closed guard, which the
    # function wraps in InvalidKubernetesConfig
    with pytest.raises(InvalidKubernetesConfig):
        network_policy_for_service_instance(
            service="some-service",
            instance="main",
            cluster="fake-cluster",
            namespace="paasta",
            firewall_config=FirewallConfigDict(
                egress=[
                    FirewallRuleDict(ports=[FirewallPortDict(protocol="TCP", port=443)])
                ]
            ),
            baseline_egress=[],
            baseline_ingress=[],
        )
