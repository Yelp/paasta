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
from paasta_tools.kubernetes_tools import limit_size_with_hash


def test_create_network_policy():
    mock_client = mock.Mock()
    mock_policy = V1NetworkPolicy()
    create_network_policy(mock_client, mock_policy, "paasta")
    mock_client.networking.create_namespaced_network_policy.assert_called_with(
        namespace="paasta", body=mock_policy
    )


def _fake_instance_config(instance: str, namespace: str) -> mock.Mock:
    config = mock.Mock(spec=EksDeploymentConfig)
    config.get_instance.return_value = instance
    config.get_namespace.return_value = namespace
    return config


def _patch_peer_loader(configs_by_service):
    def make_loader(service=None, soa_dir=None):
        loader = mock.Mock()
        loader.instance_configs.return_value = configs_by_service.get(service, [])
        return loader

    return mock.patch(
        "paasta_tools.firewall_tools.PaastaServiceConfigLoader",
        side_effect=make_loader,
        autospec=True,
    )


def test_peers_for_service_ref_bare_name():
    # A bare name means all instances: service label only, no instance label
    with _patch_peer_loader(
        {
            "some-service": [
                _fake_instance_config("main", "paastasvc-some-service"),
                _fake_instance_config("worker", "paastasvc-some-service"),
            ]
        }
    ):
        peers = _peers_for_service_ref("some-service", "fake-cluster")
    assert len(peers) == 1
    assert peers[0].namespace_selector.match_labels == {
        "kubernetes.io/metadata.name": "paastasvc-some-service"
    }
    assert peers[0].pod_selector.match_labels == {
        "paasta.yelp.com/service": "some-service"
    }


def test_peers_for_service_ref_dotted_name():
    # A dotted name means one instance: both labels, raw service name
    with _patch_peer_loader(
        {
            "some-service": [
                _fake_instance_config("main", "paastasvc-some-service"),
                _fake_instance_config("worker", "paastasvc-some-service"),
            ]
        }
    ):
        peers = _peers_for_service_ref("some-service.main", "fake-cluster")
    assert len(peers) == 1
    assert peers[0].namespace_selector.match_labels == {
        "kubernetes.io/metadata.name": "paastasvc-some-service"
    }
    assert peers[0].pod_selector.match_labels == {
        "paasta.yelp.com/service": "some-service",
        "paasta.yelp.com/instance": "main",
    }


def test_peers_for_service_ref_custom_namespace():
    # The namespace comes from the peer's instance config, not the
    # paastasvc- convention: services can override it
    with _patch_peer_loader(
        {"some-service": [_fake_instance_config("main", "custom-namespace")]}
    ):
        peers = _peers_for_service_ref("some-service.main", "fake-cluster")
    assert peers[0].namespace_selector.match_labels == {
        "kubernetes.io/metadata.name": "custom-namespace"
    }


def test_peers_for_service_ref_truncated_instance_label():
    # Batch tools (tron) label pods with the instance name truncated to
    # the 63-char label limit plus a hash suffix; the peer must match
    long_instance = "a" * 70
    with _patch_peer_loader(
        {
            "some-service": [
                _fake_instance_config(long_instance, "paastasvc-some-service")
            ]
        }
    ):
        peers = _peers_for_service_ref(f"some-service.{long_instance}", "fake-cluster")
    assert peers[0].pod_selector.match_labels == {
        "paasta.yelp.com/service": "some-service",
        "paasta.yelp.com/instance": limit_size_with_hash(long_instance),
    }


def test_peers_for_service_ref_instances_span_namespaces():
    # A bare name whose instances live in different namespaces produces
    # one peer per namespace
    with _patch_peer_loader(
        {
            "some-service": [
                _fake_instance_config("main", "paastasvc-some-service"),
                _fake_instance_config("worker", "custom-namespace"),
            ]
        }
    ):
        peers = _peers_for_service_ref("some-service", "fake-cluster")
    assert len(peers) == 2
    namespaces = {
        p.namespace_selector.match_labels["kubernetes.io/metadata.name"] for p in peers
    }
    assert namespaces == {"paastasvc-some-service", "custom-namespace"}


def test_peers_for_service_ref_instance_not_found():
    with _patch_peer_loader(
        {"some-service": [_fake_instance_config("main", "paastasvc-some-service")]}
    ):
        with pytest.raises(ValueError):
            _peers_for_service_ref("some-service.worker", "fake-cluster")


def test_peers_for_service_ref_service_not_on_cluster():
    with _patch_peer_loader({"some-service": []}):
        with pytest.raises(ValueError):
            _peers_for_service_ref("some-service", "fake-cluster")


def test_peers_for_service_ref_underscores():
    # Pod label uses the raw service name even when the namespace is
    # sanitised
    with _patch_peer_loader(
        {
            "service_with_underscores": [
                _fake_instance_config("main", "paastasvc-service--with--underscores")
            ]
        }
    ):
        peers = _peers_for_service_ref("service_with_underscores.main", "fake-cluster")
    assert peers[0].pod_selector.match_labels == {
        "paasta.yelp.com/service": "service_with_underscores",
        "paasta.yelp.com/instance": "main",
    }


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
    with _patch_peer_loader(
        {"some-service": [_fake_instance_config("main", "paastasvc-some-service")]}
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
    with _patch_peer_loader(
        {"some-api": [_fake_instance_config("main", "paastasvc-some-api")]}
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

    with _patch_peer_loader(
        {
            "some-service": [_fake_instance_config("main", "paastasvc-some-service")],
            "some-api": [_fake_instance_config("main", "paastasvc-some-api")],
        }
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

    with _patch_peer_loader(
        {"some-service": [_fake_instance_config("main", "paastasvc-some-service")]}
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
