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

from paasta_tools.firewall_tools import FirewallConfigDict
from paasta_tools.firewall_tools import FirewallPortDict
from paasta_tools.firewall_tools import FirewallRuleDict
from paasta_tools.firewall_tools import _egress_rule_for_firewall_rule
from paasta_tools.firewall_tools import _ingress_rule_for_firewall_rule
from paasta_tools.firewall_tools import _network_policy_ports
from paasta_tools.firewall_tools import _peer_for_cidr
from paasta_tools.firewall_tools import _peers_for_firewall_rule
from paasta_tools.firewall_tools import create_network_policy
from paasta_tools.firewall_tools import network_policy_for_service_instance
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig


def test_create_network_policy():
    mock_client = mock.Mock()
    mock_policy = V1NetworkPolicy()
    create_network_policy(mock_client, mock_policy, "paasta")
    mock_client.networking.create_namespaced_network_policy.assert_called_with(
        namespace="paasta", body=mock_policy
    )


def test_peer_for_cidr():
    peer = _peer_for_cidr("52.94.0.0/16")
    assert peer.ip_block.cidr == "52.94.0.0/16"


def test_peer_for_cidr_invalid():
    with pytest.raises(ValueError):
        _peer_for_cidr("not-a-cidr")


def test_network_policy_ports():
    rule = {"to_cidrs": ["52.94.0.0/16"], "ports": [{"protocol": "TCP", "port": 443}]}
    assert _network_policy_ports(rule) == [
        V1NetworkPolicyPort(protocol="TCP", port=443)
    ]


def test_network_policy_ports_empty():
    assert _network_policy_ports({"to_cidrs": ["52.94.0.0/16"]}) == []


def test_egress_rule_for_firewall_rule():
    rule = {
        "to_cidrs": ["52.94.0.0/16"],
        "ports": [{"protocol": "TCP", "port": 443}],
    }
    egress_rule = _egress_rule_for_firewall_rule(rule)
    assert egress_rule == V1NetworkPolicyEgressRule(
        to=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="52.94.0.0/16"))],
        ports=[V1NetworkPolicyPort(protocol="TCP", port=443)],
    )


def test_ingress_rule_for_firewall_rule():
    rule = {"from_cidrs": ["10.0.0.0/8"]}
    ingress_rule = _ingress_rule_for_firewall_rule(rule)
    assert ingress_rule == V1NetworkPolicyIngressRule(
        _from=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="10.0.0.0/8"))],
        ports=[],
    )


def test_firewall_rule_with_no_destinations_raises():
    # The fail-closed guard: an empty peer list would mean "allow to
    # anywhere", so it must raise instead of building a rule
    with pytest.raises(ValueError):
        _peers_for_firewall_rule(
            "egress", {"ports": [{"protocol": "TCP", "port": 443}]}
        )
    with pytest.raises(ValueError):
        _peers_for_firewall_rule("ingress", {"from_cidrs": []})
    with pytest.raises(ValueError):
        _peers_for_firewall_rule("egress", {})


def test_network_policy_for_service_instance():
    firewall_config = FirewallConfigDict(
        egress=[
            FirewallRuleDict(
                to_cidrs=["52.94.0.0/16"],
                ports=[FirewallPortDict(protocol="TCP", port=443)],
            )
        ],
        ingress=[
            FirewallRuleDict(
                from_cidrs=["10.0.0.0/8"],
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

    policy = network_policy_for_service_instance(
        service="some-service",
        instance="main",
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
                    to=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="52.94.0.0/16"))],
                    ports=[V1NetworkPolicyPort(protocol="TCP", port=443)],
                ),
                V1NetworkPolicyEgressRule(
                    to=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="10.0.0.0/8"))],
                    ports=[V1NetworkPolicyPort(protocol="UDP", port=53)],
                ),
            ],
            ingress=[
                V1NetworkPolicyIngressRule(
                    _from=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="10.0.0.0/8"))],
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
        egress=[FirewallRuleDict(to_cidrs=["52.94.0.0/16"])]
    )
    baseline_ingress = [FirewallRuleDict(from_cidrs=["10.0.0.0/8"])]

    policy = network_policy_for_service_instance(
        service="some-service",
        instance="main",
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
                    to=[V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr="52.94.0.0/16"))],
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
            namespace="paasta",
            firewall_config=FirewallConfigDict(
                egress=[
                    FirewallRuleDict(ports=[FirewallPortDict(protocol="TCP", port=443)])
                ]
            ),
            baseline_egress=[],
            baseline_ingress=[],
        )
