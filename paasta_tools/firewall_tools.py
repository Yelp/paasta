# Copyright 2015-2026 Yelp Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import ipaddress
from typing import List
from typing import Literal
from typing import cast

from kubernetes.client import V1IPBlock
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1NetworkPolicy
from kubernetes.client import V1NetworkPolicyEgressRule
from kubernetes.client import V1NetworkPolicyIngressRule
from kubernetes.client import V1NetworkPolicyPeer
from kubernetes.client import V1NetworkPolicyPort
from kubernetes.client import V1NetworkPolicySpec
from kubernetes.client import V1ObjectMeta
from mypy_extensions import TypedDict

from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import get_kubernetes_app_name

FIREWALL_RULE_KEYS = {
    "egress": ("to_cidrs"),
    "ingress": ("from_cidrs"),
}


class FirewallPortDict(TypedDict, total=False):
    protocol: Literal["TCP", "UDP", "SCTP"]
    port: int
    endPort: int


class FirewallRuleDict(TypedDict, total=False):
    to_cidrs: List[str]
    from_cidrs: List[str]
    ports: List[FirewallPortDict]


class FirewallConfigDict(TypedDict, total=False):
    egress: List[FirewallRuleDict]
    ingress: List[FirewallRuleDict]


def network_policy_for_service_instance(
    service: str,
    instance: str,
    namespace: str,
    firewall_config: FirewallConfigDict,
    baseline_egress: List[FirewallRuleDict],
    baseline_ingress: List[FirewallRuleDict],
) -> V1NetworkPolicy:
    """
    Create NetworkPolicy for service instance from its firewall config.

    :param service: service name
    :param instance: instance name
    :param namespace: namespace to create NetworkPolicy in
    :param firewall_config: parsed contents of `firewall` key
    :param baseline_egress: baseline egress rules (from system config)
    :param baseline_ingress: baseline ingress rules (from system config)
    :return: V1NetworkPolicy object for service instance
    """
    try:
        # Only constrain directions that have rules: an empty ingress/egress
        # list with the direction in policyTypes means deny-all, so a config
        # with only egress rules must not drag in deny-all ingress
        ingress_rules = [
            _ingress_rule_for_firewall_rule(rule)
            for rule in firewall_config.get("ingress", []) + baseline_ingress
        ]
        egress_rules = [
            _egress_rule_for_firewall_rule(rule)
            for rule in firewall_config.get("egress", []) + baseline_egress
        ]
        network_policy = V1NetworkPolicy(
            api_version="networking.k8s.io/v1",
            kind="NetworkPolicy",
            metadata=V1ObjectMeta(
                name=get_kubernetes_app_name(service, instance),
                namespace=namespace,
                labels={
                    "paasta.yelp.com/managed": "true",
                    "paasta.yelp.com/service": service,
                    "paasta.yelp.com/instance": instance,
                },
            ),
            spec=V1NetworkPolicySpec(
                pod_selector=V1LabelSelector(
                    match_labels={
                        "paasta.yelp.com/service": service,
                        "paasta.yelp.com/instance": instance,
                    }
                ),
                policy_types=["Egress", "Ingress"]
                if (egress_rules and ingress_rules)
                else (["Egress"] if egress_rules else ["Ingress"]),
                ingress=ingress_rules or None,
                egress=egress_rules or None,
            ),
        )
    except Exception as e:
        raise InvalidKubernetesConfig(e, service, instance)

    return network_policy


def create_network_policy(
    kube_client: KubeClient,
    network_policy: V1NetworkPolicy,
    namespace: str,
) -> None:
    """
    Create k8s NetworkPolicy in specified namespace.

    :param kube_client: k8s client
    :param network_policy: V1NetworkPolicy object to create
    :param namespace: Namespace to create NetworkPolicy in
    """
    return kube_client.networking.create_namespaced_network_policy(
        namespace=namespace, body=network_policy
    )


def _peer_for_cidr(cidr: str) -> V1NetworkPolicyPeer:
    """
    Translate a CIDR block into a NetworkPolicyPeer.

    :param cidr: CIDR block from a to_cidrs/from_cidrs list
    :raises ValueError: if the CIDR is invalid
    :return: V1NetworkPolicyPeer object for CIDR block
    """
    ipaddress.ip_network(cidr)  # will raise ValueError if bad CIDR
    return V1NetworkPolicyPeer(ip_block=V1IPBlock(cidr=cidr))


def _network_policy_ports(rule: FirewallRuleDict) -> List[V1NetworkPolicyPort]:
    """
    Translate rule's ports list into V1NetworkPolicyPort objects.

    No ports = all ports, returns empty list.

    :param rule: rule from the firewall config
    :return: List of V1NetworkPolicyPort objects for the rule
    """
    ports = []
    for port in rule.get("ports", []):
        ports.append(
            V1NetworkPolicyPort(
                protocol=port["protocol"],
                port=port["port"],
                end_port=port.get("endPort"),
            )
        )
    return ports


def _peers_for_firewall_rule(
    direction: str, rule: FirewallRuleDict
) -> List[V1NetworkPolicyPeer]:
    """
    Translate a rule's destinations into NetworkPolicyPeers.

    :param direction: "egress" or "ingress"; decides the destination key
    :param rule: rule from the firewall config
    :raises ValueError: if the rule has no destinations, which would allow traffic to anywhere
    """
    cidrs_key = FIREWALL_RULE_KEYS[direction]
    cidrs = cast(List[str], rule.get(cidrs_key, []))
    if not cidrs:
        raise ValueError(
            f"firewall {direction} rule has no destinations, which would allow traffic to anywhere"
        )
    return [_peer_for_cidr(cidr) for cidr in cidrs]


def _egress_rule_for_firewall_rule(rule: FirewallRuleDict) -> V1NetworkPolicyEgressRule:
    """
    Translate firewall rule into V1NetworkPolicyEgressRule.

    :param rule: rule from firewall config
    :return: V1NetworkPolicyEgressRule object for the rule
    """
    return V1NetworkPolicyEgressRule(
        to=_peers_for_firewall_rule("egress", rule),
        ports=_network_policy_ports(rule),
    )


def _ingress_rule_for_firewall_rule(
    rule: FirewallRuleDict,
) -> V1NetworkPolicyIngressRule:
    """
    Translate firewall rule into V1NetworkPolicyIngressRule.

    :param rule: rule from firewall config
    :return: V1NetworkPolicyIngressRule object for the rule
    """
    return V1NetworkPolicyIngressRule(
        _from=_peers_for_firewall_rule("ingress", rule),
        ports=_network_policy_ports(rule),
    )
