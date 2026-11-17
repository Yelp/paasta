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
import re
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
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import get_kubernetes_app_name
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import DEFAULT_SOA_DIR

FIREWALL_RULE_KEYS = {
    "egress": ("to_services", "to_cidrs"),
    "ingress": ("from_services", "from_cidrs"),
}

# Allowlist for to_services/from_services refs: a bare service name or
# service.instance, matching PaaSTA service and instance naming. Validating
# before the PaastaServiceConfigLoader lookup matters because the loader
# builds filesystem paths from the service name.
_SERVICE_REF_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?)?$"
)


def _validate_service_ref(service_ref: str) -> None:
    """
    Validate a service reference before it is used to load config.

    :param service_ref: service name from a to_services/from_services list
    :raises ValueError: if the ref is not a bare or dotted service name
    """
    if not _SERVICE_REF_RE.match(service_ref):
        raise ValueError(f"invalid service reference: {service_ref!r}")


class FirewallPortDict(TypedDict):
    protocol: Literal["TCP", "UDP", "SCTP"]
    port: int


class FirewallRuleDict(TypedDict, total=False):
    to_services: List[str]
    from_services: List[str]
    to_cidrs: List[str]
    from_cidrs: List[str]
    ports: List[FirewallPortDict]


class FirewallConfigDict(TypedDict, total=False):
    egress: List[FirewallRuleDict]
    ingress: List[FirewallRuleDict]


def network_policy_for_service_instance(
    service: str,
    instance: str,
    cluster: str,
    namespace: str,
    firewall_config: FirewallConfigDict,
    baseline_egress: List[FirewallRuleDict],
    baseline_ingress: List[FirewallRuleDict],
) -> V1NetworkPolicy:
    """
    Create NetworkPolicy for service instance from its firewall config.

    :param service: service name
    :param instance: instance name
    :param cluster: cluster the policy is generated for
    :param namespace: namespace to create NetworkPolicy in
    :param firewall_config: parsed contents of `firewall` key
    :param baseline_egress: baseline egress rules (from system config)
    :param baseline_ingress: baseline ingress rules (from system config)
    :return: V1NetworkPolicy object for service instance
    """
    try:
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
                policy_types=["Egress", "Ingress"],
                ingress=[
                    _ingress_rule_for_firewall_rule(rule, cluster)
                    for rule in firewall_config.get("ingress", []) + baseline_ingress
                ],
                egress=[
                    _egress_rule_for_firewall_rule(rule, cluster)
                    for rule in firewall_config.get("egress", []) + baseline_egress
                ],
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


def _peers_for_service_ref(
    service_ref: str, cluster: str, soa_dir: str = DEFAULT_SOA_DIR
) -> List[V1NetworkPolicyPeer]:
    """
    Translate service reference into NetworkPolicyPeers.

    A bare name ("some-service") = all instances of service.
    Dotted name ("some-service.main") = one instance.

    The peer's namespace comes from its instance config, since services can
    override the default with the `namespace` key; a bare name whose
    instances span multiple namespaces produces one peer per namespace.

    :param service_ref: service name from a to_services/from_services list
    :param cluster: cluster the policy is generated for
    :param soa_dir: soa config dir to load the peer service's config from
    :raises ValueError: if the service or named instance is not on the cluster
    :return: NetworkPolicyPeer objects for the service reference
    """
    _validate_service_ref(service_ref)
    service, _, instance = service_ref.partition(".")
    instance_configs = list(
        PaastaServiceConfigLoader(service=service, soa_dir=soa_dir).instance_configs(
            cluster, KubernetesDeploymentConfig
        )
    )
    if not instance_configs:
        raise ValueError(f"no instances of service '{service}' found on {cluster}")
    if instance:
        matching = [c for c in instance_configs if c.get_instance() == instance]
        if not matching:
            raise ValueError(
                f"instance '{instance}' of service '{service}' not found on {cluster}"
            )
        namespaces = [matching[0].get_namespace()]
        # Batch tools like tron label pods with the instance name
        # truncated to fit the 63-char label limit (see tron_tools.py);
        # limit_size_with_hash is a no-op for short names, so this only
        # changes the long-name case
        pod_labels = {
            "paasta.yelp.com/service": service,
            "paasta.yelp.com/instance": limit_size_with_hash(instance),
        }
    else:
        namespaces = sorted({c.get_namespace() for c in instance_configs})
        pod_labels = {"paasta.yelp.com/service": service}
    return [
        V1NetworkPolicyPeer(
            namespace_selector=V1LabelSelector(
                match_labels={"kubernetes.io/metadata.name": namespace}
            ),
            pod_selector=V1LabelSelector(match_labels=pod_labels),
        )
        for namespace in namespaces
    ]


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
            )
        )
    return ports


def _peers_for_firewall_rule(
    direction: str, rule: FirewallRuleDict, cluster: str, soa_dir: str = DEFAULT_SOA_DIR
) -> List[V1NetworkPolicyPeer]:
    """
    Translate a rule's destinations into NetworkPolicyPeers.

    :param direction: "egress" or "ingress"; decides the destination keys
    :param rule: rule from the firewall config
    :param cluster: cluster the policy is generated for
    :param soa_dir: soa config dir to load peer configs from
    :raises ValueError: if the rule has no destinations, which would allow traffic to anywhere
    """
    services_key, cidrs_key = FIREWALL_RULE_KEYS[direction]
    peers = []
    for service_ref in cast(List[str], rule.get(services_key, [])):
        peers.extend(_peers_for_service_ref(service_ref, cluster, soa_dir))
    for cidr in cast(List[str], rule.get(cidrs_key, [])):
        peers.append(_peer_for_cidr(cidr))
    if not peers:
        raise ValueError(
            f"firewall {direction} rule has no destinations, which would allow traffic to anywhere"
        )
    return peers


def _egress_rule_for_firewall_rule(
    rule: FirewallRuleDict, cluster: str, soa_dir: str = DEFAULT_SOA_DIR
) -> V1NetworkPolicyEgressRule:
    """
    Translate firewall rule into V1NetworkPolicyEgressRule.

    :param rule: rule from firewall config
    :param cluster: cluster the policy is generated for
    :return: V1NetworkPolicyEgressRule object for the rule
    """
    return V1NetworkPolicyEgressRule(
        to=_peers_for_firewall_rule("egress", rule, cluster, soa_dir),
        ports=_network_policy_ports(rule),
    )


def _ingress_rule_for_firewall_rule(
    rule: FirewallRuleDict, cluster: str, soa_dir: str = DEFAULT_SOA_DIR
) -> V1NetworkPolicyIngressRule:
    """
    Translate firewall rule into V1NetworkPolicyIngressRule.

    :param rule: rule from firewall config
    :param cluster: cluster the policy is generated for
    :return: V1NetworkPolicyIngressRule object for the rule
    """
    return V1NetworkPolicyIngressRule(
        _from=_peers_for_firewall_rule("ingress", rule, cluster, soa_dir),
        ports=_network_policy_ports(rule),
    )
