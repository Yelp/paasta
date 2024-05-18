import mock
import pytest

from paasta_tools import firewall
from paasta_tools import iptables
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import NoConfigurationForServiceError


EMPTY_RULE = iptables.Rule(
    protocol="ip",
    src="0.0.0.0/0.0.0.0",
    dst="0.0.0.0/0.0.0.0",
    target=None,
    matches=(),
    target_parameters=(),
)


@pytest.fixture
def service_group():
    return firewall.ServiceGroup(service="my_cool_service", instance="web")


@pytest.yield_fixture
def mock_services_running_here():
    with mock.patch.object(
        firewall,
        "services_running_here",
        autospec=True,
        side_effect=lambda: iter(
            (
                ("example_happyhour", "main", "02:42:a9:fe:00:00", "1.1.1.1"),
                ("example_happyhour", "main", "02:42:a9:fe:00:01", "2.2.2.2"),
                ("example_happyhour", "batch", "02:42:a9:fe:00:02", "3.3.3.3"),
                ("my_cool_service", "web", "02:42:a9:fe:00:03", "4.4.4.4"),
                ("my_cool_service", "web", "02:42:a9:fe:00:04", "5.5.5.5"),
            )
        ),
    ):
        yield


def test_service_group_chain_name(service_group):
    """The chain name must be stable, unique, and short."""
    assert service_group.chain_name == "PAASTA.my_cool_se.f031797563"
    assert len(service_group.chain_name) <= 28


@pytest.yield_fixture
def mock_service_config():
    with mock.patch.object(
        firewall, "get_instance_config", autospec=True
    ) as mock_instance_config, mock.patch.object(
        firewall,
        "get_all_namespaces_for_service",
        autospec=True,
        return_value={"example_happyhour.main": {"proxy_port": "20000"}},
    ), mock.patch.object(
        firewall, "load_system_paasta_config", autospec=True
    ) as mock_system_paasta_config, mock.patch.object(
        firewall, "_synapse_backends", autospec=True
    ) as mock_synapse_backends:

        mock_system_paasta_config.return_value.get_cluster.return_value = "mycluster"

        mock_instance_config.return_value = mock.Mock()
        mock_instance_config.return_value.get_dependencies.return_value = [
            {"well-known": "internet"},
            {"smartstack": "example_happyhour.main"},
            {"cidr": "169.229.226.0/24"},
            {"cidr": "8.8.8.8", "port": 53},
            {"cidr": "8.8.4.4", "port": "1024:65535"},
        ]
        mock_instance_config.return_value.get_outbound_firewall.return_value = "monitor"

        mock_synapse_backends.return_value = [
            {"host": "1.2.3.4", "port": 123},
            {"host": "5.6.7.8", "port": 567},
        ]
        yield mock_instance_config


def test_service_group_rules_no_dependencies(mock_service_config, service_group):
    mock_service_config.return_value.get_dependencies.return_value = None
    assert service_group.get_rules(
        DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
    ) == (
        EMPTY_RULE._replace(
            target="LOG",
            matches=(("limit", (("limit", ("1/sec",)), ("limit-burst", ("1",)))),),
            target_parameters=(("log-prefix", ("paasta.my_cool_service ",)),),
        ),
        EMPTY_RULE._replace(target="PAASTA-COMMON"),
    )


def test_service_group_rules_monitor(mock_service_config, service_group):
    assert service_group.get_rules(
        DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
    ) == (
        EMPTY_RULE._replace(
            target="LOG",
            matches=(("limit", (("limit", ("1/sec",)), ("limit-burst", ("1",)))),),
            target_parameters=(("log-prefix", ("paasta.my_cool_service ",)),),
        ),
        EMPTY_RULE._replace(target="PAASTA-COMMON"),
        EMPTY_RULE._replace(target="PAASTA-INTERNET"),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="1.2.3.4/255.255.255.255",
            matches=(
                ("comment", (("comment", ("backend example_happyhour.main",)),)),
                ("tcp", (("dport", ("123",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="5.6.7.8/255.255.255.255",
            matches=(
                ("comment", (("comment", ("backend example_happyhour.main",)),)),
                ("tcp", (("dport", ("567",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="169.254.255.254/255.255.255.255",
            matches=(
                ("comment", (("comment", ("proxy_port example_happyhour.main",)),)),
                ("tcp", (("dport", ("20000",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="ip",
            target="ACCEPT",
            dst="169.229.226.0/255.255.255.0",
            matches=(("comment", (("comment", ("allow 169.229.226.0/24:*",)),)),),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="8.8.8.8/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.8.8/32:53",)),)),
                ("tcp", (("dport", ("53",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="udp",
            target="ACCEPT",
            dst="8.8.8.8/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.8.8/32:53",)),)),
                ("udp", (("dport", ("53",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="8.8.4.4/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.4.4/32:1024:65535",)),)),
                ("tcp", (("dport", ("1024:65535",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="udp",
            target="ACCEPT",
            dst="8.8.4.4/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.4.4/32:1024:65535",)),)),
                ("udp", (("dport", ("1024:65535",)),)),
            ),
        ),
    )


def test_service_group_rules_block(mock_service_config, service_group):
    mock_service_config.return_value.get_outbound_firewall.return_value = "block"
    assert service_group.get_rules(
        DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
    ) == (
        EMPTY_RULE._replace(
            target="REJECT",
            matches=(),
            target_parameters=(("reject-with", ("icmp-port-unreachable",)),),
        ),
        EMPTY_RULE._replace(
            target="LOG",
            matches=(("limit", (("limit", ("1/sec",)), ("limit-burst", ("1",)))),),
            target_parameters=(("log-prefix", ("paasta.my_cool_service ",)),),
        ),
        EMPTY_RULE._replace(target="PAASTA-COMMON"),
        EMPTY_RULE._replace(target="PAASTA-INTERNET"),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="1.2.3.4/255.255.255.255",
            matches=(
                ("comment", (("comment", ("backend example_happyhour.main",)),)),
                ("tcp", (("dport", ("123",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="5.6.7.8/255.255.255.255",
            matches=(
                ("comment", (("comment", ("backend example_happyhour.main",)),)),
                ("tcp", (("dport", ("567",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="169.254.255.254/255.255.255.255",
            matches=(
                ("comment", (("comment", ("proxy_port example_happyhour.main",)),)),
                ("tcp", (("dport", ("20000",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="ip",
            target="ACCEPT",
            dst="169.229.226.0/255.255.255.0",
            matches=(("comment", (("comment", ("allow 169.229.226.0/24:*",)),)),),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="8.8.8.8/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.8.8/32:53",)),)),
                ("tcp", (("dport", ("53",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="udp",
            target="ACCEPT",
            dst="8.8.8.8/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.8.8/32:53",)),)),
                ("udp", (("dport", ("53",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="8.8.4.4/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.4.4/32:1024:65535",)),)),
                ("tcp", (("dport", ("1024:65535",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            protocol="udp",
            target="ACCEPT",
            dst="8.8.4.4/255.255.255.255",
            matches=(
                ("comment", (("comment", ("allow 8.8.4.4/32:1024:65535",)),)),
                ("udp", (("dport", ("1024:65535",)),)),
            ),
        ),
    )


def test_service_group_rules_synapse_backend_error(mock_service_config, service_group):
    mock_service_config.return_value.get_dependencies.return_value = [
        {"smartstack": "example_happyhour.main"}
    ]
    firewall._synapse_backends.side_effect = IOError("Error loading file")
    assert service_group.get_rules(
        DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
    ) == (
        EMPTY_RULE._replace(
            target="LOG",
            matches=(("limit", (("limit", ("1/sec",)), ("limit-burst", ("1",)))),),
            target_parameters=(("log-prefix", ("paasta.my_cool_service ",)),),
        ),
        EMPTY_RULE._replace(target="PAASTA-COMMON"),
        EMPTY_RULE._replace(
            protocol="tcp",
            target="ACCEPT",
            dst="169.254.255.254/255.255.255.255",
            matches=(
                ("comment", (("comment", ("proxy_port example_happyhour.main",)),)),
                ("tcp", (("dport", ("20000",)),)),
            ),
        ),
    )


def test_service_group_rules_empty_when_invalid_instance_type(
    service_group, mock_service_config
):
    with mock.patch.object(
        firewall, "get_instance_config", side_effect=NotImplementedError()
    ):
        assert (
            service_group.get_rules(
                DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
            )
            == ()
        )


def test_service_group_rules_empty_when_service_is_deleted(
    service_group, mock_service_config
):
    """A deleted service which still has running containers shouldn't cause exceptions."""
    with mock.patch.object(
        firewall, "get_instance_config", side_effect=NoConfigurationForServiceError()
    ):
        assert (
            service_group.get_rules(
                DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
            )
            == ()
        )


@mock.patch.object(iptables, "ensure_chain", autospec=True)
@mock.patch.object(iptables, "reorder_chain", autospec=True)
def test_service_group_update_rules(reorder_mock, ensure_mock, service_group):
    with mock.patch.object(
        type(service_group), "get_rules", return_value=mock.sentinel.RULES
    ):
        service_group.update_rules(
            DEFAULT_SOA_DIR, firewall.DEFAULT_SYNAPSE_SERVICE_DIR
        )
    ensure_mock.assert_called_once_with(service_group.chain_name, mock.sentinel.RULES)
    reorder_mock.assert_called_once_with(service_group.chain_name)


def test_ensure_internet_chain():
    with mock.patch.object(iptables, "ensure_chain", autospec=True) as m:
        firewall._ensure_internet_chain()
    (call,) = m.call_args_list
    args, _ = call
    assert args[0] == "PAASTA-INTERNET"
    assert args[1] == (
        EMPTY_RULE._replace(target="ACCEPT"),
        EMPTY_RULE._replace(dst="127.0.0.0/255.0.0.0", target="RETURN"),
        EMPTY_RULE._replace(dst="10.0.0.0/255.0.0.0", target="RETURN"),
        EMPTY_RULE._replace(dst="172.16.0.0/255.240.0.0", target="RETURN"),
        EMPTY_RULE._replace(dst="192.168.0.0/255.255.0.0", target="RETURN"),
        EMPTY_RULE._replace(dst="169.254.0.0/255.255.0.0", target="RETURN"),
    )


@pytest.fixture
def mock_active_service_groups():
    groups = {
        firewall.ServiceGroup("cool_service", "main"): {
            "02:42:a9:fe:00:02",
            "fe:a3:a3:da:2d:51",
            "fe:a3:a3:da:2d:50",
        },
        firewall.ServiceGroup("cool_service", "main"): {"fe:a3:a3:da:2d:40"},
        firewall.ServiceGroup("dumb_service", "other"): {
            "fe:a3:a3:da:2d:30",
            "fe:a3:a3:da:2d:31",
        },
    }
    return groups


@mock.patch.object(firewall.ServiceGroup, "get_rules", return_value=mock.sentinel.RULES)
@mock.patch.object(iptables, "reorder_chain", autospec=True)
@mock.patch.object(iptables, "ensure_chain", autospec=True)
@mock.patch.object(iptables, "insert_rule", autospec=True)
def test_prepare_new_container(
    insert_rule_mock, ensure_chain_mock, reorder_chain_mock, get_rules_mock
):
    firewall.prepare_new_container(
        DEFAULT_SOA_DIR,
        firewall.DEFAULT_SYNAPSE_SERVICE_DIR,
        "myservice",
        "myinstance",
        "00:00:00:00:00:00",
    )
    assert ensure_chain_mock.mock_calls == [
        mock.call("PAASTA-DNS", mock.ANY),
        mock.call("PAASTA-INTERNET", mock.ANY),
        mock.call("PAASTA-COMMON", mock.ANY),
        mock.call("PAASTA.myservice.7e8522249a", mock.sentinel.RULES),
    ]
    assert reorder_chain_mock.mock_calls == [mock.call("PAASTA.myservice.7e8522249a")]
    assert insert_rule_mock.mock_calls == [
        mock.call(
            "PAASTA",
            EMPTY_RULE._replace(
                target="PAASTA.myservice.7e8522249a",
                matches=(("mac", (("mac-source", ("00:00:00:00:00:00",)),)),),
            ),
        )
    ]


@pytest.mark.parametrize(
    ("resolv_conf", "expected"),
    (
        (
            "nameserver         8.8.8.8\n"
            "nameserver\t8.8.4.4\n"
            "nameserver 169.254.255.254\n",
            ("8.8.8.8", "8.8.4.4", "169.254.255.254"),
        ),
        (
            "#nameserver 8.8.8.8\n"
            "nameserver\n"
            "nameserver 8.8.4.4\n"
            "nameserver 2001:4860:4860::8888\n",
            ("8.8.4.4",),
        ),
        (
            "domain yelpcorp.com\n" "nameserver 8.8.4.4\n" "search a b c d\n",
            ("8.8.4.4",),
        ),
    ),
)
def test_dns_servers(tmpdir, resolv_conf, expected):
    path = tmpdir.join("resolv.conf")
    path.write(resolv_conf)
    with mock.patch.object(firewall, "RESOLV_CONF", path.strpath):
        assert tuple(firewall._dns_servers()) == expected


def test_ensure_dns_chain(tmpdir):
    path = tmpdir.join("resolv.conf")
    path.write("nameserver 8.8.8.8\n" "nameserver 8.8.4.4\n")
    with mock.patch.object(
        iptables, "ensure_chain", autospec=True
    ) as m, mock.patch.object(firewall, "RESOLV_CONF", path.strpath):
        firewall._ensure_dns_chain()
    (call,) = m.call_args_list
    args, _ = call
    assert args[0] == "PAASTA-DNS"
    assert args[1] == (
        EMPTY_RULE._replace(
            dst="8.8.8.8/255.255.255.255",
            target="ACCEPT",
            protocol="udp",
            matches=(("udp", (("dport", ("53",)),)),),
        ),
        EMPTY_RULE._replace(
            dst="8.8.8.8/255.255.255.255",
            target="ACCEPT",
            protocol="tcp",
            matches=(("tcp", (("dport", ("53",)),)),),
        ),
        EMPTY_RULE._replace(
            dst="8.8.4.4/255.255.255.255",
            target="ACCEPT",
            protocol="udp",
            matches=(("udp", (("dport", ("53",)),)),),
        ),
        EMPTY_RULE._replace(
            dst="8.8.4.4/255.255.255.255",
            target="ACCEPT",
            protocol="tcp",
            matches=(("tcp", (("dport", ("53",)),)),),
        ),
    )


def test_ensure_common_chain():
    with mock.patch.object(iptables, "ensure_chain", autospec=True) as m:
        firewall._ensure_common_chain()
    (call,) = m.call_args_list
    args, _ = call
    assert args[0] == "PAASTA-COMMON"
    assert args[1] == (
        EMPTY_RULE._replace(
            target="ACCEPT", matches=(("conntrack", (("ctstate", ("ESTABLISHED",)),)),)
        ),
        EMPTY_RULE._replace(
            dst="169.254.255.254/255.255.255.255",
            target="ACCEPT",
            protocol="tcp",
            matches=(
                ("comment", (("comment", ("scribed",)),)),
                ("tcp", (("dport", ("1463",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            dst="169.254.255.254/255.255.255.255",
            target="ACCEPT",
            protocol="udp",
            matches=(
                ("comment", (("comment", ("metrics-relay",)),)),
                ("udp", (("dport", ("8125",)),)),
            ),
        ),
        EMPTY_RULE._replace(
            dst="169.254.255.254/255.255.255.255",
            target="ACCEPT",
            protocol="tcp",
            matches=(
                ("comment", (("comment", ("sensu",)),)),
                ("tcp", (("dport", ("3030",)),)),
            ),
        ),
        EMPTY_RULE._replace(target="PAASTA-DNS"),
    )
