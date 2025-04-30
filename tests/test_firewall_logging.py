import mock
import pytest

from paasta_tools import firewall_logging


@mock.patch.object(firewall_logging, "lookup_service_instance_by_ip")
def test_syslog_to_paasta_log(mock_lookup_service_instance_by_ip, mock_log):
    syslog_data = fake_syslog_data("my-hostname", SRC="1.2.3.4")
    mock_lookup_service_instance_by_ip.return_value = ("myservice", "myinstance")

    firewall_logging.syslog_to_paasta_log(syslog_data, "my-cluster")

    assert mock_log.mock_calls == [
        mock.call(
            service="myservice",
            component="security",
            level="debug",
            cluster="my-cluster",
            instance="myinstance",
            line="my-hostname: my-prefix IN=docker0 SRC=1.2.3.4",
        )
    ]


@mock.patch.object(firewall_logging, "lookup_service_instance_by_ip")
def test_syslog_to_paasta_log_no_container(
    mock_lookup_service_instance_by_ip, mock_log
):
    syslog_data = fake_syslog_data("my-hostname", SRC="1.2.3.4")
    mock_lookup_service_instance_by_ip.return_value = (None, None)
    firewall_logging.syslog_to_paasta_log(syslog_data, "my-cluster")
    assert mock_log.mock_calls == []


def test_parse_syslog_undecodable():
    assert (
        firewall_logging.parse_syslog(b"<4>Jun  6 07:52:38 myhost \xba~\xa6r") is None
    )


@pytest.mark.parametrize(
    "syslog_data",
    [
        "<4>Jun  6 07:52:38 myhost someothermessage: hello world",
        "<4>Jun  6 07:52:38 myhost kernel: hello world",
        "<4>Jun  6 07:52:38 myhost kernel [0.0]: hello world",
    ],
)
@mock.patch.object(firewall_logging, "lookup_service_instance_by_ip")
def test_syslog_to_paasta_log_bad_message(
    mock_lookup_service_instance_by_ip, syslog_data, mock_log
):
    firewall_logging.syslog_to_paasta_log(syslog_data.encode(), "my-cluster")
    assert mock_lookup_service_instance_by_ip.mock_calls == []
    assert mock_log.mock_calls == []


@mock.patch.object(
    firewall_logging,
    "services_running_here",
    return_value=[
        ("service1", "instance1", "00:00:00:00:00", "1.1.1.1"),
        ("service1", "instance2", "00:00:00:00:00", "2.2.2.2"),
    ],
)
@mock.patch.object(firewall_logging, "log")
def test_lookup_service_instance_by_ip(my_mock_log, mock_services_running_here):
    assert firewall_logging.lookup_service_instance_by_ip("1.1.1.1") == (
        "service1",
        "instance1",
    )
    assert firewall_logging.lookup_service_instance_by_ip("2.2.2.2") == (
        "service1",
        "instance2",
    )
    assert firewall_logging.lookup_service_instance_by_ip("3.3.3.3") == (None, None)
    assert my_mock_log.info.mock_calls == [
        mock.call("Unable to find container for ip 3.3.3.3")
    ]


def test_parse_args():
    assert firewall_logging.parse_args([]).listen_host == "127.0.0.1"
    assert firewall_logging.parse_args([]).listen_port == 1516
    assert firewall_logging.parse_args([]).verbose is False
    assert firewall_logging.parse_args(["-v"]).verbose is True
    assert firewall_logging.parse_args(["-p", "1234"]).listen_port == 1234
    assert firewall_logging.parse_args(["-l", "0.0.0.0"]).listen_host == "0.0.0.0"


@mock.patch.object(firewall_logging, "logging")
def test_setup_logging(logging_mock):
    firewall_logging.setup_logging(True)
    assert logging_mock.basicConfig.mock_calls == [mock.call(level=logging_mock.DEBUG)]


@mock.patch.object(firewall_logging, "MultiUDPServer")
def test_run_server(udpserver_mock):
    firewall_logging.run_server("myhost", 1234)
    assert udpserver_mock.mock_calls == [
        mock.call(("myhost", 1234), firewall_logging.SyslogUDPHandler),
        mock.call().serve_forever(),
    ]


@mock.patch.object(firewall_logging, "logging")
@mock.patch.object(firewall_logging, "MultiUDPServer")
@mock.patch.object(firewall_logging, "signal")
def test_main_single_worker(signal_mock, udpserver_mock, logging_mock):
    firewall_logging.main(["-w", "1"])
    assert logging_mock.basicConfig.mock_calls == [
        mock.call(level=logging_mock.WARNING)
    ]
    assert udpserver_mock.mock_calls == [
        mock.call(("127.0.0.1", 1516), firewall_logging.SyslogUDPHandler),
        mock.call().serve_forever(),
    ]


@mock.patch.object(firewall_logging, "logging")
@mock.patch.object(firewall_logging, "MultiUDPServer")
@mock.patch.object(firewall_logging.os, "fork", return_value=0)
@mock.patch.object(firewall_logging, "signal")
def test_main_two_workers(signal_mock, fork_mock, udpserver_mock, logging_mock):
    firewall_logging.main(["-w", "2"])
    assert logging_mock.basicConfig.mock_calls == [
        mock.call(level=logging_mock.WARNING)
    ]
    assert udpserver_mock.mock_calls == [
        mock.call(("127.0.0.1", 1516), firewall_logging.SyslogUDPHandler),
        mock.call().serve_forever(),
        mock.call(("127.0.0.1", 1516), firewall_logging.SyslogUDPHandler),
        mock.call().serve_forever(),
    ]


def fake_syslog_data(hostname, **kwargs):
    prefix = (
        f"<4>Jun  6 07:52:38 {hostname} kernel: [2736265.340132] my-prefix IN=docker0 "
    )
    fields_str = " ".join(map("=".join, kwargs.items()))
    return (prefix + fields_str + " \n").encode()


@pytest.fixture
def mock_log():
    with mock.patch.object(firewall_logging, "_log", autospec=True) as mock_log:
        yield mock_log
