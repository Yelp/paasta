import mock
import contextlib
from paasta_tools import check_mesos_resource_utilization


def test_check_thresholds_mem_over():
    with contextlib.nested(
        mock.patch('paasta_tools.check_mesos_resource_utilization.fetch_mesos_stats'),
        mock.patch('paasta_tools.check_mesos_resource_utilization.send_event'),
    ) as (
        mock_fetch_mesos_stats,
        mock_send_event,
    ):
        mock_fetch_mesos_stats.return_value = {'master/mem_percent': 100, 'master/cpus_percent': 0}
        actual = check_mesos_resource_utilization.check_thresholds('90')
        assert 'CRITICAL: Memory' in actual
        assert 'OK: CPU' in actual
        mock_send_event.assert_called_once_with(2, mock.ANY)


def test_check_thresholds_cpu_over():
    with contextlib.nested(
        mock.patch('paasta_tools.check_mesos_resource_utilization.fetch_mesos_stats'),
        mock.patch('paasta_tools.check_mesos_resource_utilization.send_event'),
    ) as (
        mock_fetch_mesos_stats,
        mock_send_event,
    ):
        mock_fetch_mesos_stats.return_value = {'master/mem_percent': 10, 'master/cpus_percent': 100}
        actual = check_mesos_resource_utilization.check_thresholds('90')
        assert 'CRITICAL: CPU' in actual
        assert 'OK: Memory' in actual
        mock_send_event.assert_called_once_with(2, mock.ANY)


def test_check_thresholds_ok():
    with contextlib.nested(
        mock.patch('paasta_tools.check_mesos_resource_utilization.fetch_mesos_stats'),
        mock.patch('paasta_tools.check_mesos_resource_utilization.send_event'),
    ) as (
        mock_fetch_mesos_stats,
        mock_send_event,
    ):
        mock_fetch_mesos_stats.return_value = {'master/mem_percent': 10, 'master/cpus_percent': 10}
        actual = check_mesos_resource_utilization.check_thresholds('90')
        assert 'OK: CPU' in actual
        assert 'OK: Memory' in actual
        mock_send_event.assert_called_once_with(0, mock.ANY)
