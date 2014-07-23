import sys
from contextlib import nested

import mock
import pytest

from service_deployment_tools.monitoring.check_synapse_replication import (
    check_replication,
    parse_range,
    run_synapse_check
)


def test_check_replication():
    """Codes and messages should conform to the nagios api and
    the specification of the check_replication function"""

    # replication < 2 => warn
    mock_warn_range = (2, 1000)
    # replication < 1 => critical
    mock_crit_range = (1, 1000)

    code, message = check_replication('foo', 0,
                                      mock_warn_range, mock_crit_range)
    assert code == 2 and 'foo' in message

    code, message = check_replication('foo', 1,
                                      mock_warn_range, mock_crit_range)
    assert code == 1 and 'foo' in message

    code, message = check_replication('bar', 2,
                                      mock_warn_range, mock_crit_range)
    assert code == 0 and 'bar' in message


def test_parse_range():
    range_data = ["0:1", "1:", "100:", "20:30", ":2", ":200"]
    expected_ranges = [
        (0, 1), (1, sys.maxint), (100, sys.maxint), (20, 30), (0, 2), (0, 200)
    ]

    computed_ranges = map(parse_range, range_data)
    assert computed_ranges == expected_ranges


def test_run_synapse_check():
    module = 'service_deployment_tools.monitoring.check_synapse_replication'
    parse_method = module + '.parse_synapse_check_options'
    replication_method = module + '.get_replication_for_services'
    check_replication_method = module + '.check_replication'

    mock_parse_options = mock.Mock()
    mock_parse_options.synapse_host_port = 'foo'
    mock_parse_options.services = ['wat', 'service', 'is']

    mock_replication = {'wat': 2, 'service': 1, 'is': 0}

    for return_value in range(0, 4):
        with nested(
            mock.patch(parse_method, return_value=mock_parse_options),
            mock.patch(replication_method, return_value=mock_replication),
            mock.patch(check_replication_method,
                       return_value=(return_value, 'CHECK'))):
            with pytest.raises(SystemExit) as error:
                run_synapse_check()
            assert error.value.code == return_value

    # Mock check results for those services
    mock_service_status = {'wat': 0, 'service': 1, 'is': 2}

    def mock_check(name, replication, warn, crit):
        return mock_service_status[name], 'CHECK'

    with nested(mock.patch(parse_method, return_value=mock_parse_options),
                mock.patch(replication_method, return_value=mock_replication),
                mock.patch(check_replication_method, new=mock_check)):
        with pytest.raises(SystemExit) as error:
            run_synapse_check()
        assert error.value.code == 2
