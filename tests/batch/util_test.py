import mock
import pytest
from botocore.exceptions import ClientError

try:
    from clusterman.batch.util import suppress_request_limit_exceeded
except ImportError:
    pytest.mark.skip('Could not import the batch; are you in a Yelp-y environment?')


@mock.patch('clusterman.batch.util.get_monitoring_client')
@mock.patch('clusterman.batch.util.logger')
def test_suppress_rle(mock_logger, mock_monitoring_client):
    mock_counter = mock_monitoring_client.return_value.create_counter.return_value
    with suppress_request_limit_exceeded():
        raise ClientError({'Error': {'Code': 'RequestLimitExceeded'}}, 'foo')
    assert mock_logger.warning.call_count == 1
    assert mock_monitoring_client.return_value.create_counter.call_count == 1
    assert mock_counter.count.call_count == 1


@mock.patch('clusterman.batch.util.get_monitoring_client')
@mock.patch('clusterman.batch.util.logger')
def test_ignore_other_exceptions(mock_logger, mock_monitoring_client):
    mock_counter = mock_monitoring_client.return_value.create_counter.return_value
    with suppress_request_limit_exceeded(), pytest.raises(Exception):
        raise Exception('foo')
    assert mock_logger.warning.call_count == 0
    assert mock_monitoring_client.return_value.create_counter.call_count == 0
    assert mock_counter.count.call_count == 0
