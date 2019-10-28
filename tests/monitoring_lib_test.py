import mock
import pytest

from clusterman.monitoring_lib import get_monitoring_client
from clusterman.monitoring_lib import LogMonitoringClient
from clusterman.monitoring_lib import SignalFXMonitoringClient


@pytest.mark.parametrize('ym', [None, mock.Mock()])
def test_default_monitoring_client(ym):
    with mock.patch('clusterman.monitoring_lib.yelp_meteorite', ym):
        assert get_monitoring_client() == (LogMonitoringClient if not ym else SignalFXMonitoringClient)
