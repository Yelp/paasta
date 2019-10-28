# Copyright 2019 Yelp Inc.
#
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
import mock
import pytest

from clusterman.monitoring_lib import get_monitoring_client
from clusterman.monitoring_lib import LogMonitoringClient
from clusterman.monitoring_lib import SignalFXMonitoringClient


@pytest.mark.parametrize('ym', [None, mock.Mock()])
def test_default_monitoring_client(ym):
    with mock.patch('clusterman.monitoring_lib.yelp_meteorite', ym):
        assert get_monitoring_client() == (LogMonitoringClient if not ym else SignalFXMonitoringClient)
