# Copyright 2015-2016 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from paasta_tools.monitoring.config_providers import extract_classic_monitoring_info
from paasta_tools.monitoring.config_providers import extract_monitoring_info


def test_extract_classic_monitoring_info():
    test_dict = {
        'monitoring': {
            "team": "testing",
            "notification_email": "testing",
            "service_type": "testing",
            "runbook": "testing",
            "tip": "testing",
            "page": "testing",
            "alert_after": "testing",
            "realert_every": "testing",
            "extra": "testing",
            "this_is_extra": "so extra",
            "also_extra": "moar extra",
        }
    }

    extra_keys = set(["this_is_extra", "also_extra"])
    monitoring_keys = set(test_dict['monitoring'].keys()) - extra_keys

    extracted = extract_classic_monitoring_info(test_dict)
    for key in monitoring_keys:
        # Should not mutate the original
        assert key in test_dict['monitoring'] and key in extracted

    assert 'extra' in extracted
    assert extracted['extra']['this_is_extra'] == 'so extra'
    assert extracted['extra']['also_extra'] == 'moar extra'


def test_extract_monitoring_info():
    module = 'paasta_tools.monitoring.config_providers'
    extract_method = module + '.extract_classic_monitoring_info'

    with mock.patch(extract_method, return_value=-1, autospec=True):
        assert extract_monitoring_info('classic', {}) == -1

    with pytest.raises(Exception):
        extract_monitoring_info('foobar', {})
