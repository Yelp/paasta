#!/usr/bin/env python
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
from mock import Mock
from mock import patch

from paasta_tools import paasta_metastatus
from paasta_tools.metrics.metastatus_lib import ResourceUtilization as RU


def test_get_service_instance_stats():
    # The patch stuff is confusing.
    # Basically we patch the validate_service_instance in the paasta_metastatus module and not the utils module
    instance_config_mock = Mock()
    instance_config_mock.get_gpus.return_value = None
    with patch(
        "paasta_tools.paasta_metastatus.get_instance_config",
        autospec=True,
        return_value=instance_config_mock,
    ):
        stats = paasta_metastatus.get_service_instance_stats(
            "fakeservice", "fakeinstance", "fakecluster"
        )
        assert set(stats.keys()) == {"mem", "cpus", "disk", "gpus"}


def test_fill_table_rows_with_service_instance_stats():
    fake_service_instance_stats = {"mem": 40, "cpus": 0.3, "disk": 1.0, "gpus": 0}
    fake_table_rows = [[]]
    # For reference, ResourceUtilization is (metric, total, free)
    fake_rsrc_utils = [RU("mem", 100, 80), RU("cpus", 100, 50), RU("disk", 20, 15)]
    paasta_metastatus.fill_table_rows_with_service_instance_stats(
        fake_service_instance_stats, fake_rsrc_utils, fake_table_rows
    )
    result_str = fake_table_rows[0][0]
    # Clearly memory is the limiting factor as there is only 80 memory and each service instance takes 40 memory
    assert "2" in result_str
    assert "mem" in result_str
