# Copyright 2015-2017 Yelp Inc.
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
from unittest.mock import patch

import asynctest

from paasta_tools.monitoring import check_mesos_outdated_tasks


@patch(
    "paasta_tools.monitoring.check_mesos_outdated_tasks.get_mesos_master", autospec=True
)
def test_check_mesos_tasks(mock_get_mesos_master):
    mock_get_mesos_master.return_value.state = asynctest.CoroutineMock(
        func=asynctest.CoroutineMock(),
        return_value={
            "slaves": [
                {
                    "id": "4abbb181-fd06-4729-815b-6b55cebdf8ee-S2",
                    "hostname": "mesos-slave1.example.com",
                }
            ],
            "frameworks": [
                {
                    "name": "marathon",
                    "tasks": [
                        {
                            "state": "TASK_RUNNING",
                            "name": "service.instance.gitlast_SHA.config3f15fefe",
                            "slave_id": "4abbb181-fd06-4729-815b-6b55cebdf8ee-S2",
                            "statuses": [
                                {
                                    "state": "TASK_RUNNING",
                                    "timestamp": 1509392500.9267,
                                    "container_status": {
                                        "container_id": {
                                            "value": "a69b426d-f283-4287-9bee-6b8811386e1a"
                                        }
                                    },
                                }
                            ],
                        },
                        {
                            "state": "TASK_RUNNING",
                            "name": "service.instance.gitold_SHA.config3f15fefe",
                            "slave_id": "4abbb181-fd06-4729-815b-6b55cebdf8ee-S2",
                            "statuses": [
                                {
                                    "state": "TASK_RUNNING",
                                    "timestamp": 1509342500.9267,
                                    "container_status": {
                                        "container_id": {
                                            "value": "a69b426d-f283-4287-9bee-6b8811386e1b"
                                        }
                                    },
                                }
                            ],
                        },
                    ],
                }
            ],
        },
    )
    output, remedy = check_mesos_outdated_tasks.check_mesos_tasks()
    assert len(output) == 1
    assert "a69b426d-f283-4287-9bee-6b8811386e1b" in output[0]
    assert "old_SHA" in output[0]
