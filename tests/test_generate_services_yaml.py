# Copyright 2015 Yelp Inc.
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

from paasta_tools import generate_services_yaml


MOCK_NAMESPACES = [
    ('foo.main', {'proxy_port': 1024}),
    ('bar.canary', {'proxy_port': 1025}),
]


def test_generate_configuration():
    expected = {
        'foo.main': {
            'host': '169.254.255.254',
            'port': 1024
        },
        'bar.canary': {
            'host': '169.254.255.254',
            'port': 1025
        }
    }

    with mock.patch('paasta_tools.generate_services_yaml.get_all_namespaces',
                    return_value=MOCK_NAMESPACES):
        actual = generate_services_yaml.generate_configuration()

    assert expected == actual
