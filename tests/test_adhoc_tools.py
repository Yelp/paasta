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

import contextlib

import mock

from paasta_tools import adhoc_tools
from paasta_tools.utils import DeploymentsJson
from paasta_tools.utils import NoConfigurationForServiceError


def test_get_default_interactive_config():
    with contextlib.nested(
        mock.patch('paasta_tools.adhoc_tools.load_adhoc_job_config', autospec=True),
    ) as (
        mock_load_adhoc_job_config,
    ):
        mock_load_adhoc_job_config.return_value = adhoc_tools.AdhocJobConfig(
            service='fake_service',
            instance='interactive',
            cluster='fake_cluster',
            config_dict={},
            branch_dict={'deploy_group': 'fake_deploy_group'},
        )
        result = adhoc_tools.get_default_interactive_config(
            'fake_serivce',
            'fake_cluster',
            '/fake/soa/dir',
            load_deployments=False,
        )
        assert result.get_cpus() == 4
        assert result.get_mem() == 10240
        assert result.get_disk() == 1024


def test_get_default_interactive_config_reads_from_tty():
    with contextlib.nested(
        mock.patch('paasta_tools.adhoc_tools.prompt_pick_one', autospec=True),
        mock.patch('paasta_tools.adhoc_tools.load_adhoc_job_config', autospec=True),
        mock.patch('paasta_tools.adhoc_tools.load_v2_deployments_json', autospec=True),
    ) as (
        mock_prompt_pick_one,
        mock_load_adhoc_job_config,
        mock_load_deployments_json,
    ):
        mock_prompt_pick_one.return_value = 'fake_deploygroup'
        mock_load_adhoc_job_config.side_effect = NoConfigurationForServiceError
        mock_load_deployments_json.return_value = DeploymentsJson({
            'deployments': {
                'fake_deploygroup': {
                    'docker_image': mock.sentinel.docker_image,
                },
            },
        })
        result = adhoc_tools.get_default_interactive_config(
            'fake_serivce',
            'fake_cluster',
            '/fake/soa/dir',
            load_deployments=True,
        )
        assert result.get_deploy_group() == 'fake_deploygroup'
        assert result.get_docker_image() == mock.sentinel.docker_image
