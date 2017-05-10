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

from paasta_tools import list_marathon_service_instances


def test_get_desired_marathon_configs():
    with mock.patch(
        'paasta_tools.list_marathon_service_instances.get_services_for_cluster', autospec=True,
    ) as mock_get_services_for_cluster, mock.patch(
        'paasta_tools.list_marathon_service_instances.load_marathon_service_config', autospec=True,
    ) as mock_load_marathon_service_config, mock.patch(
        'paasta_tools.list_marathon_service_instances.load_system_paasta_config', autospec=True,
    ):
        mock_app_dict = {'id': '/service.instance.git.configs'}
        mock_get_services_for_cluster.return_value = [('service', 'instance')]
        mock_load_marathon_service_config.return_value = mock.MagicMock(
            format_marathon_app_dict=mock.MagicMock(return_value=mock_app_dict),
        )
        assert list_marathon_service_instances.get_desired_marathon_configs(
            '/fake/soa/dir') == {'service.instance.git.configs': mock_app_dict}


def test_get_service_instances_that_need_bouncing():
    with mock.patch(
        'paasta_tools.list_marathon_service_instances.get_desired_marathon_configs', autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks', autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_draining_hosts', autospec=True,
    ):
        mock_get_desired_marathon_configs.return_value = {
            'fake--service.fake--instance.sha.config': {'instances': 5},
            'fake--service2.fake--instance.sha.config': {'instances': 5},
        }
        fake_apps = [
            mock.MagicMock(instances=5, id='/fake--service.fake--instance.sha.config2'),
            mock.MagicMock(instances=5, id='/fake--service2.fake--instance.sha.config'),
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            mock_client, '/fake/soa/dir')) == {'fake_service.fake_instance'}


def test_get_service_instances_that_need_bouncing_two_existing_services():
    with mock.patch(
        'paasta_tools.list_marathon_service_instances.get_desired_marathon_configs', autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks', autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_draining_hosts', autospec=True,
    ):
        mock_get_desired_marathon_configs.return_value = {
            'fake--service.fake--instance.sha.config': {'instances': 5},
        }
        fake_apps = [
            mock.MagicMock(instances=5, id='/fake--service.fake--instance.sha.config'),
            mock.MagicMock(instances=5, id='/fake--service.fake--instance.sha.config2'),
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            mock_client, '/fake/soa/dir')) == {'fake_service.fake_instance'}


def test_get_service_instances_that_need_bouncing_no_difference():
    with mock.patch(
        'paasta_tools.list_marathon_service_instances.get_desired_marathon_configs', autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks', autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_draining_hosts', autospec=True,
    ):
        mock_get_desired_marathon_configs.return_value = {'fake--service.fake--instance.sha.config': {'instances': 5}}
        fake_apps = [mock.MagicMock(instances=5, id='/fake--service.fake--instance.sha.config')]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            mock_client, '/fake/soa/dir')) == set()


def test_get_service_instances_that_need_bouncing_instances_difference():
    with mock.patch(
        'paasta_tools.list_marathon_service_instances.get_desired_marathon_configs', autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks', autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_draining_hosts', autospec=True,
    ):
        mock_get_desired_marathon_configs.return_value = {'fake--service.fake--instance.sha.config': {'instances': 5}}
        fake_apps = [mock.MagicMock(instances=4, id='/fake--service.fake--instance.sha.config')]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            mock_client, '/fake/soa/dir')) == {'fake_service.fake_instance'}


def test_get_service_instances_that_need_bouncing_at_risk():
    with mock.patch(
        'paasta_tools.list_marathon_service_instances.get_desired_marathon_configs', autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks', autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        'paasta_tools.list_marathon_service_instances.get_draining_hosts', autospec=True,
    ):
        mock_get_desired_marathon_configs.return_value = {'fake--service.fake--instance.sha.config': {'instances': 5}}
        fake_apps = [mock.MagicMock(instances=5, id='/fake--service.fake--instance.sha.config')]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        mock_get_num_at_risk_tasks.return_value = 1
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            mock_client, '/fake/soa/dir')) == {'fake_service.fake_instance'}
