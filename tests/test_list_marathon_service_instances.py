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
import contextlib

import mock

from paasta_tools import list_marathon_service_instances


def test_long_job_id_to_short_job_id():
    assert list_marathon_service_instances.long_job_id_to_short_job_id(
        'service.instance.git.config') == 'service.instance'


def list_get_current_apps():
    with contextlib.nested(
        mock.patch('paasta_tools.list_marathon_service_instances.load_marathon_config'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_marathon_client'),
    ) as (
        mock_load_marathon_config,
        mock_get_marathon_client,
    ):
        mock_task = mock.MagicMock(id='/service.instance.git.config')

        mock_load_marathon_config.return_value = mock.MagicMock()
        mock_get_marathon_client.return_value = mock.MagicMock(
            list_apps=mock.MagicMock(return_value=[mock_task]),
        )
        assert list_marathon_service_instances.get_current_apps() == {'/service.instance.git.config': mock_task}


def test_get_desired_marathon_configs():
    with contextlib.nested(
        mock.patch('paasta_tools.list_marathon_service_instances.get_services_for_cluster'),
        mock.patch('paasta_tools.list_marathon_service_instances.load_marathon_service_config'),
        mock.patch('paasta_tools.list_marathon_service_instances.load_system_paasta_config'),
    ) as (
        mock_get_services_for_cluster,
        mock_load_marathon_service_config,
        _,
    ):
        mock_app_dict = {'id': '/service.instance.git.configs'}
        mock_get_services_for_cluster.return_value = [('service', 'instance')]
        mock_load_marathon_service_config.return_value = mock.MagicMock(
            format_marathon_app_dict=mock.MagicMock(return_value=mock_app_dict),
        )
        assert list_marathon_service_instances.get_desired_marathon_configs(
            'fake-cluster', '/fake/soa/dir') == {'service.instance.git.configs': mock_app_dict}


def test_get_service_instances_that_need_bouncing():
    with contextlib.nested(
        mock.patch('paasta_tools.list_marathon_service_instances.get_desired_marathon_configs'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_current_apps'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks'),
    ) as (
        mock_get_desired_marathon_configs,
        mock_get_current_apps,
        mock_get_num_at_risk_tasks,
    ):
        mock_get_desired_marathon_configs.return_value = {
            'fake--service.fake--instance.sha.config': {'instances': 5},
            'fake--service2.fake--instance.sha.config': {'instances': 5},
        }
        mock_get_current_apps.return_value = {
            'fake--service.fake--instance.sha.config2': mock.MagicMock(instances=5),
            'fake--service2.fake--instance.sha.config': mock.MagicMock(instances=5),
        }
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            '/fake/soa/dir')) == {'fake_service.fake_instance'}


def test_get_service_instances_that_need_bouncing_no_difference():
    with contextlib.nested(
        mock.patch('paasta_tools.list_marathon_service_instances.get_desired_marathon_configs'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_current_apps'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks'),
    ) as (
        mock_get_desired_marathon_configs,
        mock_get_current_apps,
        mock_get_num_at_risk_tasks,
    ):
        mock_get_desired_marathon_configs.return_value = {'fake--service.fake--instance.sha.config': {'instances': 5}}
        mock_get_current_apps.return_value = {'fake--service.fake--instance.sha.config': mock.MagicMock(instances=5)}
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing('/fake/soa/dir')) == set()


def test_get_service_instances_that_need_bouncing_instances_difference():
    with contextlib.nested(
        mock.patch('paasta_tools.list_marathon_service_instances.get_desired_marathon_configs'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_current_apps'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks'),
    ) as (
        mock_get_desired_marathon_configs,
        mock_get_current_apps,
        mock_get_num_at_risk_tasks,
    ):
        mock_get_desired_marathon_configs.return_value = {'fake--service.fake--instance.sha.config': {'instances': 5}}
        mock_get_current_apps.return_value = {'fake--service.fake--instance.sha.config': mock.MagicMock(instances=4)}
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            '/fake/soa/dir')) == {'fake_service.fake_instance'}


def test_get_service_instances_that_need_bouncing_at_risk():
    with contextlib.nested(
        mock.patch('paasta_tools.list_marathon_service_instances.get_desired_marathon_configs'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_current_apps'),
        mock.patch('paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks'),
    ) as (
        mock_get_desired_marathon_configs,
        mock_get_current_apps,
        mock_get_num_at_risk_tasks,
    ):
        mock_get_desired_marathon_configs.return_value = {'fake--service.fake--instance.sha.config': {'instances': 5}}
        mock_get_current_apps.return_value = {'fake--service.fake--instance.sha.config': mock.MagicMock(instances=5)}
        mock_get_num_at_risk_tasks.return_value = 1
        assert set(list_marathon_service_instances.get_service_instances_that_need_bouncing(
            '/fake/soa/dir')) == {'fake_service.fake_instance'}
