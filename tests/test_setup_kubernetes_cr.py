# Copyright 2015-2018 Yelp Inc.
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

from paasta_tools import setup_kubernetes_cr
from paasta_tools.kubernetes_tools import KubeCustomResource


def test_load_custom_resources():
    mock_resources = [{
        'version': 'v1',
        'kube_kind': {'plural': 'FlinkClusters', 'singular': 'flinkcluster'},
        'file_prefix': 'flinkcluster',
        'group': 'yelp.com',
    }]
    mock_config = mock.Mock(get_kubernetes_custom_resources=mock.Mock(return_value=mock_resources))
    assert setup_kubernetes_cr.load_custom_resources(mock_config) == [setup_kubernetes_cr.CustomResource(
        version='v1',
        kube_kind=setup_kubernetes_cr.KubeKind(plural='FlinkClusters', singular='flinkcluster'),
        file_prefix='flinkcluster',
        group='yelp.com',
    )]


def test_main():
    with mock.patch(
        'paasta_tools.setup_kubernetes_cr.KubeClient', autospec=True,
    ), mock.patch(
        'paasta_tools.setup_kubernetes_cr.setup_all_custom_resources', autospec=True,
    ) as mock_setup:
        mock_setup.return_value = True
        with pytest.raises(SystemExit) as e:
            setup_kubernetes_cr.main()
            assert e.value.code == 0

        mock_setup.return_value = False
        with pytest.raises(SystemExit) as e:
            setup_kubernetes_cr.main()
            assert e.value.code == 1


def test_setup_all_custom_resources():
    with mock.patch(
        'paasta_tools.setup_kubernetes_cr.ensure_namespace', autospec=True,
    ), mock.patch(
        'paasta_tools.setup_kubernetes_cr.load_all_configs', autospec=True,
    ), mock.patch(
        'paasta_tools.setup_kubernetes_cr.setup_custom_resources', autospec=True,
    ) as mock_setup, mock.patch(
        'paasta_tools.setup_kubernetes_cr.load_custom_resources', autospec=True,
    ) as mock_load_custom_resources:
        mock_system_config = mock.Mock(get_cluster=mock.Mock(return_value='westeros-prod'))
        mock_setup.side_effect = [True, False]

        mock_client = mock.Mock()
        flink_crd = mock.Mock()
        flink_crd.spec.names = mock.Mock(plural='flinkclusters', kind='FlinkCluster')
        cassandra_crd = mock.Mock()
        cassandra_crd.spec.names = mock.Mock(plural='cassandraclusters', kind='CassandraCluster')
        mock_client.apiextensions.list_custom_resource_definition.return_value = mock.Mock(
            items=[flink_crd, cassandra_crd],
        )

        custom_resources = [
            mock.Mock(kube_kind=mock.Mock(plural='flinkclusters', singular='FlinkCluster')),
            mock.Mock(kube_kind=mock.Mock(plural='cassandraclusters', singular='CassandraCluster')),
        ]

        assert not setup_kubernetes_cr.setup_all_custom_resources(
            mock_client, '/nail/soa', mock_system_config, custom_resources=custom_resources,
        )

        mock_load_custom_resources.return_value = [
            mock.Mock(plural='flinkclusters'), mock.Mock(plural='cassandraclusters'),
        ]
        mock_setup.side_effect = [True, True]
        mock_system_config = mock.Mock(get_cluster=mock.Mock(return_value='westeros-prod'))
        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client, '/nail/soa', mock_system_config, custom_resources=custom_resources,
        )

        mock_load_custom_resources.return_value = []
        mock_system_config = mock.Mock(get_cluster=mock.Mock(return_value='westeros-prod'))
        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client, '/nail/soa', mock_system_config, custom_resources=[],
        )


def test_load_all_configs():
    with mock.patch(
        'paasta_tools.setup_kubernetes_cr.service_configuration_lib.read_extra_service_information', autospec=True,
    ) as mock_read_info, mock.patch(
        'os.listdir', autospec=True,
    ) as mock_oslist:
        mock_oslist.return_value = ['kurupt', 'mc']
        ret = setup_kubernetes_cr.load_all_configs(
            cluster='westeros-prod',
            file_prefix='thing',
            soa_dir='/nail/soa',
        )
        mock_read_info.assert_has_calls(
            [
                mock.call('mc', 'thing-westeros-prod', soa_dir='/nail/soa'),
                mock.call('kurupt', 'thing-westeros-prod', soa_dir='/nail/soa'),
            ], any_order=True,
        )
        assert 'kurupt' in ret.keys()
        assert 'mc' in ret.keys()


def test_setup_custom_resources():
    with mock.patch(
        'paasta_tools.setup_kubernetes_cr.list_custom_resources', autospec=True,
    ) as mock_list_cr, mock.patch(
        'paasta_tools.setup_kubernetes_cr.reconcile_kubernetes_resource', autospec=True,
    ) as mock_reconcile_kubernetes_resource:
        mock_client = mock.Mock()
        mock_kind = mock.Mock()
        assert setup_kubernetes_cr.setup_custom_resources(
            kube_client=mock_client,
            kind=mock_kind,
            version='v1',
            config_dicts={},
            group='yelp.com',
            cluster='mycluster',
        )

        mock_reconcile_kubernetes_resource.side_effect = [True, False]
        assert not setup_kubernetes_cr.setup_custom_resources(
            kube_client=mock_client,
            kind=mock_kind,
            version='v1',
            config_dicts={'kurupt': 'something', 'mc': 'another'},
            group='yelp.com',
            cluster='mycluster',
        )

        mock_reconcile_kubernetes_resource.side_effect = [True, True]
        assert setup_kubernetes_cr.setup_custom_resources(
            kube_client=mock_client,
            kind=mock_kind,
            version='v1',
            config_dicts={'kurupt': 'something', 'mc': 'another'},
            group='yelp.com',
            cluster='mycluster',
        )
        mock_reconcile_kubernetes_resource.assert_has_calls([
            mock.call(
                kube_client=mock_client,
                service='kurupt',
                instance_configs='something',
                cluster='mycluster',
                instance=None,
                kind=mock_kind,
                custom_resources=mock_list_cr.return_value,
                version='v1',
                group='yelp.com',
            ),
            mock.call(
                kube_client=mock_client,
                service='mc',
                instance_configs='another',
                cluster='mycluster',
                instance=None,
                kind=mock_kind,
                custom_resources=mock_list_cr.return_value,
                version='v1',
                group='yelp.com',
            ),
        ])


def test_format_custom_resource():
    with mock.patch(
        'paasta_tools.setup_kubernetes_cr.get_config_hash', autospec=True,
    ) as mock_get_config_hash:
        expected = {
            'apiVersion': 'yelp.com/v1',
            'kind': 'flinkcluster',
            'metadata': {
                'name': 'kurupt--fm-radio--station',
                'labels': {
                    'yelp.com/paasta_service': 'kurupt_fm',
                    'yelp.com/paasta_instance': 'radio_station',
                    'yelp.com/paasta_cluster': 'mycluster',
                    'yelp.com/paasta_config_sha': mock_get_config_hash.return_value,
                },
                'annotations': {
                    'yelp.com/desired_state': 'running',
                },
            },
            'spec': {'dummy': 'conf'},
        }
        assert setup_kubernetes_cr.format_custom_resource(
            instance_config={'dummy': 'conf'},
            service='kurupt_fm',
            instance='radio_station',
            cluster='mycluster',
            kind='flinkcluster',
            version='v1',
            group='yelp.com',
        ) == expected


def test_reconcile_kubernetes_resource():
    with mock.patch(
        'paasta_tools.setup_kubernetes_cr.format_custom_resource', autospec=True,
    ) as mock_format_custom_resource, mock.patch(
        'paasta_tools.setup_kubernetes_cr.create_custom_resource', autospec=True,
    ) as mock_create_custom_resource, mock.patch(
        'paasta_tools.setup_kubernetes_cr.update_custom_resource', autospec=True,
    ) as mock_update_custom_resource:
        mock_kind = mock.Mock(singular='flinkcluster')
        mock_custom_resources = [
            KubeCustomResource(
                service='kurupt',
                instance='fm',
                config_sha='conf123',
                kind='flinkcluster',
            ),
        ]
        mock_client = mock.Mock()
        # no instances, do nothing
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service='mc',
            instance_configs={},
            cluster='mycluster',
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version='v1',
            group='yelp.com',
        )
        assert not mock_create_custom_resource.called
        assert not mock_update_custom_resource.called

        # instance up to date, do nothing
        mock_format_custom_resource.return_value = {
            'metadata': {
                'labels': {
                    'yelp.com/paasta_config_sha': 'conf123',
                },
            },
        }
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service='kurupt',
            instance_configs={'fm': {'some': 'config'}},
            cluster='cluster',
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version='v1',
            group='yelp.com',
        )
        assert not mock_create_custom_resource.called
        assert not mock_update_custom_resource.called

        # instance diff config, update
        mock_format_custom_resource.return_value = {
            'metadata': {
                'labels': {
                    'yelp.com/paasta_config_sha': 'conf456',
                },
            },
        }
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service='kurupt',
            instance_configs={'fm': {'some': 'config'}},
            cluster='mycluster',
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version='v1',
            group='yelp.com',
        )
        assert not mock_create_custom_resource.called
        mock_update_custom_resource.assert_called_with(
            kube_client=mock_client,
            name='kurupt-fm',
            version='v1',
            kind=mock_kind,
            formatted_resource=mock_format_custom_resource.return_value,
            group='yelp.com',
        )

        # instance not exist, create
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service='mc',
            instance_configs={'grindah': {'some': 'conf'}},
            cluster='mycluster',
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version='v1',
            group='yelp.com',
        )
        mock_create_custom_resource.assert_called_with(
            kube_client=mock_client,
            version='v1',
            kind=mock_kind,
            formatted_resource=mock_format_custom_resource.return_value,
            group='yelp.com',
        )

        # instance not exist, create but error with k8s
        mock_create_custom_resource.side_effect = Exception
        assert not setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service='mc',
            instance_configs={'grindah': {'some': 'conf'}},
            cluster='mycluster',
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version='v1',
            group='yelp.com',
        )
        mock_create_custom_resource.assert_called_with(
            kube_client=mock_client,
            version='v1',
            kind=mock_kind,
            formatted_resource=mock_format_custom_resource.return_value,
            group='yelp.com',
        )
