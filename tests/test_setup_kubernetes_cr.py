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
from paasta_tools.utils import SystemPaastaConfig


def test_main():
    with mock.patch(
        "paasta_tools.setup_kubernetes_cr.KubeClient", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_cr.setup_all_custom_resources", autospec=True
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
        "paasta_tools.setup_kubernetes_cr.ensure_namespace", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_cr.load_all_configs", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_cr.setup_custom_resources", autospec=True
    ) as mock_setup, mock.patch(
        "paasta_tools.setup_kubernetes_cr.load_custom_resource_definitions",
        autospec=True,
    ) as mock_load_custom_resources:
        mock_system_config = mock.Mock(
            get_cluster=mock.Mock(return_value="westeros-prod")
        )
        # if some CRs setup okay should return True
        mock_setup.side_effect = [True, False]

        mock_client = mock.Mock()
        flink_crd = mock.Mock()
        flink_crd.spec.names = mock.Mock(plural="flinkclusters", kind="FlinkCluster")
        cassandra_crd = mock.Mock()
        cassandra_crd.spec.names = mock.Mock(
            plural="cassandraclusters", kind="CassandraCluster"
        )
        mock_client.apiextensions_v1_beta1.list_custom_resource_definition.return_value = mock.Mock(
            items=[flink_crd, cassandra_crd]
        )

        mock_client.apiextensions.list_custom_resource_definition.return_value = (
            mock.Mock(items=[])
        )

        custom_resource_definitions = [
            mock.Mock(
                kube_kind=mock.Mock(plural="flinkclusters", singular="FlinkCluster")
            ),
            mock.Mock(
                kube_kind=mock.Mock(
                    plural="cassandraclusters", singular="CassandraCluster"
                )
            ),
        ]

        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client,
            "/nail/soa",
            mock_system_config,
            custom_resource_definitions=custom_resource_definitions,
        )

        mock_load_custom_resources.return_value = [
            mock.Mock(plural="flinks"),
            mock.Mock(plural="cassandraclusters"),
        ]
        mock_setup.side_effect = [True, True]
        mock_system_config = mock.Mock(
            get_cluster=mock.Mock(return_value="westeros-prod")
        )
        # if all CRs setup okay should return True
        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client,
            "/nail/soa",
            mock_system_config,
            custom_resource_definitions=custom_resource_definitions,
        )

        mock_load_custom_resources.return_value = []
        mock_system_config = mock.Mock(
            get_cluster=mock.Mock(return_value="westeros-prod")
        )
        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client, "/nail/soa", mock_system_config, custom_resource_definitions=[]
        )

        mock_setup.side_effect = []
        # if no CRs setup should return True
        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client, "/nail/soa", mock_system_config, custom_resource_definitions=[]
        )

        mock_setup.side_effect = [False, False]
        # if all CRs setup fail should return False
        assert setup_kubernetes_cr.setup_all_custom_resources(
            mock_client, "/nail/soa", mock_system_config, custom_resource_definitions=[]
        )


def test_setup_all_custom_resources_flink():
    with mock.patch(
        "paasta_tools.setup_kubernetes_cr.ensure_namespace", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_cr.load_all_configs", autospec=True
    ) as mock_load_all, mock.patch(
        "paasta_tools.setup_kubernetes_cr.setup_custom_resources", autospec=True
    ) as mock_setup:
        mock_system_config = mock.Mock(
            get_cluster=mock.Mock(return_value="westeros-prod")
        )
        # if some CRs setup okay should return True
        mock_setup.side_effect = [True, False]

        mock_client = mock.Mock()
        flink_crd = mock.Mock()
        flink_crd.spec.names = mock.Mock(
            singular="flink", plural="flinks", kind="flink"
        )

        mock_client.apiextensions.list_custom_resource_definition.return_value = (
            mock.Mock(items=[flink_crd])
        )

        mock_client.apiextensions_v1_beta1.list_custom_resource_definition.return_value = mock.Mock(
            items=[]
        )

        custom_resource_definitions = [
            mock.Mock(
                kube_kind=mock.Mock(plural="flinks", singular="flink", kind="Flink")
            ),
        ]

        setup_kubernetes_cr.setup_all_custom_resources(
            mock_client,
            "/nail/soa",
            mock_system_config,
            custom_resource_definitions=custom_resource_definitions,
        )

        assert mock_load_all.called


def test_load_all_configs():
    with mock.patch(
        "paasta_tools.utils.load_service_instance_configs",
        autospec=True,
    ) as mock_load_configs, mock.patch("os.listdir", autospec=True) as mock_oslist:
        mock_oslist.return_value = ["kurupt", "mc"]
        ret = setup_kubernetes_cr.load_all_configs(
            cluster="westeros-prod", file_prefix="thing", soa_dir="/nail/soa"
        )
        mock_load_configs.assert_has_calls(
            [
                mock.call("mc", "thing", "westeros-prod", soa_dir="/nail/soa"),
                mock.call("kurupt", "thing", "westeros-prod", soa_dir="/nail/soa"),
            ],
            any_order=True,
        )
        assert "kurupt" in ret.keys()
        assert "mc" in ret.keys()


def test_load_all_flink_configs():
    with mock.patch(
        "paasta_tools.utils.load_service_instance_configs",
        autospec=True,
    ) as mock_load_configs, mock.patch("os.listdir", autospec=True) as mock_oslist:
        mock_oslist.return_value = ["kurupt", "mc"]
        mock_load_configs.side_effect = [
            {
                "foo": {"mem": 2},
                "bar": {"cpus": 3},
            },
            {
                "bar": {"cpus": 3},
            },
        ]
        ret = setup_kubernetes_cr.load_all_configs(
            cluster="westeros-prod", file_prefix="flink", soa_dir="/nail/soa"
        )

        mock_load_configs.assert_has_calls(
            [
                mock.call("kurupt", "flink", "westeros-prod", soa_dir="/nail/soa"),
                mock.call("mc", "flink", "westeros-prod", soa_dir="/nail/soa"),
            ],
            any_order=True,
        )

        assert "kurupt" in ret.keys()
        assert "mc" in ret.keys()

        assert ret["kurupt"] == {"foo": {"mem": 2}, "bar": {"cpus": 3}}
        assert ret["mc"] == {"bar": {"cpus": 3}}


def test_setup_custom_resources():
    with mock.patch(
        "paasta_tools.setup_kubernetes_cr.list_custom_resources", autospec=True
    ) as mock_list_cr, mock.patch(
        "paasta_tools.setup_kubernetes_cr.reconcile_kubernetes_resource", autospec=True
    ) as mock_reconcile_kubernetes_resource:
        mock_client = mock.Mock()
        mock_kind = mock.Mock()
        mock_crd = mock.Mock()
        assert setup_kubernetes_cr.setup_custom_resources(
            kube_client=mock_client,
            kind=mock_kind,
            version="v1",
            config_dicts={},
            group="yelp.com",
            cluster="mycluster",
            crd=mock_crd,
        )

        mock_reconcile_kubernetes_resource.side_effect = [True, False]
        assert not setup_kubernetes_cr.setup_custom_resources(
            kube_client=mock_client,
            kind=mock_kind,
            version="v1",
            config_dicts={"kurupt": "something", "mc": "another"},
            group="yelp.com",
            cluster="mycluster",
            crd=mock_crd,
        )

        mock_reconcile_kubernetes_resource.side_effect = [True, True]
        assert setup_kubernetes_cr.setup_custom_resources(
            kube_client=mock_client,
            kind=mock_kind,
            version="v1",
            config_dicts={"kurupt": "something", "mc": "another"},
            group="yelp.com",
            cluster="mycluster",
            crd=mock_crd,
        )
        mock_reconcile_kubernetes_resource.assert_has_calls(
            [
                mock.call(
                    kube_client=mock_client,
                    service="kurupt",
                    instance_configs="something",
                    cluster="mycluster",
                    instance=None,
                    kind=mock_kind,
                    custom_resources=mock_list_cr.return_value,
                    version="v1",
                    group="yelp.com",
                    crd=mock_crd,
                ),
                mock.call(
                    kube_client=mock_client,
                    service="mc",
                    instance_configs="another",
                    cluster="mycluster",
                    instance=None,
                    kind=mock_kind,
                    custom_resources=mock_list_cr.return_value,
                    version="v1",
                    group="yelp.com",
                    crd=mock_crd,
                ),
            ]
        )


def test_format_custom_resource():
    with mock.patch(
        "paasta_tools.setup_kubernetes_cr.get_config_hash", autospec=True
    ) as mock_get_config_hash, mock.patch(
        "paasta_tools.setup_kubernetes_cr.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig(
            {"dashboard_links": {}, "cr_owners": {"flink": "stream-processing"}}, ""
        )
        expected = {
            "apiVersion": "yelp.com/v1",
            "kind": "flink",
            "metadata": {
                "name": "kurupt--fm-radio--station",
                "namespace": "paasta-flinks",
                "labels": {
                    "yelp.com/paasta_service": "kurupt_fm",
                    "yelp.com/paasta_instance": "radio_station",
                    "yelp.com/paasta_cluster": "mycluster",
                    "yelp.com/owner": "stream-processing",
                    "yelp.com/paasta_config_sha": mock_get_config_hash.return_value,
                    "paasta.yelp.com/service": "kurupt_fm",
                    "paasta.yelp.com/instance": "radio_station",
                    "paasta.yelp.com/cluster": "mycluster",
                    "paasta.yelp.com/config_sha": mock_get_config_hash.return_value,
                    "paasta.yelp.com/git_sha": "gitsha",
                },
                "annotations": {
                    "yelp.com/desired_state": "running",
                    "paasta.yelp.com/desired_state": "running",
                    "paasta.yelp.com/dashboard_base_url": "http://flink.k8s.mycluster.paasta:31080/",
                },
            },
            "spec": {"dummy": "conf"},
        }
        assert (
            setup_kubernetes_cr.format_custom_resource(
                instance_config={"dummy": "conf"},
                service="kurupt_fm",
                instance="radio_station",
                cluster="mycluster",
                kind="flink",
                version="v1",
                group="yelp.com",
                namespace="paasta-flinks",
                git_sha="gitsha",
                is_eks=False,
            )
            == expected
        )


def test_paasta_config_flink_dashboard_base_url():
    with mock.patch(
        "paasta_tools.setup_kubernetes_cr.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig(
            {
                "dashboard_links": {
                    "mycluster": {"Flink": "http://flink.mycluster.paasta"}
                }
            },
            "",
        )
        expected = "http://flink.mycluster.paasta/"
        assert (
            setup_kubernetes_cr.get_dashboard_base_url(
                kind="flink", cluster="mycluster", is_eks=False
            )
            == expected
        )


@mock.patch(
    "paasta_tools.setup_kubernetes_cr.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_reconcile_kubernetes_resource(mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS):
    with mock.patch(
        "paasta_tools.setup_kubernetes_cr.format_custom_resource", autospec=True
    ) as mock_format_custom_resource, mock.patch(
        "paasta_tools.setup_kubernetes_cr.create_custom_resource", autospec=True
    ) as mock_create_custom_resource, mock.patch(
        "paasta_tools.setup_kubernetes_cr.update_custom_resource", autospec=True
    ) as mock_update_custom_resource:
        mock_kind = mock.Mock(singular="flink", plural="flinks")
        mock_custom_resources = [
            KubeCustomResource(
                service="kurupt",
                instance="fm",
                config_sha="conf123",
                git_sha="git123",
                kind="flink",
                name="foo",
                namespace="paasta-flinks",
            )
        ]
        mock_client = mock.Mock()
        # no instances, do nothing
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service="mc",
            instance_configs={},
            cluster="mycluster",
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version="v1",
            group="yelp.com",
            crd=mock.Mock(),
        )
        assert not mock_create_custom_resource.called
        assert not mock_update_custom_resource.called

        # instance up to date, do nothing
        mock_format_custom_resource.return_value = {
            "metadata": {
                "labels": {
                    "yelp.com/paasta_config_sha": "conf123",
                    "paasta.yelp.com/config_sha": "conf123",
                    "paasta.yelp.com/git_sha": "git123",
                },
                "name": "foo",
                "namespace": "paasta-flinks",
            }
        }
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service="kurupt",
            instance_configs={"fm": {"some": "config"}},
            cluster="cluster",
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version="v1",
            group="yelp.com",
            crd=mock.Mock(),
        )
        assert not mock_create_custom_resource.called
        assert not mock_update_custom_resource.called

        # instance diff config, update
        mock_format_custom_resource.return_value = {
            "metadata": {
                "labels": {"paasta.yelp.com/config_sha": "conf456"},
                "name": "foo",
                "namespace": "paasta-flinks",
            }
        }
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service="kurupt",
            instance_configs={"fm": {"some": "config"}},
            cluster="mycluster",
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version="v1",
            group="yelp.com",
            crd=mock.Mock(),
        )
        assert not mock_create_custom_resource.called
        mock_update_custom_resource.assert_called_with(
            kube_client=mock_client,
            name="kurupt-fm",
            version="v1",
            kind=mock_kind,
            formatted_resource=mock_format_custom_resource.return_value,
            group="yelp.com",
        )

        # instance not exist, create
        assert setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service="mc",
            instance_configs={"grindah": {"some": "conf"}},
            cluster="mycluster",
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version="v1",
            group="yelp.com",
            crd=mock.Mock(),
        )
        mock_create_custom_resource.assert_called_with(
            kube_client=mock_client,
            version="v1",
            kind=mock_kind,
            formatted_resource=mock_format_custom_resource.return_value,
            group="yelp.com",
        )

        # instance not exist, create but error with k8s
        mock_create_custom_resource.side_effect = Exception
        assert not setup_kubernetes_cr.reconcile_kubernetes_resource(
            kube_client=mock_client,
            service="mc",
            instance_configs={"grindah": {"some": "conf"}},
            cluster="mycluster",
            custom_resources=mock_custom_resources,
            kind=mock_kind,
            version="v1",
            group="yelp.com",
            crd=mock.Mock(),
        )
        mock_create_custom_resource.assert_called_with(
            kube_client=mock_client,
            version="v1",
            kind=mock_kind,
            formatted_resource=mock_format_custom_resource.return_value,
            group="yelp.com",
        )
