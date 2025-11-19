# Copyright 2015-2019 Yelp Inc.
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

import paasta_tools.flink_tools as flink_tools
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flink_tools import FlinkDeploymentConfigDict


def test_get_flink_ingress_url_root():
    assert (
        flink_tools.get_flink_ingress_url_root("mycluster", False)
        == "http://flink.k8s.mycluster.paasta:31080/"
    )


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_error(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.ok = False
    mock_response.status_code = 401
    mock_response.reason = "Unauthorized"
    mock_response.text = "401 Authorization Required"

    service = "kurupt"
    instance = "main"
    response = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "overview"
    )

    assert response == {
        "status": 401,
        "error": "Unauthorized",
        "text": "401 Authorization Required",
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_overview(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "taskmanagers": 5,
        "slots-total": 25,
        "slots-available": 3,
        "jobs-running": 1,
        "jobs-finished": 0,
        "jobs-cancelled": 0,
        "jobs-failed": 0,
    }

    service = "kurupt"
    instance = "main"
    overview = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "overview"
    )

    assert overview == {
        "taskmanagers": 5,
        "slots-total": 25,
        "slots-available": 3,
        "jobs-running": 1,
        "jobs-finished": 0,
        "jobs-cancelled": 0,
        "jobs-failed": 0,
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_config(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "refresh-interval": 3000,
        "timezone-name": "Coordinated Universal Time",
        "timezone-offset": 0,
        "flink-version": "1.13.5",
        "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
        "features": {"web-submit": True},
    }

    service = "kurupt"
    instance = "main"
    config = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "config"
    )

    assert config == {
        "flink-version": "1.13.5",
        "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_list_jobs(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
    }

    service = "kurupt"
    instance = "main"
    jobs = flink_tools.curl_flink_endpoint(flink_tools.cr_id(service, instance), "jobs")

    assert jobs == {
        "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_get_job_details(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
        "name": "beam_happyhour.main.test_job",
        "isStoppable": False,
        "state": "RUNNING",
        "start-time": 1655053223341,
        "end-time": -1,
        "duration": 855872054,
        "maxParallelism": -1,
        "now": 1655909095395,
        "timestamps": {
            "CREATED": 1655053223735,
            "SUSPENDED": 0,
            "FAILING": 0,
            "FINISHED": 0,
            "FAILED": 0,
            "RUNNING": 1655842396454,
            "CANCELLING": 0,
            "RESTARTING": 1655842393301,
            "RECONCILING": 0,
            "CANCELED": 0,
            "INITIALIZING": 1655053223341,
        },
    }

    service = "kurupt"
    instance = "main"
    job = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "jobs/4210f0646f5c9ce1db0b3e5ae4372b82"
    )

    assert job == {
        "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
        "state": "RUNNING",
        "name": "beam_happyhour.main.test_job",
        "start-time": 1655053223341,
    }


def test_get_flink_jobmanager_overview():
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value='{"taskmanagers":10,"slots-total":10,"flink-version":"1.6.4","flink-commit":"6241481"}',
    ) as mock_dashboard_get:
        cluster = "mycluster"
        cr_name = "kurupt--fm-7c7b459d59"
        overview = flink_tools.get_flink_jobmanager_overview(cr_name, cluster, False)
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path="overview", is_eks=False
        )
        assert overview == {
            "taskmanagers": 10,
            "slots-total": 10,
            "flink-version": "1.6.4",
            "flink-commit": "6241481",
        }


class TestGetFlinkPoolFromFlinkDeploymentConfig:
    def test_explicit_spot_false(self):
        # When spot is explicitly set to False, should return "flink"
        config_dict = FlinkDeploymentConfigDict({"spot": False})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink"

    def test_explicit_spot_true(self):
        # When spot is explicitly set to True, should return "flink-spot"
        config_dict = FlinkDeploymentConfigDict({"spot": True})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

    def test_spot_not_set(self):
        # When spot is not set, should default to "flink-spot"
        config_dict = FlinkDeploymentConfigDict({"some_other_key": "value"})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

    def test_empty_config_dict(self):
        # When config_dict is empty (but not None), should default to "flink-spot"
        config_dict = FlinkDeploymentConfigDict({})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

    def test_non_bool_spot_value(self):
        # When spot has a non-boolean value like a string, it should treat it as non-False
        config_dict = FlinkDeploymentConfigDict({"spot": "some_string"})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

        # Test with a numeric value
        config_dict = FlinkDeploymentConfigDict({"spot": 0})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"


class TestSafeStr:
    def test_safe_str_with_string(self):
        assert flink_tools._safe_str("hello") == "hello"

    def test_safe_str_with_int(self):
        assert flink_tools._safe_str(123) == "123"

    def test_safe_str_with_float(self):
        assert flink_tools._safe_str(2.0) == "2.0"

    def test_safe_str_with_none(self):
        assert flink_tools._safe_str(None) is None

    def test_safe_str_with_bool(self):
        assert flink_tools._safe_str(True) == "True"
        assert flink_tools._safe_str(False) == "False"


class TestGetSqlclientJobConfig:
    @mock.patch("paasta_tools.flink_tools.os.path.exists", autospec=True)
    @mock.patch("service_configuration_lib.read_yaml_file", autospec=True)
    @mock.patch("paasta_tools.utils.load_system_paasta_config", autospec=True)
    def test_basic_sources_and_sinks(
        self, mock_load_config, mock_read_yaml, mock_exists
    ):
        # Setup mocks
        mock_config = mock.Mock()
        mock_config.get_ecosystem_for_cluster.return_value = "prod"
        mock_load_config.return_value = mock_config
        mock_exists.return_value = True

        # Mock YAML config with sources, sinks, and parallelism
        mock_read_yaml.return_value = {
            "parallelism": 10,
            "sources": [
                {
                    "table_name": "test_source",
                    "config": {
                        "schema_id": 12345,
                    },
                }
            ],
            "sinks": [
                {
                    "table_name": "test_sink",
                    "config": {
                        "namespace": "test_namespace",
                        "source": "test_source",
                        "alias": "1.0",
                    },
                }
            ],
        }

        result = flink_tools.get_sqlclient_job_config(
            "sqlclient", "test_instance", "test_cluster"
        )

        assert "sources" in result
        assert "sinks" in result
        assert "ecosystem" in result
        assert "parallelism" in result
        assert result["ecosystem"] == "prod"
        assert result["parallelism"] == 10
        assert len(result["sources"]) == 1
        assert len(result["sinks"]) == 1

        # Verify source details
        source = result["sources"][0]
        assert source["table_name"] == "test_source"
        assert source["schema_id"] == 12345

        # Verify sink details
        sink = result["sinks"][0]
        assert sink["table_name"] == "test_sink"
        assert sink["namespace"] == "test_namespace"
        assert sink["source"] == "test_source"
        assert sink["alias"] == "1.0"

    @mock.patch("paasta_tools.flink_tools.os.path.exists", autospec=True)
    @mock.patch("paasta_tools.utils.load_system_paasta_config", autospec=True)
    def test_missing_config_file(self, mock_load_config, mock_exists):
        # Setup mocks
        mock_config = mock.Mock()
        mock_config.get_ecosystem_for_cluster.return_value = "prod"
        mock_load_config.return_value = mock_config
        mock_exists.return_value = False

        result = flink_tools.get_sqlclient_job_config(
            "sqlclient", "nonexistent", "test_cluster"
        )

        assert "error" in result
        assert "not found" in result["error"].lower()

    @mock.patch("paasta_tools.flink_tools.os.path.exists", autospec=True)
    @mock.patch("service_configuration_lib.read_yaml_file", autospec=True)
    @mock.patch("paasta_tools.utils.load_system_paasta_config", autospec=True)
    def test_handles_float_alias(self, mock_load_config, mock_read_yaml, mock_exists):
        # Test that YAML float parsing (alias: 2.0) is handled correctly
        mock_config = mock.Mock()
        mock_config.get_ecosystem_for_cluster.return_value = "prod"
        mock_load_config.return_value = mock_config
        mock_exists.return_value = True

        mock_read_yaml.return_value = {
            "sources": [
                {
                    "table_name": "test_source",
                    "config": {
                        "namespace": "test_ns",
                        "source": "test_src",
                        "alias": 2.0,  # This will be a float from YAML
                    },
                }
            ],
            "sinks": [],
        }

        result = flink_tools.get_sqlclient_job_config(
            "sqlclient", "test_instance", "test_cluster"
        )

        # Verify alias is converted to string "2.0"
        source = result["sources"][0]
        assert source["alias"] == "2.0"
        assert isinstance(source["alias"], str)


class TestGetSqlclientParallelism:
    @mock.patch("paasta_tools.flink_tools.get_sqlclient_job_config", autospec=True)
    def test_get_parallelism(self, mock_get_config):
        mock_get_config.return_value = {"parallelism": 10}

        result = flink_tools.get_sqlclient_parallelism(
            "sqlclient", "test_instance", "test_cluster"
        )

        assert result == 10

    @mock.patch("paasta_tools.flink_tools.get_sqlclient_job_config", autospec=True)
    def test_get_parallelism_none(self, mock_get_config):
        mock_get_config.return_value = {}

        result = flink_tools.get_sqlclient_parallelism(
            "sqlclient", "test_instance", "test_cluster"
        )

        assert result is None


class TestGetSqlclientUdfPlugin:
    @mock.patch("paasta_tools.flink_tools.get_sqlclient_job_config", autospec=True)
    def test_get_udf_plugin(self, mock_get_config):
        mock_get_config.return_value = {
            "udf_config": {
                "plugin_name": "test_udf_plugin",
                "plugin_version": "1.0.0",
            }
        }

        result = flink_tools.get_sqlclient_udf_plugin(
            "sqlclient", "test_instance", "test_cluster"
        )

        assert result == "test_udf_plugin"

    @mock.patch("paasta_tools.flink_tools.get_sqlclient_job_config", autospec=True)
    def test_get_udf_plugin_none(self, mock_get_config):
        mock_get_config.return_value = {}

        result = flink_tools.get_sqlclient_udf_plugin(
            "sqlclient", "test_instance", "test_cluster"
        )

        assert result is None


class TestAnalyzeSlotUtilization:
    def test_underutilized_suggests_reduce(self):
        overview = mock.Mock()
        overview.slots_total = 12
        overview.slots_available = 9
        overview.taskmanagers = 3

        instance_config = mock.Mock()
        instance_config.config_dict = {"taskmanager": {"instances": 3}}

        result = flink_tools.analyze_slot_utilization(overview, instance_config)

        assert result["utilization_pct"] == 25.0
        assert result["recommendation"] is not None
        assert result["recommendation"]["action"] == "reduce"
        assert result["recommendation"]["new_instances"] == 1

    def test_overutilized_suggests_increase(self):
        overview = mock.Mock()
        overview.slots_total = 4
        overview.slots_available = 0
        overview.taskmanagers = 1

        instance_config = mock.Mock()
        instance_config.config_dict = {"taskmanager": {"instances": 1}}

        result = flink_tools.analyze_slot_utilization(overview, instance_config)

        assert result["utilization_pct"] == 100.0
        # Note: recommendation exists but we don't display "increase" suggestions
        assert result["recommendation"] is not None
        assert result["recommendation"]["action"] == "increase"

    def test_optimal_utilization(self):
        overview = mock.Mock()
        overview.slots_total = 10
        overview.slots_available = 3
        overview.taskmanagers = 2

        instance_config = mock.Mock()
        instance_config.config_dict = {"taskmanager": {"instances": 2}}

        result = flink_tools.analyze_slot_utilization(overview, instance_config)

        assert result["utilization_pct"] == 70.0
        assert result["recommendation"] is None


class TestFormatKafkaTopics:
    def test_format_with_schema_id(self):
        topics_info = {
            "sources": [
                {
                    "table_name": "test_source",
                    "schema_id": 12345,
                    "namespace": None,
                    "source": None,
                    "alias": None,
                }
            ],
            "sinks": [],
            "ecosystem": "prod",
        }

        with mock.patch(
            "paasta_tools.flink_tools.get_sqlclient_job_config",
            autospec=True,
            return_value=topics_info,
        ):
            output = flink_tools.format_kafka_topics(
                "sqlclient", "test", "test_cluster"
            )

        output_str = "\n".join(output)
        assert "Data Pipeline Topology:" in output_str
        assert "Sources: 1 topics" in output_str
        assert "test_source" in output_str
        assert "Schema ID:       12345" in output_str
        assert (
            "pipeline_studio_v2.yelpcorp.com/?search_by=2&ecosystem=prod&schema_id=12345"
            in output_str
        )
        assert "datapipe schema describe --schema-id 12345" in output_str
        assert "datapipe stream tail --schema-id 12345" in output_str

    def test_format_with_namespace_source_alias(self):
        topics_info = {
            "sources": [],
            "sinks": [
                {
                    "table_name": "test_sink",
                    "namespace": "test_namespace",
                    "source": "test_source",
                    "alias": "1.0",
                    "pkeys": "id",
                }
            ],
            "ecosystem": "devc",
        }

        with mock.patch(
            "paasta_tools.flink_tools.get_sqlclient_job_config",
            autospec=True,
            return_value=topics_info,
        ):
            output = flink_tools.format_kafka_topics(
                "sqlclient", "test", "test_cluster"
            )

        output_str = "\n".join(output)
        assert "Sinks:   1 topics" in output_str
        assert "test_sink" in output_str
        assert "Namespace:       test_namespace" in output_str
        assert "Source:          test_source" in output_str
        assert "Alias:           1.0" in output_str
        assert "Primary Keys:    id" in output_str
        assert (
            "pipeline_studio_v2.yelpcorp.com/namespaces/test_namespace/sources/test_source/asset-details?alias=1.0"
            in output_str
        )
        assert (
            "datapipe schema describe --namespace test_namespace --source test_source --alias 1.0"
            in output_str
        )
        assert (
            "datapipe stream tail --namespace test_namespace --source test_source --alias 1.0"
            in output_str
        )

    def test_format_with_error(self):
        topics_info = {"error": "Test error message"}

        with mock.patch(
            "paasta_tools.flink_tools.get_sqlclient_job_config",
            autospec=True,
            return_value=topics_info,
        ):
            output = flink_tools.format_kafka_topics(
                "sqlclient", "test", "test_cluster"
            )

        output_str = "\n".join(output)
        assert "Kafka Topics: Unable to fetch" in output_str
        assert "Error: Test error message" in output_str

    def test_format_with_consumer_group_prod(self):
        topics_info = {
            "sources": [
                {
                    "table_name": "test_source",
                    "schema_id": 123,
                    "namespace": None,
                    "source": None,
                    "alias": None,
                }
            ],
            "sinks": [],
            "ecosystem": "prod",
        }

        with mock.patch(
            "paasta_tools.flink_tools.get_sqlclient_job_config",
            autospec=True,
            return_value=topics_info,
        ):
            output = flink_tools.format_kafka_topics(
                "sqlclient", "test_instance", "test_cluster", job_name="test_job"
            )

        output_str = "\n".join(output)
        assert "Consumer Group: flink.sqlclient.test_instance.test_job" in output_str
        assert (
            "kafka-view.admin.yelp.com/clusters/scribe.uswest2-prod/groups/flink.sqlclient.test_instance.test_job"
            in output_str
        )
        assert "grafana.yelpcorp.com/d/kcHXkIBnz/consumer-metrics" in output_str
        assert "var-consumergroup=flink.sqlclient.test_instance.test_job" in output_str

    def test_format_with_consumer_group_devc(self):
        topics_info = {
            "sources": [
                {
                    "table_name": "test_source",
                    "schema_id": 123,
                    "namespace": None,
                    "source": None,
                    "alias": None,
                }
            ],
            "sinks": [],
            "ecosystem": "devc",
        }

        with mock.patch(
            "paasta_tools.flink_tools.get_sqlclient_job_config",
            autospec=True,
            return_value=topics_info,
        ):
            output = flink_tools.format_kafka_topics(
                "sqlclient", "test_instance", "test_cluster", job_name="test_job"
            )

        output_str = "\n".join(output)
        assert "Consumer Group: flink.sqlclient.test_instance.test_job" in output_str
        assert (
            "kafka-view.paasta-norcal-devc.yelp/clusters/buff-high.uswest1-devc/groups/flink.sqlclient.test_instance.test_job"
            in output_str
        )


class TestFormatResourceOptimization:
    def test_format_with_optimization_suggestion(self):
        overview = mock.Mock()
        overview.slots_total = 12
        overview.slots_available = 9
        overview.taskmanagers = 3

        instance_config = mock.Mock()
        instance_config.config_dict = {"taskmanager": {"instances": 3}}

        output = flink_tools.format_resource_optimization(
            "sqlclient", "test_instance", overview, instance_config
        )

        output_str = "\n".join(output)
        assert "Resource Utilization & Optimization:" in output_str
        assert "Taskmanagers:     3 instances" in output_str
        assert "Used Slots:       3 slots" in output_str
        assert "25% utilization" in output_str
        assert "OPTIMIZATION OPPORTUNITY" in output_str
        assert "instances: 1" in output_str

    def test_format_optimal_utilization(self):
        overview = mock.Mock()
        overview.slots_total = 4
        overview.slots_available = 1
        overview.taskmanagers = 1

        instance_config = mock.Mock()
        instance_config.config_dict = {"taskmanager": {"instances": 1}}

        output = flink_tools.format_resource_optimization(
            "sqlclient", "test_instance", overview, instance_config
        )

        output_str = "\n".join(output)
        assert "Resource utilization is optimal" in output_str


class TestFormatTopicLinksAndCommands:
    def test_with_schema_id(self):
        output = flink_tools._format_topic_links_and_commands(
            schema_id=12345,
            namespace=None,
            source_name=None,
            alias=None,
            ecosystem="prod",
        )

        output_str = "\n".join(output)
        assert (
            "Pipeline Studio: https://pipeline_studio_v2.yelpcorp.com/?search_by=2&ecosystem=prod&schema_id=12345"
            in output_str
        )
        assert (
            "Describe:        datapipe schema describe --schema-id 12345" in output_str
        )
        assert "Tail:            datapipe stream tail --schema-id 12345" in output_str

    def test_with_namespace_source_alias(self):
        output = flink_tools._format_topic_links_and_commands(
            schema_id=None,
            namespace="test_ns",
            source_name="test_src",
            alias="1.0",
            ecosystem="prod",
        )

        output_str = "\n".join(output)
        assert (
            "Pipeline Studio: https://pipeline_studio_v2.yelpcorp.com/namespaces/test_ns/sources/test_src/asset-details?alias=1.0"
            in output_str
        )
        assert (
            "Describe:        datapipe schema describe --namespace test_ns --source test_src --alias 1.0"
            in output_str
        )
        assert (
            "Tail:            datapipe stream tail --namespace test_ns --source test_src --alias 1.0"
            in output_str
        )


class TestFormatConsumerGroupInfo:
    def test_prod_consumer_group(self):
        output = flink_tools._format_consumer_group_info(
            "sqlclient", "test_instance", "test_job", "prod"
        )

        output_str = "\n".join(output)
        assert "Consumer Group: flink.sqlclient.test_instance.test_job" in output_str
        assert (
            "kafka-view.admin.yelp.com/clusters/scribe.uswest2-prod/groups/flink.sqlclient.test_instance.test_job"
            in output_str
        )
        assert "grafana.yelpcorp.com/d/kcHXkIBnz/consumer-metrics" in output_str

    def test_devc_consumer_group(self):
        output = flink_tools._format_consumer_group_info(
            "sqlclient", "test_instance", "test_job", "devc"
        )

        output_str = "\n".join(output)
        assert "Consumer Group: flink.sqlclient.test_instance.test_job" in output_str
        assert (
            "kafka-view.paasta-norcal-devc.yelp/clusters/buff-high.uswest1-devc/groups/flink.sqlclient.test_instance.test_job"
            in output_str
        )


class TestFormatSourceTopics:
    def test_format_sources(self):
        sources = [
            {
                "table_name": "test_source",
                "schema_id": 123,
                "namespace": None,
                "source": None,
                "alias": None,
            }
        ]

        output = flink_tools._format_source_topics(sources, "prod")

        output_str = "\n".join(output)
        assert "Source Topics:" in output_str
        assert "1. test_source" in output_str
        assert "Schema ID:       123" in output_str

    def test_empty_sources(self):
        output = flink_tools._format_source_topics([], "prod")
        assert output == []


class TestFormatSinkTopics:
    def test_format_sinks(self):
        sinks = [
            {
                "table_name": "test_sink",
                "namespace": "test_ns",
                "source": "test_src",
                "alias": "1.0",
                "pkeys": "id",
            }
        ]

        output = flink_tools._format_sink_topics(sinks, "prod")

        output_str = "\n".join(output)
        assert "Sink Topics:" in output_str
        assert "1. test_sink" in output_str
        assert "Primary Keys:    id" in output_str

    def test_empty_sinks(self):
        output = flink_tools._format_sink_topics([], "prod")
        assert output == []
