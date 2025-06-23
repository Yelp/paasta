import datetime
import hashlib
import os
import tempfile

import mock
import pytest

from paasta_tools import tron_tools
from paasta_tools import utils
from paasta_tools import yaml_tools as yaml
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.tron_tools import MASTER_NAMESPACE
from paasta_tools.tron_tools import MESOS_EXECUTOR_NAMES
from paasta_tools.tron_tools import TronActionConfigDict
from paasta_tools.utils import CAPS_DROP
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import NoDeploymentsAvailable

MOCK_SYSTEM_PAASTA_CONFIG = utils.SystemPaastaConfig(
    utils.SystemPaastaConfigDict(
        {
            "docker_registry": "mock_registry",
            "volumes": [],
            "dockercfg_location": "/mock/dockercfg",
            "spark_k8s_role": "spark",
            "enable_tron_tsc": True,
        }
    ),
    "/mock/system/configs",
)

MOCK_SYSTEM_PAASTA_CONFIG_OVERRIDES = utils.SystemPaastaConfig(
    utils.SystemPaastaConfigDict(
        {
            "docker_registry": "mock_registry",
            "volumes": [],
            "dockercfg_location": "/mock/dockercfg",
            "tron_default_pool_override": "big_pool",
            "tron_k8s_cluster_overrides": {
                "paasta-dev-test": "paasta-dev",
            },
            "enable_tron_tsc": True,
        }
    ),
    "/mock/system/configs",
)


class TestTronConfig:
    @pytest.fixture
    def config_dict(self):
        return {
            "cluster_name": "dev-batch",
            "default_paasta_cluster": "dev-oregon",
            "url": "http://mesos-master.com:2000",
        }

    def test_normal(self, config_dict):
        config = tron_tools.TronConfig(config_dict)
        assert config.get_cluster_name() == "dev-batch"
        assert config.get_url() == "http://mesos-master.com:2000"

    def test_no_cluster_name(self, config_dict):
        del config_dict["cluster_name"]
        config = tron_tools.TronConfig(config_dict)
        with pytest.raises(tron_tools.TronNotConfigured):
            config.get_cluster_name()

    def test_no_url(self, config_dict):
        del config_dict["url"]
        config = tron_tools.TronConfig(config_dict)
        with pytest.raises(tron_tools.TronNotConfigured):
            config.get_url()


def test_parse_time_variables_parses_shortdate():
    input_time = datetime.datetime(2012, 3, 14)
    test_input = "mycommand --date {shortdate-1} --format foo/logs/%L/%Y/%m/%d/"
    expected = "mycommand --date 2012-03-13 --format foo/logs/%L/%Y/%m/%d/"
    actual = tron_tools.parse_time_variables(command=test_input, parse_time=input_time)
    assert actual == expected


class TestTronActionConfig:
    @pytest.fixture
    def action_config(self):
        action_dict = {
            "name": "print",
            "command": "spark-submit something",
            "aws_credentials_yaml": "/some/yaml/path",
        }
        return tron_tools.TronActionConfig(
            service="my_service",
            instance=tron_tools.compose_instance("cool_job", "print"),
            cluster="fake-cluster",
            config_dict=action_dict,
            branch_dict={"docker_image": "foo:latest"},
        )

    def test_action_config(self, action_config):
        assert action_config.get_job_name() == "cool_job"
        assert action_config.get_action_name() == "print"
        assert action_config.get_cluster() == "fake-cluster"

    @pytest.mark.parametrize("executor", MESOS_EXECUTOR_NAMES)
    def test_get_env(
        self, mock_read_soa_metadata, action_config, executor, monkeypatch
    ):
        monkeypatch.setattr(tron_tools, "clusterman_metrics", mock.Mock())
        action_config.config_dict["executor"] = executor
        with mock.patch(
            "paasta_tools.utils.get_service_docker_registry",
            autospec=True,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ):
            env = action_config.get_env()
            assert not any([env.get("SPARK_OPTS"), env.get("CLUSTERMAN_RESOURCES")])

        assert "PAASTA_SOA_CONFIGS_SHA" not in env

    @pytest.mark.parametrize(
        "test_env,expected_env",
        (
            (
                {
                    "TEST_SECRET": "SECRET(a_service_secret)",
                    "TEST_NONSECRET": "not a secret",
                },
                {
                    "TEST_SECRET": {
                        "secret_name": "tron-secret-my--service-a--service--secret",
                        "key": "a_service_secret",
                    }
                },
            ),
            (
                {"TEST_SECRET": "SHARED_SECRET(a_shared_secret)"},
                {
                    "TEST_SECRET": {
                        "secret_name": "tron-secret-underscore-shared-a--shared--secret",
                        "key": "a_shared_secret",
                    }
                },
            ),
        ),
    )
    def test_get_secret_env(self, action_config, test_env, expected_env):
        action_config.config_dict["env"] = test_env
        secret_env = action_config.get_secret_env()
        assert secret_env == expected_env

    @pytest.mark.parametrize(
        ("test_secret_volumes", "expected_secret_volumes"),
        (
            (
                [
                    {
                        "secret_name": "secret1",
                        "container_path": "/b/c",
                        "default_mode": "0644",
                        "items": [{"key": "secret1", "path": "abc"}],
                    }
                ],
                [
                    {
                        "secret_volume_name": "tron-secret-my--service-secret1",
                        "secret_name": "secret1",
                        "container_path": "/b/c",
                        "default_mode": "0644",
                        "items": [{"key": "secret1", "path": "abc"}],
                    }
                ],
            ),
        ),
    )
    def test_get_secret_volumes(
        self, action_config, test_secret_volumes, expected_secret_volumes
    ):
        action_config.config_dict["secret_volumes"] = test_secret_volumes
        secret_volumes = action_config.get_secret_volumes()
        assert secret_volumes == expected_secret_volumes

    @pytest.mark.parametrize(
        ("is_shared, secret_name, expected_secret_volume_name"),
        (
            (False, "secret1", "tron-secret-my--service-secret1"),
            (True, "secret1", "tron-secret-underscore-shared-secret1"),
        ),
    )
    def test_get_secret_volume_name(
        self, action_config, is_shared, secret_name, expected_secret_volume_name
    ):

        with tempfile.TemporaryDirectory() as dir_path:
            service = action_config.service if not is_shared else SHARED_SECRET_SERVICE
            secret_path = os.path.join(
                dir_path, service, "secrets", f"{secret_name}.json"
            )
            os.makedirs(os.path.dirname(secret_path), exist_ok=True)
            with open(secret_path, "w") as f:
                f.write("FOOBAR")

            with mock.patch.object(action_config, "soa_dir", dir_path):
                assert (
                    action_config.get_secret_volume_name(secret_name)
                    == expected_secret_volume_name
                )

    def test_get_executor_default(self, action_config):
        assert action_config.get_executor() == "paasta"

    @pytest.mark.parametrize("executor", MESOS_EXECUTOR_NAMES)
    def test_get_executor_paasta(self, executor, action_config):
        action_config.config_dict["executor"] = executor
        assert action_config.get_executor() == executor


class TestTronJobConfig:
    @pytest.fixture(autouse=True)
    def mock_read_monitoring_config(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.read_monitoring_config",
            mock.Mock(return_value={"team": "default_team"}),
            autospec=None,
        ) as f:
            yield f

    @pytest.fixture(autouse=True)
    def mock_list_teams(self):
        with mock.patch(
            "paasta_tools.tron_tools.list_teams",
            mock.Mock(return_value=["default_team", "valid_team", "noop"]),
            autospec=None,
        ) as f:
            yield f

    @pytest.mark.parametrize(
        "action_service,action_deploy,cluster,expected_cluster",
        [
            # normal case - no cluster override present and k8s enabled
            (None, None, "paasta-dev", "paasta-dev"),
            (None, "special_deploy", "paasta-dev", "paasta-dev"),
            ("other_service", None, "paasta-dev", "paasta-dev"),
            (None, None, "paasta-dev", "paasta-dev"),
            (None, None, "paasta-dev", "paasta-dev"),
            # cluster override present and k8s enabled
            (None, None, "paasta-dev-test", "paasta-dev"),
            (None, "special_deploy", "paasta-dev-test", "paasta-dev"),
            ("other_service", None, "paasta-dev-test", "paasta-dev"),
            (None, None, "paasta-dev-test", "paasta-dev"),
            (None, None, "paasta-dev-test", "paasta-dev"),
        ],
    )
    @mock.patch("paasta_tools.tron_tools.load_v2_deployments_json", autospec=True)
    def test_get_action_config(
        self,
        mock_load_deployments,
        action_service,
        action_deploy,
        cluster,
        expected_cluster,
    ):
        """Check resulting action config with various overrides from the action."""
        action_dict = {"command": "echo first"}
        if action_service:
            action_dict["service"] = action_service
        if action_deploy:
            action_dict["deploy_group"] = action_deploy

        job_service = "my_service"
        job_deploy = "prod"
        expected_service = action_service or job_service
        expected_deploy = action_deploy or job_deploy

        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "service": job_service,
            "deploy_group": job_deploy,
            "max_runtime": "2h",
            "actions": {"normal": action_dict},
            "monitoring": {"team": "noop"},
        }

        soa_dir = "/other_dir"
        job_config = tron_tools.TronJobConfig(
            "my_job", job_dict, cluster, soa_dir=soa_dir
        )

        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG_OVERRIDES,
        ):
            action_config = job_config._get_action_config(
                "normal", action_dict=action_dict
            )

        mock_load_deployments.assert_called_once_with(expected_service, soa_dir)
        mock_deployments_json = mock_load_deployments.return_value
        mock_deployments_json.get_docker_image_for_deploy_group.assert_called_once_with(
            expected_deploy
        )
        mock_deployments_json.get_git_sha_for_deploy_group.assert_called_once_with(
            expected_deploy
        )
        mock_deployments_json.get_image_version_for_deploy_group.assert_called_once_with(
            expected_deploy
        )
        expected_branch_dict = {
            "docker_image": mock_deployments_json.get_docker_image_for_deploy_group.return_value,
            "git_sha": mock_deployments_json.get_git_sha_for_deploy_group.return_value,
            "image_version": mock_deployments_json.get_image_version_for_deploy_group.return_value,
            "desired_state": "start",
            "force_bounce": None,
        }

        expected_input_action_config = {
            "command": "echo first",
            "service": expected_service,
            "deploy_group": expected_deploy,
            "monitoring": {"team": "noop"},
        }

        assert action_config == tron_tools.TronActionConfig(
            service=expected_service,
            instance=tron_tools.compose_instance("my_job", "normal"),
            config_dict=expected_input_action_config,
            branch_dict=expected_branch_dict,
            soa_dir=soa_dir,
            cluster=expected_cluster,
        )

    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    @mock.patch("paasta_tools.tron_tools.load_v2_deployments_json", autospec=True)
    def test_get_action_config_load_deployments_false(
        self, mock_load_deployments, mock_load_system_paasta_config
    ):
        action_dict = {"command": "echo first"}
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "service": "my_service",
            "deploy_group": "prod",
            "max_runtime": "2h",
            "actions": {"normal": action_dict},
            "monitoring": {"team": "noop"},
        }
        soa_dir = "/other_dir"
        cluster = "paasta-dev"
        job_config = tron_tools.TronJobConfig(
            "my_job", job_dict, cluster, load_deployments=False, soa_dir=soa_dir
        )
        mock_load_deployments.side_effect = NoDeploymentsAvailable

        action_config = job_config._get_action_config("normal", action_dict)

        assert mock_load_deployments.call_count == 0
        assert action_config == tron_tools.TronActionConfig(
            service="my_service",
            cluster=cluster,
            instance=tron_tools.compose_instance("my_job", "normal"),
            config_dict={
                "command": "echo first",
                "service": "my_service",
                "deploy_group": "prod",
                "monitoring": {"team": "noop"},
            },
            branch_dict=None,
            soa_dir=soa_dir,
        )

    @mock.patch(
        "paasta_tools.tron_tools.TronJobConfig._get_action_config", autospec=True
    )
    @mock.patch("paasta_tools.tron_tools.format_tron_action_dict", autospec=True)
    def test_format_tron_job_dict(self, mock_format_action, mock_get_action_config):
        action_name = "normal"
        action_dict = {"command": "echo first"}
        actions = {action_name: action_dict}

        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "service": "my_service",
            "deploy_group": "prod",
            "max_runtime": "2h",
            "actions": actions,
            "expected_runtime": "1h",
            "monitoring": {"team": "noop"},
        }
        soa_dir = "/other_dir"
        cluster = "paasta-dev"
        job_config = tron_tools.TronJobConfig(
            "my_job", job_dict, cluster, soa_dir=soa_dir
        )
        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ):
            result = tron_tools.format_tron_job_dict(
                job_config=job_config, k8s_enabled=False
            )

        mock_get_action_config.assert_called_once_with(
            job_config, action_name, action_dict
        )
        mock_format_action.assert_called_once_with(
            action_config=mock_get_action_config.return_value,
        )

        assert result == {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "max_runtime": "2h",
            "actions": {
                mock_get_action_config.return_value.get_action_name.return_value: mock_format_action.return_value
            },
            "expected_runtime": "1h",
            "monitoring": {"team": "noop"},
        }

    @mock.patch(
        "paasta_tools.tron_tools.TronJobConfig._get_action_config", autospec=True
    )
    @mock.patch("paasta_tools.tron_tools.format_tron_action_dict", autospec=True)
    def test_format_tron_job_dict_k8s_enabled(
        self, mock_format_action, mock_get_action_config
    ):
        action_name = "normal"
        action_dict = {"command": "echo first"}
        actions = {action_name: action_dict}

        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "service": "my_service",
            "deploy_group": "prod",
            "max_runtime": "2h",
            "actions": actions,
            "expected_runtime": "1h",
            "monitoring": {"team": "noop"},
        }
        soa_dir = "/other_dir"
        cluster = "paasta-dev"
        job_config = tron_tools.TronJobConfig(
            "my_job", job_dict, cluster, soa_dir=soa_dir
        )
        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ):
            result = tron_tools.format_tron_job_dict(
                job_config=job_config, k8s_enabled=True
            )

        mock_get_action_config.assert_called_once_with(
            job_config, action_name, action_dict
        )
        mock_format_action.assert_called_once_with(
            action_config=mock_get_action_config.return_value,
        )

        assert result == {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "max_runtime": "2h",
            "actions": {
                mock_get_action_config.return_value.get_action_name.return_value: mock_format_action.return_value
            },
            "expected_runtime": "1h",
            "monitoring": {"team": "noop"},
        }

    @mock.patch(
        "paasta_tools.tron_tools.TronJobConfig._get_action_config", autospec=True
    )
    @mock.patch("paasta_tools.tron_tools.format_tron_action_dict", autospec=True)
    def test_format_tron_job_dict_with_cleanup_action(
        self, mock_format_action, mock_get_action_config
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "service": "my_service",
            "deploy_group": "prod",
            "max_runtime": "2h",
            "actions": {"normal": {"command": "echo first"}},
            "cleanup_action": {"command": "rm *"},
            "monitoring": {"team": "noop"},
        }
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "paasta-dev")

        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ):
            result = tron_tools.format_tron_job_dict(job_config, k8s_enabled=False)

        assert mock_get_action_config.call_args_list == [
            mock.call(job_config, "normal", job_dict["actions"]["normal"]),
            mock.call(job_config, "cleanup", job_dict["cleanup_action"]),
        ]
        assert mock_format_action.call_count == 2
        assert result == {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "max_runtime": "2h",
            "actions": {
                mock_get_action_config.return_value.get_action_name.return_value: mock_format_action.return_value
            },
            "cleanup_action": mock_format_action.return_value,
            "monitoring": {"team": "noop"},
        }

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_all_actions(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "paasta",
            "deploy_group": "test",
            "schedule": "daily 12:10:00",
            "service": "testservice",
            "actions": {
                "first": {"command": "echo first", "cpus": "bad string"},
                "second": {"command": "echo second", "mem": "not a number"},
            },
            "cleanup_action": {"command": "rm *", "cpus": "also bad"},
            "monitoring": {"team": "noop"},
        }
        mock_get_pipeline_deploy_groups.return_value = ["test"]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 3

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_invalid_deploy_group(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "deploy_group": "invalid_deploy_group",
            "monitoring": {"team": "noop", "page": True},
            "actions": {"first": {"command": "echo first"}},
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 1

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_valid_deploy_group(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "deploy_group": "deploy_group_1",
            "monitoring": {"team": "noop", "page": True},
            "actions": {"first": {"command": "echo first"}},
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 0

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_invalid_action_deploy_group(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "monitoring": {"team": "noop", "page": True},
            "actions": {
                "first": {
                    "command": "echo first",
                    "deploy_group": "invalid_deploy_group",
                }
            },
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 1

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_action_valid_deploy_group(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "deploy_group": "deploy_group_1",
            "monitoring": {"team": "noop", "page": True},
            "actions": {
                "first": {"command": "echo first", "deploy_group": "deploy_group_2"}
            },
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 0

    @mock.patch(
        "paasta_tools.tron_tools.TronActionConfig.build_spark_config", autospec=True
    )
    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_invalid_cpus_in_executor_spark_action(
        self,
        mock_load_system_paasta_config,
        mock_get_pipeline_deploy_groups,
        mock_build_spark_config,
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "monitoring": {"team": "noop", "page": True},
            "actions": {
                "first": {
                    "executor": "spark",
                    "cpus": 1,
                    "command": "echo first",
                    "deploy_group": "deploy_group_2",
                }
            },
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 1

    @mock.patch(
        "paasta_tools.tron_tools.TronActionConfig.build_spark_config", autospec=True
    )
    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_invalid_mem_in_executor_spark_action(
        self,
        mock_load_system_paasta_config,
        mock_get_pipeline_deploy_groups,
        mock_build_spark_config,
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "monitoring": {"team": "noop", "page": True},
            "actions": {
                "first": {
                    "executor": "spark",
                    "mem": 4096,
                    "command": "echo first",
                    "deploy_group": "deploy_group_2",
                }
            },
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 1

    @mock.patch(
        "paasta_tools.tron_tools.TronActionConfig.build_spark_config", autospec=True
    )
    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_valid_executor_spark_action(
        self,
        mock_load_system_paasta_config,
        mock_get_pipeline_deploy_groups,
        mock_build_spark_config,
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "monitoring": {"team": "noop", "page": True},
            "actions": {
                "first": {
                    "executor": "spark",
                    "command": "echo first",
                    "deploy_group": "deploy_group_2",
                }
            },
        }
        mock_get_pipeline_deploy_groups.return_value = [
            "deploy_group_1",
            "deploy_group_2",
        ]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 0

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_monitoring(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "deploy_group": "test",
            "monitoring": {"team": "noop", "page": True},
            "actions": {"first": {"command": "echo first"}},
        }
        mock_get_pipeline_deploy_groups.return_value = ["test"]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert len(errors) == 0

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_monitoring_without_team(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "schedule": "daily 12:10:00",
            "monitoring": {"page": True},
            "deploy_group": "test",
            "actions": {"first": {"command": "echo first"}},
        }
        mock_get_pipeline_deploy_groups.return_value = ["test"]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert errors == []
        assert job_config.get_monitoring()["team"] == "default_team"

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_validate_monitoring_with_invalid_team(
        self, mock_load_system_paasta_config, mock_get_pipeline_deploy_groups
    ):
        job_dict = {
            "node": "batch_server",
            "deploy_group": "test",
            "schedule": "daily 12:10:00",
            "monitoring": {"team": "invalid_team", "page": True},
            "actions": {"first": {"command": "echo first"}},
        }
        mock_get_pipeline_deploy_groups.return_value = ["test"]
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake-cluster")
        errors = job_config.validate()
        assert errors == [
            "Invalid team name: invalid_team. Do you mean one of these: ['valid_team']"
        ]

    @pytest.mark.parametrize(
        "tronfig_monitoring", [{"team": "tronfig_team"}, {"non_tron_key": True}]
    )
    def test_get_monitoring(self, tronfig_monitoring):
        job_dict = {"monitoring": tronfig_monitoring}
        job_config = tron_tools.TronJobConfig("my_job", job_dict, "fake_cluster")
        assert job_config.get_monitoring() == {
            "team": ("tronfig_team" if "team" in tronfig_monitoring else "default_team")
        }


class TestTronTools:
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    def test_load_tron_config(self, mock_load_system_paasta_config):
        result = tron_tools.load_tron_config()
        assert result == tron_tools.TronConfig(
            mock_load_system_paasta_config().get_tron_config()
        )

    @mock.patch("paasta_tools.tron_tools.load_tron_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.TronClient", autospec=True)
    def test_get_tron_client(self, mock_client, mock_system_tron_config):
        result = tron_tools.get_tron_client()
        assert mock_system_tron_config.return_value.get_url.call_count == 1
        mock_client.assert_called_once_with(
            mock_system_tron_config.return_value.get_url.return_value
        )
        assert result == mock_client.return_value

    def test_compose_instance(self):
        result = tron_tools.compose_instance("great_job", "fast_action")
        assert result == "great_job.fast_action"

    def test_decompose_instance_valid(self):
        result = tron_tools.decompose_instance("job_a.start")
        assert result == ("job_a", "start")

    def test_decompose_instance_invalid(self):
        with pytest.raises(InvalidInstanceConfig):
            tron_tools.decompose_instance("job_a")

    def test_format_master_config(self):
        master_config = {
            "some_key": 101,
            "another": "hello",
            "mesos_options": {
                "default_volumes": [
                    {
                        "container_path": "/nail/tmp",
                        "host_path": "/nail/tmp",
                        "mode": "RW",
                    }
                ],
                "other_mesos": True,
            },
        }
        paasta_volumes = [
            {"containerPath": "/nail/other", "hostPath": "/other/home", "mode": "RW"}
        ]
        dockercfg = "file://somewhere"
        result = tron_tools.format_master_config(
            master_config, paasta_volumes, dockercfg
        )
        assert result == {
            "some_key": 101,
            "another": "hello",
            "mesos_options": {
                "default_volumes": [
                    {
                        "container_path": "/nail/other",
                        "host_path": "/other/home",
                        "mode": "RW",
                    }
                ],
                "dockercfg_location": dockercfg,
                "other_mesos": True,
            },
        }

        master_config["k8s_options"] = {
            "kubeconfig_path": "/var/lib/tron/kubeconfig.conf"
        }

        result = tron_tools.format_master_config(
            master_config, paasta_volumes, dockercfg
        )

        assert result["k8s_options"] == {
            "kubeconfig_path": "/var/lib/tron/kubeconfig.conf",
            "default_volumes": [
                {
                    "container_path": "/nail/other",
                    "host_path": "/other/home",
                    "mode": "RW",
                }
            ],
        }

    def test_format_tron_action_dict_default_executor(self):
        action_dict = {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "expected_runtime": "30m",
        }
        branch_dict = {
            "docker_image": "my_service:paasta-123abcde",
            "git_sha": "aabbcc44",
            "desired_state": "start",
            "force_bounce": None,
        }
        action_config = tron_tools.TronActionConfig(
            service="my_service",
            instance=tron_tools.compose_instance("my_job", "do_something"),
            config_dict=action_dict,
            branch_dict=branch_dict,
            cluster="test-cluster",
        )
        with mock.patch.object(
            action_config, "get_docker_registry", return_value="docker-registry.com:400"
        ), mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            result = tron_tools.format_tron_action_dict(action_config)
        assert result["executor"] == "kubernetes"

    def test_format_tron_action_dict_paasta(self):
        action_dict = {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "service": "my_service",
            "deploy_group": "prod",
            "executor": "paasta",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "pool": "special_pool",
            "env": {"SHELL": "/bin/bash"},
            "secret_volumes": [
                {
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "extra_volumes": [
                {
                    "containerPath": "/nail/bulkdata",
                    "hostPath": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"},
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
        }
        branch_dict = {
            "docker_image": "my_service:paasta-123abcde",
            "git_sha": "aabbcc44",
            "desired_state": "start",
            "force_bounce": None,
        }
        action_config = tron_tools.TronActionConfig(
            service="my_service",
            instance=tron_tools.compose_instance("my_job", "do_something"),
            config_dict=action_dict,
            branch_dict=branch_dict,
            cluster="test-cluster",
        )

        with mock.patch.object(
            action_config, "get_docker_registry", return_value="docker-registry.com:400"
        ), mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[
                {"audience": "foo.bar.com", "container_path": "/var/foo/bar"}
            ],
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        assert result == {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "docker_image": mock.ANY,
            "executor": "kubernetes",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "env": mock.ANY,
            "idempotent": False,
            "topology_spread_constraints": [
                {
                    "label_selector": {
                        "app.kubernetes.io/managed-by": "tron",
                        "paasta.yelp.com/pool": "special_pool",
                    },
                    "max_skew": 1,
                    "topology_key": "topology.kubernetes.io/zone",
                    "when_unsatisfiable": "ScheduleAnyway",
                },
            ],
            "secret_volumes": [
                {
                    "secret_volume_name": "tron-secret-my--service-secret1",
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "extra_volumes": [
                {
                    "container_path": "/nail/bulkdata",
                    "host_path": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"container_path": "/nail/tmp", "host_path": "/nail/tmp", "mode": "RW"},
            ],
            "projected_sa_volumes": [
                {"audience": "foo.bar.com", "container_path": "/var/foo/bar"},
            ],
            "field_selector_env": {"PAASTA_POD_IP": {"field_path": "status.podIP"}},
            "node_selectors": {"yelp.com/pool": "special_pool"},
            "labels": {
                "paasta.yelp.com/cluster": "test-cluster",
                "paasta.yelp.com/instance": "my_job.do_something",
                "paasta.yelp.com/pool": "special_pool",
                "paasta.yelp.com/service": "my_service",
                "yelp.com/owner": "compute_infra_platform_experience",
                "app.kubernetes.io/managed-by": "tron",
                "tron.yelp.com/idempotent-action": "false",
            },
            "annotations": {
                "paasta.yelp.com/routable_ip": "false",
                "paasta.yelp.com/service": "my_service",
                "paasta.yelp.com/instance": "my_job.do_something",
            },
            "cap_drop": CAPS_DROP,
            "cap_add": [],
            "secret_env": {},
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
        }
        expected_docker = "{}/{}".format(
            "docker-registry.com:400", branch_dict["docker_image"]
        )
        assert result["docker_image"] == expected_docker
        assert result["env"]["SHELL"] == "/bin/bash"

    @mock.patch(
        "paasta_tools.tron_tools.TronActionConfig.get_docker_registry", autospec=True
    )
    @mock.patch("paasta_tools.kubernetes_tools.kube_client", autospec=True)
    @mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    )
    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    @mock.patch("paasta_tools.tron_tools.get_k8s_url_for_cluster", autospec=True)
    @mock.patch(
        "service_configuration_lib.spark_config._get_k8s_docker_volumes_conf",
        autospec=True,
    )
    @mock.patch(
        "service_configuration_lib.spark_config.utils.load_spark_srv_conf",
        autospec=True,
    )
    def test_format_tron_action_dict_spark(
        self,
        mock_load_spark_srv_conf,
        mock_get_k8s_docker_volumes_conf,
        mock_get_k8s_url_for_cluster,
        mock_load_system_paasta_config,
        mock_load_kube_config,
        mock_kube_client,
        mock_get_docker_registry,
    ):
        action_dict = {
            "iam_role_provider": "aws",
            "iam_role": "arn:aws:iam::000000000000:role/some_role",
            "spark_args": {
                "spark.cores.max": 4,
                "spark.driver.memory": "1g",
                "spark.executor.memory": "1g",
                "spark.executor.cores": 2,
            },
            "command": "spark-submit file://this/is/a_test.py",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "service": "my_service",
            "deploy_group": "prod",
            "executor": "spark",
            "disk": 42,
            "idempotent": True,
            "pool": "special_pool",
            "env": {"SHELL": "/bin/bash"},
            "secret_volumes": [
                {
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "extra_volumes": [
                {
                    "containerPath": "/nail/bulkdata",
                    "hostPath": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"},
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
        }
        branch_dict = {
            "docker_image": "my_service:paasta-123abcde",
            "git_sha": "aabbcc44",
            "desired_state": "start",
            "force_bounce": None,
        }
        mock_get_k8s_docker_volumes_conf.return_value = {
            "spark.kubernetes.executor.volumes.hostPath.0.mount.path": "/nail/bulkdata",
            "spark.kubernetes.executor.volumes.hostPath.0.options.path": "/nail/bulkdata",
            "spark.kubernetes.executor.volumes.hostPath.0.mount.readOnly": "true",
            "spark.kubernetes.executor.volumes.hostPath.1.mount.path": "/nail/tmp",
            "spark.kubernetes.executor.volumes.hostPath.1.options.path": "/nail/tmp",
            "spark.kubernetes.executor.volumes.hostPath.1.mount.readOnly": "false",
            "spark.kubernetes.executor.volumes.hostPath.2.mount.path": "/etc/pki/spark",
            "spark.kubernetes.executor.volumes.hostPath.2.options.path": "/etc/pki/spark",
            "spark.kubernetes.executor.volumes.hostPath.2.mount.readOnly": "true",
            "spark.kubernetes.executor.volumes.hostPath.3.mount.path": "/etc/passwd",
            "spark.kubernetes.executor.volumes.hostPath.3.options.path": "/etc/passwd",
            "spark.kubernetes.executor.volumes.hostPath.3.mount.readOnly": "true",
            "spark.kubernetes.executor.volumes.hostPath.4.mount.path": "/etc/group",
            "spark.kubernetes.executor.volumes.hostPath.4.options.path": "/etc/group",
            "spark.kubernetes.executor.volumes.hostPath.4.mount.readOnly": "true",
        }
        mock_load_spark_srv_conf.return_value = (
            {},
            {
                "target_mem_cpu_ratio": 7,
                "resource_configs": {
                    "recommended": {
                        "cpu": 4,
                        "mem": 28,
                    },
                    "medium": {
                        "cpu": 8,
                        "mem": 56,
                    },
                    "max": {
                        "cpu": 12,
                        "mem": 110,
                    },
                },
                "cost_factor": {
                    "test-cluster": {
                        "test-pool": 100,
                    },
                    "spark-pnw-prod": {
                        "batch": 0.041,
                        "stable_batch": 0.142,
                    },
                },
                "adjust_executor_res_ratio_thresh": 99999,
                "default_resources_waiting_time_per_executor": 2,
                "default_clusterman_observed_scaling_time": 15,
                "high_cost_threshold_daily": 500,
                "preferred_spark_ui_port_start": 39091,
                "preferred_spark_ui_port_end": 39100,
                "defaults": {
                    "spark.executor.cores": 4,
                    "spark.executor.instances": 2,
                    "spark.executor.memory": 28,
                    "spark.task.cpus": 1,
                    "spark.sql.shuffle.partitions": 128,
                    "spark.dynamicAllocation.executorAllocationRatio": 0.8,
                    "spark.dynamicAllocation.cachedExecutorIdleTimeout": "1500s",
                    "spark.yelp.dra.minExecutorRatio": 0.25,
                },
                "mandatory_defaults": {
                    "spark.kubernetes.allocation.batch.size": 512,
                    "spark.kubernetes.decommission.script": "/opt/spark/kubernetes/dockerfiles/spark/decom.sh",
                    "spark.logConf": "true",
                },
            },
            {
                "spark.executor.cores": 4,
                "spark.executor.instances": 2,
                "spark.executor.memory": 28,
                "spark.task.cpus": 1,
                "spark.sql.shuffle.partitions": 128,
                "spark.dynamicAllocation.executorAllocationRatio": 0.8,
                "spark.dynamicAllocation.cachedExecutorIdleTimeout": "1500s",
                "spark.yelp.dra.minExecutorRatio": 0.25,
            },
            {
                "spark.kubernetes.allocation.batch.size": 512,
                "spark.kubernetes.decommission.script": "/opt/spark/kubernetes/dockerfiles/spark/decom.sh",
                "spark.logConf": "true",
            },
            {
                "test-cluster": {
                    "test-pool": 100,
                },
                "spark-pnw-prod": {
                    "batch": 0.041,
                    "stable_batch": 0.142,
                },
            },
        )
        mock_get_k8s_url_for_cluster.return_value = (
            "https://k8s.test-cluster.paasta:6443"
        )
        mock_load_system_paasta_config.return_value = MOCK_SYSTEM_PAASTA_CONFIG
        mock_get_docker_registry.return_value = "docker-registry.com:400"
        action_config = tron_tools.TronActionConfig(
            service="my_service",
            instance=tron_tools.compose_instance("my_job", "do_something"),
            config_dict=TronActionConfigDict(action_dict),
            branch_dict=utils.BranchDictV2(branch_dict),
            cluster="test-cluster",
        )

        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.tron_tools._spark_k8s_role",
            autospec=True,
            return_value="spark",
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        confs = result["command"].split(" ")
        spark_app_name = ""
        for s in confs:
            if s.startswith("spark.app.name"):
                spark_app_name = s.split("=")[1]

        expected = {
            "command": "timeout 12h spark-submit "
            "--conf spark.cores.max=4 "
            "--conf spark.driver.memory=1g "
            "--conf spark.executor.memory=1g "
            "--conf spark.executor.cores=2 "
            f"--conf spark.app.name={spark_app_name} "
            "--conf spark.ui.port=39091 "
            "--conf spark.executor.instances=0 "
            "--conf spark.kubernetes.executor.limit.cores=2 "
            "--conf spark.scheduler.maxRegisteredResourcesWaitingTime=15min "
            "--conf spark.task.cpus=1 "
            "--conf spark.master=k8s://https://k8s.test-cluster.paasta:6443 "
            "--conf spark.executorEnv.PAASTA_SERVICE=my_service "
            "--conf spark.executorEnv.PAASTA_INSTANCE=my_job.do_something "
            "--conf spark.executorEnv.PAASTA_CLUSTER=test-cluster "
            "--conf spark.executorEnv.PAASTA_INSTANCE_TYPE=spark "
            "--conf spark.executorEnv.SPARK_EXECUTOR_DIRS=/tmp "
            "--conf spark.kubernetes.pyspark.pythonVersion=3 "
            "--conf spark.kubernetes.container.image=docker-registry.com:400/$PAASTA_DOCKER_IMAGE "
            "--conf spark.kubernetes.namespace=paasta-spark "
            "--conf spark.kubernetes.executor.label.yelp.com/paasta_service=my_service "
            "--conf spark.kubernetes.executor.label.yelp.com/paasta_instance=my_job.do_something "
            "--conf spark.kubernetes.executor.label.yelp.com/paasta_cluster=test-cluster "
            "--conf spark.kubernetes.executor.label.paasta.yelp.com/service=my_service "
            "--conf spark.kubernetes.executor.label.paasta.yelp.com/instance=my_job.do_something "
            "--conf spark.kubernetes.executor.label.paasta.yelp.com/cluster=test-cluster "
            "--conf spark.kubernetes.executor.annotation.paasta.yelp.com/service=my_service "
            "--conf spark.kubernetes.executor.annotation.paasta.yelp.com/instance=my_job.do_something "
            "--conf spark.kubernetes.executor.label.spark.yelp.com/user=TRON "
            "--conf spark.kubernetes.executor.label.spark.yelp.com/driver_ui_port=39091 "
            "--conf spark.kubernetes.node.selector.yelp.com/pool=special_pool "
            "--conf spark.kubernetes.executor.label.yelp.com/pool=special_pool "
            "--conf spark.kubernetes.executor.label.paasta.yelp.com/pool=special_pool "
            "--conf spark.kubernetes.executor.label.yelp.com/owner=core_ml "
            "--conf spark.kubernetes.executor.volumes.hostPath.0.mount.path=/nail/bulkdata "
            "--conf spark.kubernetes.executor.volumes.hostPath.0.options.path=/nail/bulkdata "
            "--conf spark.kubernetes.executor.volumes.hostPath.0.mount.readOnly=true "
            "--conf spark.kubernetes.executor.volumes.hostPath.1.mount.path=/nail/tmp "
            "--conf spark.kubernetes.executor.volumes.hostPath.1.options.path=/nail/tmp "
            "--conf spark.kubernetes.executor.volumes.hostPath.1.mount.readOnly=false "
            "--conf spark.kubernetes.executor.volumes.hostPath.2.mount.path=/etc/pki/spark "
            "--conf spark.kubernetes.executor.volumes.hostPath.2.options.path=/etc/pki/spark "
            "--conf spark.kubernetes.executor.volumes.hostPath.2.mount.readOnly=true "
            "--conf spark.kubernetes.executor.volumes.hostPath.3.mount.path=/etc/passwd "
            "--conf spark.kubernetes.executor.volumes.hostPath.3.options.path=/etc/passwd "
            "--conf spark.kubernetes.executor.volumes.hostPath.3.mount.readOnly=true "
            "--conf spark.kubernetes.executor.volumes.hostPath.4.mount.path=/etc/group "
            "--conf spark.kubernetes.executor.volumes.hostPath.4.options.path=/etc/group "
            "--conf spark.kubernetes.executor.volumes.hostPath.4.mount.readOnly=true "
            "--conf spark.dynamicAllocation.enabled=true "
            "--conf spark.dynamicAllocation.shuffleTracking.enabled=true "
            "--conf spark.dynamicAllocation.executorAllocationRatio=0.8 "
            "--conf spark.dynamicAllocation.cachedExecutorIdleTimeout=1500s "
            "--conf spark.dynamicAllocation.maxExecutors=2 "
            "--conf spark.ui.prometheus.enabled=true "
            "--conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet "
            "--conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus "
            "--conf spark.eventLog.enabled=false "
            "--conf spark.sql.shuffle.partitions=12 "
            "--conf spark.sql.files.minPartitionNum=12 "
            "--conf spark.default.parallelism=12 "
            "--conf spark.kubernetes.allocation.batch.size=512 "
            "--conf spark.kubernetes.decommission.script=/opt/spark/kubernetes/dockerfiles/spark/decom.sh "
            "--conf spark.logConf=true "
            "--conf spark.kubernetes.executor.podTemplateFile=/nail/srv/configs/spark_dns_pod_template.yaml "
            "--conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider "
            "--conf spark.driver.host=$PAASTA_POD_IP "
            "--conf spark.kubernetes.authenticate.executor.serviceAccountName=paasta--arn-aws-iam-000000000000-role-some-role "
            "file://this/is/a_test.py",
            "executor": "spark",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "secret_volumes": [
                {
                    "secret_volume_name": "tron-secret-my--service-secret1",
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "items": [{"key": "secret1", "path": "abc"}],
                    "default_mode": "0644",
                }
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
            "service_account_name": "paasta--arn-aws-iam-000000000000-role-some-role",
            "secret_env": {},
            "field_selector_env": {"PAASTA_POD_IP": {"field_path": "status.podIP"}},
            "env": {
                "PAASTA_SERVICE": "my_service",
                "PAASTA_INSTANCE": "my_job.do_something",
                "PAASTA_CLUSTER": "test-cluster",
                "PAASTA_DEPLOY_GROUP": "prod",
                "PAASTA_DOCKER_IMAGE": "my_service:paasta-123abcde",
                "PAASTA_RESOURCE_CPUS": "1",
                "PAASTA_RESOURCE_MEM": "1126",
                "PAASTA_RESOURCE_DISK": "42",
                "PAASTA_GIT_SHA": "123abcde",
                "PAASTA_INSTANCE_TYPE": "spark",
                "SHELL": "/bin/bash",
                "SPARK_USER": "root",
                "ENABLE_PER_INSTANCE_LOGSPOUT": "1",
                "KUBECONFIG": "/etc/kubernetes/spark.conf",
                "AWS_DEFAULT_REGION": "us-west-2",
                "AWS_SDK_UA_APP_ID": "my_service.my_job.do_something",
            },
            "topology_spread_constraints": [
                {
                    "label_selector": {
                        "app.kubernetes.io/managed-by": "tron",
                        "paasta.yelp.com/pool": "stable",
                    },
                    "max_skew": 1,
                    "topology_key": "topology.kubernetes.io/zone",
                    "when_unsatisfiable": "ScheduleAnyway",
                },
            ],
            "node_selectors": {"yelp.com/pool": "stable"},
            "cap_add": [],
            "cap_drop": [
                "SETPCAP",
                "MKNOD",
                "AUDIT_WRITE",
                "CHOWN",
                "NET_RAW",
                "DAC_OVERRIDE",
                "FOWNER",
                "FSETID",
                "KILL",
                "SETGID",
                "SETUID",
                "NET_BIND_SERVICE",
                "SYS_CHROOT",
                "SETFCAP",
            ],
            "labels": {
                "paasta.yelp.com/cluster": "test-cluster",
                "paasta.yelp.com/pool": "stable",
                "paasta.yelp.com/service": "my_service",
                "paasta.yelp.com/instance": "my_job.do_something",
                "yelp.com/owner": "compute_infra_platform_experience",
                "app.kubernetes.io/managed-by": "tron",
                "paasta.yelp.com/prometheus_shard": "ml-compute",
                "spark.yelp.com/user": "TRON",
                "spark.yelp.com/driver_ui_port": "39091",
                "tron.yelp.com/idempotent-action": "true",
            },
            "idempotent": True,
            "annotations": {
                "paasta.yelp.com/routable_ip": "true",
                "paasta.yelp.com/service": "my_service",
                "paasta.yelp.com/instance": "my_job.do_something",
                "prometheus.io/port": "39091",
                "prometheus.io/path": "/metrics/prometheus",
            },
            "extra_volumes": [
                {
                    "container_path": "/nail/bulkdata",
                    "host_path": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"container_path": "/nail/tmp", "host_path": "/nail/tmp", "mode": "RW"},
                {
                    "container_path": "/etc/kubernetes/spark.conf",
                    "host_path": "/etc/kubernetes/spark.conf",
                    "mode": "RO",
                },
            ],
            "ports": [39091],
            "cpus": 1,
            "mem": 1126,
            "disk": 42,
            "docker_image": "docker-registry.com:400/my_service:paasta-123abcde",
        }

        assert result == expected

        expected_docker = "{}/{}".format(
            "docker-registry.com:400", branch_dict["docker_image"]
        )
        assert result["docker_image"] == expected_docker
        assert result["env"]["SHELL"] == "/bin/bash"

    def test_format_tron_action_dict_paasta_k8s_service_account(self):
        action_dict = {
            "service_account_name": "a-magic-sa",
            "command": "echo something",
            "service": "my_service",
            "deploy_group": "prod",
            "pool": "default",
            "executor": "paasta",
            "idempotent": True,
        }
        branch_dict = {
            "docker_image": "my_service:paasta-123abcde",
            "git_sha": "aabbcc44",
            "desired_state": "start",
            "force_bounce": None,
        }
        action_config = tron_tools.TronActionConfig(
            service="my_service",
            instance="job_name.instance_name",
            config_dict=action_dict,
            branch_dict=branch_dict,
            cluster="test-cluster",
        )

        with mock.patch.object(
            action_config, "get_docker_registry", return_value="docker-registry.com:400"
        ), mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        assert result == {
            "command": "echo something",
            "docker_image": mock.ANY,
            "executor": "kubernetes",
            "cpus": 1,
            "mem": 4096,
            "disk": 1024,
            "cap_add": [],
            "cap_drop": CAPS_DROP,
            "idempotent": True,
            "labels": {
                "paasta.yelp.com/cluster": "test-cluster",
                "paasta.yelp.com/instance": "job_name.instance_name",
                "paasta.yelp.com/pool": "default",
                "paasta.yelp.com/service": "my_service",
                "yelp.com/owner": "compute_infra_platform_experience",
                "app.kubernetes.io/managed-by": "tron",
                "tron.yelp.com/idempotent-action": "true",
            },
            "annotations": {
                "paasta.yelp.com/routable_ip": "false",
                "paasta.yelp.com/service": "my_service",
                "paasta.yelp.com/instance": "job_name.instance_name",
            },
            "node_selectors": {"yelp.com/pool": "default"},
            "env": mock.ANY,
            "topology_spread_constraints": [
                {
                    "label_selector": {
                        "app.kubernetes.io/managed-by": "tron",
                        "paasta.yelp.com/pool": "default",
                    },
                    "max_skew": 1,
                    "topology_key": "topology.kubernetes.io/zone",
                    "when_unsatisfiable": "ScheduleAnyway",
                },
            ],
            "secret_env": {},
            "field_selector_env": {"PAASTA_POD_IP": {"field_path": "status.podIP"}},
            "secret_volumes": [],
            "extra_volumes": [],
            "service_account_name": "a-magic-sa",
        }
        expected_docker = "{}/{}".format(
            "docker-registry.com:400", branch_dict["docker_image"]
        )
        assert result["docker_image"] == expected_docker
        assert result["env"]["ENABLE_PER_INSTANCE_LOGSPOUT"] == "1"

    @pytest.mark.parametrize(
        "instance_name,expected_instance_label",
        (
            ("my_job.do_something", "my_job.do_something"),
            (
                f"my_job.{'a'* 100}",
                "my_job.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-6xhe",
            ),
        ),
    )
    def test_format_tron_action_dict_paasta_k8s(
        self, instance_name, expected_instance_label
    ):
        action_dict = {
            "iam_role_provider": "aws",
            "iam_role": "arn:aws:iam::000000000000:role/some_role",
            "command": "echo something",
            "node_selectors": {
                "instance_type": [
                    "c5.2xlarge",
                    "c5n.17xlarge",
                ]
            },
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "service": "my_service",
            "deploy_group": "prod",
            "executor": "paasta",
            "cpus": 2,
            "mem": 1200,
            "monitoring": {
                "team": "some_sensu_team",
            },
            "disk": 42,
            "pool": "special_pool",
            "env": {"SHELL": "/bin/bash", "SOME_SECRET": "SECRET(secret_name)"},
            "secret_volumes": [
                {
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "extra_volumes": [
                {
                    "containerPath": "/nail/bulkdata",
                    "hostPath": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"},
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
        }
        branch_dict = {
            "docker_image": "my_service:paasta-123abcde",
            "git_sha": "aabbcc44",
            "desired_state": "start",
            "force_bounce": None,
        }
        action_config = tron_tools.TronActionConfig(
            service="my_service",
            instance=instance_name,
            config_dict=action_dict,
            branch_dict=branch_dict,
            cluster="test-cluster",
        )

        with mock.patch.object(
            action_config, "get_docker_registry", return_value="docker-registry.com:400"
        ), mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.tron_tools.get_service_account_name",
            autospec=True,
            return_value="some--service--account",
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.secret_tools.is_shared_secret_from_secret_name",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        assert result == {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "docker_image": mock.ANY,
            "executor": "kubernetes",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "idempotent": False,
            "cap_add": [],
            "cap_drop": CAPS_DROP,
            "labels": {
                "paasta.yelp.com/cluster": "test-cluster",
                "paasta.yelp.com/instance": expected_instance_label,
                "paasta.yelp.com/pool": "special_pool",
                "paasta.yelp.com/service": "my_service",
                "yelp.com/owner": "compute_infra_platform_experience",
                "app.kubernetes.io/managed-by": "tron",
                "tron.yelp.com/idempotent-action": "false",
            },
            "annotations": {
                "paasta.yelp.com/routable_ip": "false",
                "paasta.yelp.com/service": "my_service",
                "paasta.yelp.com/instance": instance_name,
            },
            "node_selectors": {"yelp.com/pool": "special_pool"},
            "node_affinities": [
                {
                    "key": "node.kubernetes.io/instance-type",
                    "operator": "In",
                    "value": ["c5.2xlarge", "c5n.17xlarge"],
                }
            ],
            "env": mock.ANY,
            "topology_spread_constraints": [
                {
                    "label_selector": {
                        "app.kubernetes.io/managed-by": "tron",
                        "paasta.yelp.com/pool": "special_pool",
                    },
                    "max_skew": 1,
                    "topology_key": "topology.kubernetes.io/zone",
                    "when_unsatisfiable": "ScheduleAnyway",
                },
            ],
            "secret_env": {
                "SOME_SECRET": {
                    "secret_name": "tron-secret-my--service-secret--name",
                    "key": "secret_name",
                }
            },
            "secret_volumes": [
                {
                    "secret_volume_name": "tron-secret-my--service-secret1",
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "field_selector_env": {"PAASTA_POD_IP": {"field_path": "status.podIP"}},
            "extra_volumes": [
                {
                    "container_path": "/nail/bulkdata",
                    "host_path": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"container_path": "/nail/tmp", "host_path": "/nail/tmp", "mode": "RW"},
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
            "service_account_name": "some--service--account",
        }
        expected_docker = "{}/{}".format(
            "docker-registry.com:400", branch_dict["docker_image"]
        )
        assert result["docker_image"] == expected_docker
        assert result["env"]["SHELL"] == "/bin/bash"
        assert result["env"]["ENABLE_PER_INSTANCE_LOGSPOUT"] == "1"
        assert "SOME_SECRET" not in result["env"]

    def test_format_tron_action_dict_paasta_no_branch_dict(self):
        action_dict = {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "service": "my_service",
            "deploy_group": "prod",
            "executor": "paasta",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "pool": "special_pool",
            "env": {"SHELL": "/bin/bash"},
            "secret_volumes": [
                {
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "extra_volumes": [
                {
                    "containerPath": "/nail/bulkdata",
                    "hostPath": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"},
            ],
        }
        action_config = tron_tools.TronActionConfig(
            service="my_service",
            instance=tron_tools.compose_instance("my_job", "do_something"),
            config_dict=action_dict,
            branch_dict=None,
            cluster="paasta-dev",
        )

        with mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            result = tron_tools.format_tron_action_dict(action_config)
        assert result == {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "docker_image": "",
            "executor": "kubernetes",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "idempotent": False,
            "env": mock.ANY,
            "topology_spread_constraints": [
                {
                    "label_selector": {
                        "app.kubernetes.io/managed-by": "tron",
                        "paasta.yelp.com/pool": "special_pool",
                    },
                    "max_skew": 1,
                    "topology_key": "topology.kubernetes.io/zone",
                    "when_unsatisfiable": "ScheduleAnyway",
                },
            ],
            "secret_volumes": [
                {
                    "secret_volume_name": "tron-secret-my--service-secret1",
                    "secret_name": "secret1",
                    "container_path": "/b/c",
                    "default_mode": "0644",
                    "items": [{"key": "secret1", "path": "abc"}],
                }
            ],
            "extra_volumes": [
                {
                    "container_path": "/nail/bulkdata",
                    "host_path": "/nail/bulkdata",
                    "mode": "RO",
                },
                {"container_path": "/nail/tmp", "host_path": "/nail/tmp", "mode": "RW"},
            ],
            "field_selector_env": {"PAASTA_POD_IP": {"field_path": "status.podIP"}},
            "node_selectors": {"yelp.com/pool": "special_pool"},
            "labels": {
                "paasta.yelp.com/cluster": "paasta-dev",
                "paasta.yelp.com/instance": "my_job.do_something",
                "paasta.yelp.com/pool": "special_pool",
                "paasta.yelp.com/service": "my_service",
                "yelp.com/owner": "compute_infra_platform_experience",
                "app.kubernetes.io/managed-by": "tron",
                "tron.yelp.com/idempotent-action": "false",
            },
            "annotations": {
                "paasta.yelp.com/routable_ip": "false",
                "paasta.yelp.com/service": "my_service",
                "paasta.yelp.com/instance": "my_job.do_something",
            },
            "cap_drop": CAPS_DROP,
            "cap_add": [],
            "secret_env": {},
        }
        assert result["env"]["SHELL"] == "/bin/bash"

    @mock.patch("paasta_tools.tron_tools.read_extra_service_information", autospec=True)
    def test_load_tron_service_config(self, mock_read_extra_service_information):
        mock_read_extra_service_information.return_value = {
            "_template": {"actions": {"action1": {}}},
            "job1": {"actions": {"action1": {}}},
        }
        job_configs = tron_tools.load_tron_service_config_no_cache(
            service="service",
            cluster="test-cluster",
            load_deployments=False,
            soa_dir="fake",
        )
        assert job_configs == [
            tron_tools.TronJobConfig(
                name="job1",
                service="service",
                cluster="test-cluster",
                config_dict={"actions": {"action1": {}}},
                load_deployments=False,
                soa_dir="fake",
            )
        ]
        mock_read_extra_service_information.assert_called_once_with(
            service_name="service", extra_info="tron-test-cluster", soa_dir="fake"
        )

    @mock.patch("paasta_tools.tron_tools.read_extra_service_information", autospec=True)
    def test_load_tron_service_config_empty(self, mock_read_extra_service_information):
        mock_read_extra_service_information.return_value = {}
        job_configs = tron_tools.load_tron_service_config_no_cache(
            service="service",
            cluster="test-cluster",
            load_deployments=False,
            soa_dir="fake",
        )
        assert job_configs == []
        mock_read_extra_service_information.assert_called_once_with(
            service_name="service", extra_info="tron-test-cluster", soa_dir="fake"
        )

    @mock.patch(
        "paasta_tools.tron_tools.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    )
    @mock.patch("paasta_tools.tron_tools.load_tron_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.load_tron_service_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.format_tron_job_dict", autospec=True)
    @mock.patch("paasta_tools.tron_tools.yaml.dump", autospec=True)
    @pytest.mark.parametrize("service", [MASTER_NAMESPACE, "my_app"])
    @pytest.mark.parametrize("k8s_enabled", (True, False))
    def test_create_complete_config(
        self,
        mock_yaml_dump,
        mock_format_job,
        mock_tron_service_config,
        mock_tron_system_config,
        mock_system_config,
        service,
        k8s_enabled,
    ):
        job_config = tron_tools.TronJobConfig("my_job", {}, "fake-cluster")
        mock_tron_service_config.return_value = [job_config]
        soa_dir = "/testing/services"
        cluster = "fake-cluster"

        assert (
            tron_tools.create_complete_config(
                service=service,
                cluster=cluster,
                soa_dir=soa_dir,
                k8s_enabled=k8s_enabled,
            )
            == mock_yaml_dump.return_value
        )
        mock_tron_service_config.assert_called_once_with(
            service=service,
            cluster=cluster,
            for_validation=False,
            load_deployments=True,
            soa_dir=soa_dir,
        )
        mock_format_job.assert_called_once_with(
            job_config=job_config, k8s_enabled=k8s_enabled
        )
        complete_config = {"jobs": {"my_job": mock_format_job.return_value}}
        mock_yaml_dump.assert_called_once_with(
            complete_config, Dumper=mock.ANY, default_flow_style=mock.ANY
        )

    def test_create_complete_config_e2e(self, tmpdir):
        soa_dir = tmpdir.mkdir("test_create_complete_config_soa")
        job_file = soa_dir.mkdir("fake_service").join("tron-fake-cluster.yaml")
        job_file.write(
            """
fake_job:
    node: paasta
    time_zone: 'US/Pacific'
    schedule: 'cron 0 * * * *'
    monitoring:
        team: fake_team
        ticket: false
        slack_channels: ['#fake-channel']
    deploy_group: dev
    actions:
        run:
            command: '/bin/true'
            cpus: 0.1
            mem: 1000
            retries: 3
            env:
                PAASTA_ENV_VAR: 'fake_value'
            """
        )

        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            tronfig = tron_tools.create_complete_config(
                service="fake_service",
                cluster="fake-cluster",
                soa_dir=str(soa_dir),
                k8s_enabled=True,
            )

        hasher = hashlib.md5()
        hasher.update(tronfig.encode("UTF-8"))
        # warning: if this hash changes, all tron jobs will be reconfigured (like
        # a long-running service big bounce) the next time `setup_tron_namespace`
        # runs after the change is released. if the change introduces configs
        # that are not static, this will cause continuous reconfiguration, which
        # will add significant load to the Tron API, which happened in DAR-1461.
        # but if this is intended, just change the hash.
        assert hasher.hexdigest() == "5cf9985b6227ce18dc87376b687046f7"

    def test_override_default_pool_override(self, tmpdir):
        soa_dir = tmpdir.mkdir("test_create_complete_config_soa")
        job_file = soa_dir.mkdir("fake_service").join("tron-fake-cluster.yaml")
        job_file.write(
            """
fake_job:
    node: paasta
    time_zone: 'US/Pacific'
    schedule: 'cron 0 * * * *'
    monitoring:
        team: fake_team
        ticket: false
        slack_channels: ['#fake-channel']
    deploy_group: dev
    actions:
        run:
            command: '/bin/true'
            cpus: 0.1
            mem: 1000
            retries: 3
            env:
                PAASTA_ENV_VAR: 'fake_value'
            """
        )
        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG_OVERRIDES,
        ), mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG_OVERRIDES,
        ), mock.patch(
            "paasta_tools.tron_tools.add_volumes_for_authenticating_services",
            autospec=True,
            return_value=[],
        ):
            tronfig = tron_tools.create_complete_config(
                service="fake_service",
                cluster="fake-cluster",
                soa_dir=str(soa_dir),
                k8s_enabled=True,
            )
        print(yaml.safe_load(tronfig)["jobs"]["fake_job"]["actions"]["run"])
        assert (
            yaml.safe_load(tronfig)["jobs"]["fake_job"]["actions"]["run"][
                "node_selectors"
            ]["yelp.com/pool"]
            == MOCK_SYSTEM_PAASTA_CONFIG_OVERRIDES.get_tron_default_pool_override()
        )

    @mock.patch("paasta_tools.tron_tools.load_tron_service_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.format_tron_job_dict", autospec=True)
    @mock.patch("subprocess.run", autospec=True)
    def test_validate_complete_config_paasta_validate_fails(
        self, mock_run, mock_format_job, mock_load_config
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.get_name.return_value = "my_job"
        job_config.validate = mock.Mock(return_value=["some error"])
        mock_load_config.return_value = [job_config]

        result = tron_tools.validate_complete_config("a_service", "a-cluster")

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 0
        assert mock_run.call_count == 0
        assert result == ["some error"]

    @mock.patch("paasta_tools.tron_tools.load_tron_service_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.format_tron_job_dict", autospec=True)
    @mock.patch("subprocess.run", autospec=True)
    def test_validate_complete_config_tronfig_fails(
        self, mock_run, mock_format_job, mock_load_config
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.get_name.return_value = "my_job"
        job_config.validate = mock.Mock(return_value=[])
        mock_load_config.return_value = [job_config]
        mock_format_job.return_value = {}
        mock_run.return_value = mock.Mock(
            returncode=1, stdout="tronfig error", stderr=""
        )

        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ):
            result = tron_tools.validate_complete_config("a_service", "a-cluster")

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 1
        assert mock_run.call_count == 1
        assert result == ["tronfig error"]

    @mock.patch("paasta_tools.tron_tools.load_tron_service_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.format_tron_job_dict", autospec=True)
    @mock.patch("subprocess.run", autospec=True)
    def test_validate_complete_config_passes(
        self,
        mock_run,
        mock_format_job,
        mock_load_config,
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.get_name.return_value = "my_job"
        job_config.validate = mock.Mock(return_value=[])
        mock_load_config.return_value = [job_config]
        mock_format_job.return_value = {}
        mock_run.return_value = mock.Mock(returncode=0, stdout="OK", stderr="")

        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config",
            autospec=True,
            return_value=MOCK_SYSTEM_PAASTA_CONFIG,
        ):
            result = tron_tools.validate_complete_config("a_service", "a-cluster")

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 1
        assert mock_run.call_count == 1
        assert not result

    @mock.patch("os.walk", autospec=True)
    @mock.patch("os.listdir", autospec=True)
    def test_get_tron_namespaces_paasta(self, mock_ls, mock_walk):
        cluster_name = "stage"
        expected_namespaces = ["app", "foo"]
        mock_walk.return_value = [
            ("/my_soa_dir/foo", [], ["tron-stage.yaml"]),
            ("/my_soa_dir/app", [], ["tron-stage.yaml"]),
            ("my_soa_dir/woo", [], ["something-else.yaml"]),
        ]
        soa_dir = "/my_soa_dir"

        with mock.patch(
            "paasta_tools.tron_tools.filter_templates_from_config", autospec=True
        ) as mock_filter_templates_from_config:
            mock_filter_templates_from_config.return_value = {
                "test-tron-job": {"actions": {"run": {"executor": "paasta"}}},
                "test-spark-job": {"actions": {"run": {"executor": "ssh"}}},
            }
            namespaces = tron_tools.get_tron_namespaces(
                cluster=cluster_name,
                soa_dir=soa_dir,
                tron_executors=["paasta"],
            )
            assert sorted(expected_namespaces) == sorted(namespaces)

    @mock.patch("os.walk", autospec=True)
    @mock.patch("os.listdir", autospec=True)
    def test_get_tron_namespaces_spark(self, mock_ls, mock_walk):
        cluster_name = "stage"
        expected_namespaces = ["app", "foo"]
        mock_walk.return_value = [
            ("/my_soa_dir/foo", [], ["tron-stage.yaml"]),
            ("/my_soa_dir/app", [], ["tron-stage.yaml"]),
            ("my_soa_dir/woo", [], ["something-else.yaml"]),
        ]
        soa_dir = "/my_soa_dir"

        with mock.patch(
            "paasta_tools.tron_tools.filter_templates_from_config", autospec=True
        ) as mock_filter_templates_from_config:

            mock_filter_templates_from_config.return_value = {
                "test-spark-job": {"actions": {"run": {"executor": "ssh"}}},
            }
            namespaces = tron_tools.get_tron_namespaces(
                cluster=cluster_name,
                soa_dir=soa_dir,
                tron_executors=["paasta"],
            )
            assert [] == sorted(namespaces)
            namespaces = tron_tools.get_tron_namespaces(
                cluster=cluster_name,
                soa_dir=soa_dir,
                tron_executors=["ssh"],
            )
            assert sorted(expected_namespaces) == sorted(namespaces)

    @mock.patch("glob.glob", autospec=True)
    def test_list_tron_clusters(self, mock_glob):
        mock_glob.return_value = [
            "/home/service/tron-dev-cluster2.yaml",
            "/home/service/tron-prod.yaml",
            "/home/service/kubernetes-other.yaml",
        ]
        result = tron_tools.list_tron_clusters("foo")
        assert sorted(result) == ["dev-cluster2", "prod"]


def test_parse_service_instance_from_executor_id_happy():
    actual = tron_tools.parse_service_instance_from_executor_id(
        "schematizer.traffic_generator.28414.turnstyle.46da87d7-6092-4ed4-b926-ffa7b21c7785"
    )
    assert actual == ("schematizer", "traffic_generator.turnstyle")


def test_parse_service_instance_from_executor_id_sad():
    actual = tron_tools.parse_service_instance_from_executor_id("UNKNOWN")
    assert actual == ("unknown_service", "unknown_job.unknown_action")
