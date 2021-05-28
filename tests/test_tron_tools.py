import datetime

import mock
import pytest

from paasta_tools import tron_tools
from paasta_tools.tron_tools import MASTER_NAMESPACE
from paasta_tools.tron_tools import MESOS_EXECUTOR_NAMES
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import NoDeploymentsAvailable


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

    @pytest.fixture
    def spark_action_config(self):
        action_dict = {
            "name": "print",
            "command": "spark-submit something",
            "aws_credentials_yaml": "/some/yaml/path",
            "executor": "spark",
            "spark_args": {"spark.eventLog.enabled": "false"},
            "spark_paasta_cluster": "fake-spark-cluster",
            "spark_paasta_pool": "fake-spark-pool",
            "extra_volumes": [
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"}
            ],
        }
        return tron_tools.TronActionConfig(
            service="my_service",
            instance=tron_tools.compose_instance("cool_job", "print"),
            cluster="fake-cluster",
            config_dict=action_dict,
            branch_dict={"docker_image": ""},
        )

    def test_action_config(self, action_config):
        assert action_config.get_job_name() == "cool_job"
        assert action_config.get_action_name() == "print"
        assert action_config.get_cluster() == "fake-cluster"

    @pytest.mark.parametrize("for_validation", [True, "N/A"])
    @pytest.mark.parametrize("cluster_manager", ["mesos", "kubernetes"])
    def test_get_spark_config_dict(
        self, spark_action_config, for_validation, cluster_manager
    ):
        spark_action_config.config_dict["spark_cluster_manager"] = cluster_manager
        spark_action_config.for_validation = for_validation
        with mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config", autospec=True
        ) as system_paasta_config, mock.patch(
            "paasta_tools.tron_tools.get_spark_conf", autospec=True
        ) as mock_get_spark_conf, mock.patch(
            "paasta_tools.tron_tools.get_aws_credentials", autospec=True
        ) as mock_get_aws_credentials:
            if cluster_manager == "mesos":
                expected_mesos_leader = (
                    "N/A" if for_validation else "zk://1.2.3.4/mesos"
                )
                expected_extra_volumes = [
                    {
                        "containerPath": "/nail/tmp",
                        "hostPath": "/nail/tmp",
                        "mode": "RW",
                    }
                ]
                if not for_validation:
                    system_paasta_config.return_value.get_zk_hosts.return_value = (
                        "1.2.3.4/mesos"
                    )
            elif cluster_manager == "kubernetes":
                expected_mesos_leader = None
                expected_extra_volumes = [
                    {
                        "hostPath": "/etc/pki/spark",
                        "containerPath": "/etc/spark_k8s_secrets",
                        "mode": "RO",
                    },
                    {
                        "containerPath": "/nail/tmp",
                        "hostPath": "/nail/tmp",
                        "mode": "RW",
                    },
                ]

            spark_action_config.get_spark_config_dict()
            mock_get_spark_conf.assert_called_once_with(
                cluster_manager=cluster_manager,
                spark_app_base_name="tron_spark_my_service_cool_job.print",
                user_spark_opts={"spark.eventLog.enabled": "false"},
                paasta_cluster="fake-spark-cluster",
                paasta_pool="fake-spark-pool",
                paasta_service="my_service",
                paasta_instance="cool_job.print",
                docker_img="",
                aws_creds=mock_get_aws_credentials.return_value,
                extra_volumes=expected_extra_volumes,
                with_secret=False,
                mesos_leader=expected_mesos_leader,
                load_paasta_default_volumes=False,
            )

    @pytest.mark.parametrize("executor", MESOS_EXECUTOR_NAMES)
    def test_get_env(self, action_config, executor, monkeypatch):
        monkeypatch.setattr(tron_tools, "clusterman_metrics", mock.Mock())
        action_config.config_dict["executor"] = executor
        with mock.patch(
            "paasta_tools.utils.get_service_docker_registry", autospec=True,
        ), mock.patch(
            "paasta_tools.tron_tools.stringify_spark_env", autospec=True,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config", autospec=True
        ), mock.patch(
            "paasta_tools.tron_tools.get_aws_credentials",
            autospec=True,
            return_value=("access", "secret", "token"),
        ), mock.patch(
            "paasta_tools.tron_tools.generate_clusterman_metrics_entries",
            autospec=True,
            return_value={
                "cpus": ("cpus|dimension=2", 1900),
                "mem": ("mem|dimension=1", "42"),
            },
        ):
            env = action_config.get_env()
            if executor == "spark":
                assert all([env["SPARK_OPTS"], env["CLUSTERMAN_RESOURCES"]])
                assert env["AWS_ACCESS_KEY_ID"] == "access"
                assert env["AWS_SECRET_ACCESS_KEY"] == "secret"
                assert env["AWS_DEFAULT_REGION"] == "us-west-2"
                assert env["SPARK_MESOS_SECRET"] == "SHARED_SECRET(SPARK_MESOS_SECRET)"
            else:
                assert not any([env.get("SPARK_OPTS"), env.get("CLUSTERMAN_RESOURCES")])

    def test_spark_get_cmd(self, action_config):
        action_config.config_dict["executor"] = "spark"
        with mock.patch.object(
            action_config,
            "get_spark_config_dict",
            return_value={"spark.master": "mesos://host:port"},
        ), mock.patch(
            "paasta_tools.utils.get_service_docker_registry", autospec=True,
        ), mock.patch(
            "paasta_tools.tron_tools.load_system_paasta_config", autospec=True
        ), mock.patch(
            "paasta_tools.tron_tools.get_aws_credentials",
            autospec=True,
            return_value=("access", "secret", "token"),
        ):
            assert (
                action_config.get_cmd()
                == "unset MESOS_DIRECTORY MESOS_SANDBOX; spark-submit --conf spark.master=mesos://host:port something"
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
        "action_service,action_deploy",
        [
            (None, None),
            (None, "special_deploy"),
            ("other_service", None),
            (None, None),
            (None, None),
        ],
    )
    @mock.patch("paasta_tools.tron_tools.load_v2_deployments_json", autospec=True)
    def test_get_action_config(
        self, mock_load_deployments, action_service, action_deploy
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
        expected_cluster = "paasta-dev"

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
            "my_job", job_dict, expected_cluster, soa_dir=soa_dir
        )

        action_config = job_config._get_action_config("normal", action_dict=action_dict)

        mock_load_deployments.assert_called_once_with(expected_service, soa_dir)
        mock_deployments_json = mock_load_deployments.return_value
        mock_deployments_json.get_docker_image_for_deploy_group.assert_called_once_with(
            expected_deploy
        )
        mock_deployments_json.get_git_sha_for_deploy_group.assert_called_once_with(
            expected_deploy
        )
        expected_branch_dict = {
            "docker_image": mock_deployments_json.get_docker_image_for_deploy_group.return_value,
            "git_sha": mock_deployments_json.get_git_sha_for_deploy_group.return_value,
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

    @mock.patch("paasta_tools.tron_tools.load_v2_deployments_json", autospec=True)
    def test_get_action_config_load_deployments_false(self, mock_load_deployments):
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
        result = tron_tools.format_tron_job_dict(
            job_config=job_config, k8s_enabled=False
        )

        mock_get_action_config.assert_called_once_with(
            job_config, action_name, action_dict
        )
        mock_format_action.assert_called_once_with(
            action_config=mock_get_action_config.return_value, use_k8s=False,
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
            "use_k8s": False,
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
            "use_k8s": True,
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
        result = tron_tools.format_tron_job_dict(
            job_config=job_config, k8s_enabled=True
        )

        mock_get_action_config.assert_called_once_with(
            job_config, action_name, action_dict
        )
        mock_format_action.assert_called_once_with(
            action_config=mock_get_action_config.return_value, use_k8s=True,
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
            "use_k8s": True,
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
            "use_k8s": False,
        }

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    def test_validate_all_actions(self, mock_get_pipeline_deploy_groups):
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
    def test_validate_invalid_deploy_group(self, mock_get_pipeline_deploy_groups):
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
    def test_validate_valid_deploy_group(self, mock_get_pipeline_deploy_groups):
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
    def test_validate_invalid_action_deploy_group(
        self, mock_get_pipeline_deploy_groups
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
    def test_validate_action_valid_deploy_group(self, mock_get_pipeline_deploy_groups):
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

    @mock.patch("paasta_tools.utils.get_pipeline_deploy_groups", autospec=True)
    def test_validate_monitoring(self, mock_get_pipeline_deploy_groups):
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
    def test_validate_monitoring_without_team(self, mock_get_pipeline_deploy_groups):
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
    def test_validate_monitoring_with_invalid_team(
        self, mock_get_pipeline_deploy_groups
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
    @mock.patch("paasta_tools.tron_tools.load_system_paasta_config", autospec=True)
    def test_load_tron_config(self, mock_system_paasta_config):
        result = tron_tools.load_tron_config()
        assert mock_system_paasta_config.return_value.get_tron_config.call_count == 1
        assert result == tron_tools.TronConfig(
            mock_system_paasta_config.return_value.get_tron_config.return_value
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
        ), mock.patch("paasta_tools.utils.load_system_paasta_config", autospec=True):
            result = tron_tools.format_tron_action_dict(action_config)
        assert result["executor"] == "mesos"

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
            "extra_volumes": [
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"}
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
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        assert result == {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "docker_image": mock.ANY,
            "executor": "mesos",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "env": mock.ANY,
            "extra_volumes": [
                {"container_path": "/nail/tmp", "host_path": "/nail/tmp", "mode": "RW"}
            ],
            "docker_parameters": mock.ANY,
            "constraints": [
                {"attribute": "pool", "operator": "LIKE", "value": "special_pool"}
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
        }
        expected_docker = "{}/{}".format(
            "docker-registry.com:400", branch_dict["docker_image"]
        )
        assert result["docker_image"] == expected_docker
        assert result["env"]["SHELL"] == "/bin/bash"
        assert isinstance(result["docker_parameters"], list)

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
            "extra_volumes": [
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"}
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
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        assert result == {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "docker_image": "",
            "executor": "mesos",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "env": mock.ANY,
            "extra_volumes": [
                {"container_path": "/nail/tmp", "host_path": "/nail/tmp", "mode": "RW"}
            ],
            "docker_parameters": mock.ANY,
            "constraints": [
                {"attribute": "pool", "operator": "LIKE", "value": "special_pool"}
            ],
        }
        assert result["env"]["SHELL"] == "/bin/bash"
        assert isinstance(result["docker_parameters"], list)

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

    @mock.patch("paasta_tools.tron_tools.load_system_paasta_config", autospec=True)
    @pytest.mark.parametrize(
        "spark_cluster_manager, expected_extra_volumes",
        [
            (
                "mesos",
                [
                    {
                        "container_path": "/nail/tmp",
                        "host_path": "/nail/tmp",
                        "mode": "RW",
                    },
                ],
            ),
            (
                "kubernetes",
                [
                    {
                        "container_path": "/nail/tmp",
                        "host_path": "/nail/tmp",
                        "mode": "RW",
                    },
                    {
                        "container_path": "/etc/spark_k8s_secrets",
                        "host_path": "/etc/pki/spark",
                        "mode": "RO",
                    },
                ],
            ),
        ],
    )
    def test_format_tron_action_dict_spark(
        self, mock_system_paasta_config, spark_cluster_manager, expected_extra_volumes,
    ):
        action_dict = {
            "command": "echo something",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "service": "my_service",
            "deploy_group": "prod",
            "executor": "spark",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "pool": "special_pool",
            "env": {"SHELL": "/bin/bash"},
            "extra_volumes": [
                {"containerPath": "/nail/tmp", "hostPath": "/nail/tmp", "mode": "RW"}
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
            "spark_args": {"spark.eventLog.enabled": "false"},
            "spark_cluster_manager": spark_cluster_manager,
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
        ), mock.patch.object(action_config, "get_env", return_value={}), mock.patch(
            "paasta_tools.utils.InstanceConfig.use_docker_disk_quota",
            autospec=True,
            return_value=False,
        ), mock.patch.object(
            action_config, "get_spark_config_dict", return_value={},
        ):
            result = tron_tools.format_tron_action_dict(action_config)

        assert result == {
            "command": "unset MESOS_DIRECTORY MESOS_SANDBOX; echo something",
            "requires": ["required_action"],
            "retries": 2,
            "retries_delay": "5m",
            "docker_image": mock.ANY,
            "executor": "mesos",
            "cpus": 2,
            "mem": 1200,
            "disk": 42,
            "env": mock.ANY,
            "extra_volumes": expected_extra_volumes,
            "docker_parameters": mock.ANY,
            "constraints": [
                {"attribute": "pool", "operator": "LIKE", "value": "special_pool"}
            ],
            "trigger_downstreams": True,
            "triggered_by": ["foo.bar.{shortdate}"],
            "trigger_timeout": "5m",
        }
        expected_docker = "{}/{}".format(
            "docker-registry.com:400", branch_dict["docker_image"]
        )
        assert result["docker_image"] == expected_docker
        assert isinstance(result["docker_parameters"], list)
        assert {"key": "net", "value": "host"} in result["docker_parameters"]

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

    @mock.patch("paasta_tools.tron_tools.load_system_paasta_config", autospec=True)
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
            service=service, cluster=cluster, load_deployments=True, soa_dir=soa_dir
        )
        mock_format_job.assert_called_once_with(
            job_config=job_config, k8s_enabled=k8s_enabled
        )
        complete_config = {"jobs": {"my_job": mock_format_job.return_value}}
        mock_yaml_dump.assert_called_once_with(
            complete_config, Dumper=mock.ANY, default_flow_style=mock.ANY
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

        result = tron_tools.validate_complete_config("a_service", "a-cluster")

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 1
        assert mock_run.call_count == 1
        assert result == ["tronfig error"]

    @mock.patch("paasta_tools.tron_tools.load_tron_service_config", autospec=True)
    @mock.patch("paasta_tools.tron_tools.format_tron_job_dict", autospec=True)
    @mock.patch("subprocess.run", autospec=True)
    def test_validate_complete_config_passes(
        self, mock_run, mock_format_job, mock_load_config
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.get_name.return_value = "my_job"
        job_config.validate = mock.Mock(return_value=[])
        mock_load_config.return_value = [job_config]
        mock_format_job.return_value = {}
        mock_run.return_value = mock.Mock(returncode=0, stdout="OK", stderr="")

        result = tron_tools.validate_complete_config("a_service", "a-cluster")

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 1
        assert mock_run.call_count == 1
        assert not result

    @mock.patch("os.walk", autospec=True)
    @mock.patch("os.listdir", autospec=True)
    def test_get_tron_namespaces(self, mock_ls, mock_walk):
        cluster_name = "stage"
        expected_namespaces = ["app", "foo"]
        mock_walk.return_value = [
            ("/my_soa_dir/foo", [], ["tron-stage.yaml"]),
            ("/my_soa_dir/app", [], ["tron-stage.yaml"]),
            ("my_soa_dir/woo", [], ["something-else.yaml"]),
        ]
        soa_dir = "/my_soa_dir"

        namespaces = tron_tools.get_tron_namespaces(
            cluster=cluster_name, soa_dir=soa_dir
        )
        assert sorted(expected_namespaces) == sorted(namespaces)

    @mock.patch("glob.glob", autospec=True)
    def test_list_tron_clusters(self, mock_glob):
        mock_glob.return_value = [
            "/home/service/tron-dev-cluster2.yaml",
            "/home/service/tron-prod.yaml",
            "/home/service/marathon-other.yaml",
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
