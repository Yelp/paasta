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
import argparse

import mock
import pytest
from boto3.exceptions import Boto3Error
from ruamel.yaml import YAML

from paasta_tools.cli.cmds.spark_run import configure_and_run_docker_container
from paasta_tools.cli.cmds.spark_run import create_spark_config_str
from paasta_tools.cli.cmds.spark_run import emit_resource_requirements
from paasta_tools.cli.cmds.spark_run import get_docker_cmd
from paasta_tools.cli.cmds.spark_run import get_docker_run_cmd
from paasta_tools.cli.cmds.spark_run import get_smart_paasta_instance_name
from paasta_tools.cli.cmds.spark_run import get_spark_config
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import SystemPaastaConfig


@mock.patch("paasta_tools.cli.cmds.spark_run.os.geteuid", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.os.getegid", autospec=True)
def test_get_docker_run_cmd(mock_getegid, mock_geteuid):
    mock_geteuid.return_value = 1234
    mock_getegid.return_value = 100

    container_name = "fake_name"
    volumes = ["v1:v1:rw", "v2:v2:rw"]
    env = {"k1": "v1", "k2": "v2"}
    docker_img = "fake-registry/fake-service"
    docker_cmd = "pyspark"
    nvidia = False

    actual = get_docker_run_cmd(
        container_name, volumes, env, docker_img, docker_cmd, nvidia
    )

    assert actual[5:] == [
        "--user=1234:100",
        "--name=fake_name",
        "--env",
        "k1=v1",
        "--env",
        "k2=v2",
        "--volume=v1:v1:rw",
        "--volume=v2:v2:rw",
        "fake-registry/fake-service",
        "sh",
        "-c",
        "pyspark",
        {},
    ]


@pytest.mark.parametrize("mrjob", [True, False])
def test_get_smart_paasta_instance_name(mrjob):
    args = argparse.Namespace(
        instance="foo", cmd="USER blah spark-submit blah blah blah", mrjob=mrjob,
    )
    with mock.patch(
        "paasta_tools.cli.cmds.spark_run.get_username",
        return_value="root",
        autospec=True,
    ):
        assert (
            get_smart_paasta_instance_name(args) == "foo_root_mrjob"
            if mrjob
            else "foo_root_spark-submit"
        )


def test_get_smart_paasta_instance_name_tron():
    args = argparse.Namespace(
        instance="foo", cmd="spark-submit blah blah blah", mrjob=True,
    )
    with mock.patch(
        "paasta_tools.cli.cmds.spark_run.os.environ",
        dict(
            TRON_JOB_NAMESPACE="master",
            TRON_JOB_NAME="yelp-main",
            TRON_ACTION="rm_rf_slash",
        ),
        autospec=None,
    ):
        assert get_smart_paasta_instance_name(args) == "yelp-main.rm_rf_slash"


class TestGetSparkConfig:

    dev_account_id = "12345"
    dev_log_dir = "s3a://dev/log/path"

    other_account_id = "23456"
    other_log_dir = "s3a://other/log/path"

    unrecognized_account_id = "34567"

    @pytest.fixture(autouse=True)
    def mock_find_mesos_leader(self):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.find_mesos_leader", autospec=True
        ) as m:
            m.return_value = "fake_leader:5050"
            yield m

    @pytest.fixture(autouse=True)
    def mock_account_id(self):
        with mock.patch("boto3.client", autospec=True) as m:
            mock_account_id = m.return_value.get_caller_identity.return_value.get
            mock_account_id.return_value = self.dev_account_id
            yield mock_account_id

    @pytest.fixture(autouse=True)
    def mock_spark_run_config(self, tmpdir):
        spark_run_file = str(tmpdir.join("spark_config.yaml"))
        spark_run_conf = {
            "environments": {
                "dev": {
                    "account_id": self.dev_account_id,
                    "default_event_log_dir": self.dev_log_dir,
                },
                "test_dev": {
                    "account_id": self.other_account_id,
                    "default_event_log_dir": self.other_log_dir,
                },
            }
        }
        with open(spark_run_file, "w") as fp:
            YAML().dump(spark_run_conf, fp)
        yield spark_run_file

    @pytest.fixture(autouse=True)
    def mock_load_mesos_secret_for_spark(self):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.load_mesos_secret_for_spark", autospec=True
        ) as m:
            yield m

    def get_spark_config(self, paasta_args=None):
        paasta_args = paasta_args or {}
        args = mock.MagicMock()
        for k, v in paasta_args.items():
            setattr(args, k, v)
        if "cluster" not in paasta_args:
            args.cluster = "fake_cluster"
        return get_spark_config(
            args=args,
            spark_app_name="fake_name",
            spark_ui_port=123,
            docker_img="fake-registry/fake-service",
            system_paasta_config=SystemPaastaConfig(
                {"cluster_fqdn_format": "paasta-{cluster:s}.something"}, "fake_dir"
            ),
            volumes=["v1:v1:rw", "v2:v2:rw"],
            access_key="test_access_key",
            secret_key="test_secret_key",
        )

    def test_find_master(self):
        spark_conf = self.get_spark_config()
        assert spark_conf["spark.master"] == "mesos://fake_leader:5050"
        assert "spark.master=mesos://fake_leader:5050" in create_spark_config_str(
            spark_conf, is_mrjob=False
        )

    @pytest.mark.parametrize(
        "spark_args,expected_partitions",
        [
            ("spark.cores.max=10", 20),
            ("spark.cores.max=10 spark.sql.shuffle.partitions=14", 14),
        ],
    )
    def test_default_shuffle_partitions(self, spark_args, expected_partitions):
        spark_conf = self.get_spark_config({"spark_args": spark_args})
        assert int(spark_conf["spark.sql.shuffle.partitions"]) == expected_partitions

    @pytest.mark.parametrize(
        "default_event_log_dir,spark_args,expected_spark_config",
        [
            # do not override if user disabled the eventlog
            (
                dev_log_dir,
                "spark.eventLog.enabled=false",
                {"spark.eventLog.enabled": "false"},
            ),
            # do not override if user manually configure logDir
            (
                dev_log_dir,
                "spark.eventLog.dir=s3a://fake-location",
                {
                    "spark.eventLog.enabled": "true",
                    "spark.eventLog.dir": "s3a://fake-location",
                },
            ),
            # use specified dir if account id is found in config
            (
                dev_log_dir,
                None,
                {"spark.eventLog.enabled": "true", "spark.eventLog.dir": dev_log_dir},
            ),
            # do not enable if no default_event_log_dir found in config
            (None, None, {"spark.eventLog.enabled": None, "spark.eventLog.dir": None}),
        ],
    )
    def test_event_logging(
        self, default_event_log_dir, spark_args, expected_spark_config
    ):
        args = {}
        if spark_args is not None:
            args["spark_args"] = spark_args

        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.get_default_event_log_dir", autospec=True
        ) as m:
            m.return_value = default_event_log_dir
            spark_conf = self.get_spark_config(args)

        for key, expected_value in expected_spark_config.items():
            if expected_value is None:
                assert key not in spark_conf
            else:
                assert spark_conf[key] == expected_value


@mock.patch("paasta_tools.cli.cmds.spark_run.get_aws_credentials", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.os.path.exists", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.pick_random_port", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_username", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_spark_config", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.run_docker_container", autospec=True)
@mock.patch("time.time", autospec=True)
class TestConfigureAndRunDockerContainer:

    instance_config = InstanceConfig(
        cluster="fake_cluster",
        instance="fake_instance",
        service="fake_service",
        config_dict={
            "extra_volumes": [{"hostPath": "/h1", "containerPath": "/c1", "mode": "RO"}]
        },
        branch_dict={"docker_image": "fake_service:fake_sha"},
    )

    system_paasta_config = SystemPaastaConfig(
        {"volumes": [{"hostPath": "/h2", "containerPath": "/c2", "mode": "RO"}]},
        "fake_dir",
    )

    @pytest.fixture
    def mock_create_spark_config_str(self):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.create_spark_config_str", autospec=True
        ) as _mock_create_spark_config_str:
            yield _mock_create_spark_config_str

    def test_configure_and_run_docker_container(
        self,
        mock_time,
        mock_run_docker_container,
        mock_get_spark_config,
        mock_get_username,
        mock_pick_random_port,
        mock_os_path_exists,
        mock_get_aws_credentials,
    ):
        mock_pick_random_port.return_value = 123
        mock_get_username.return_value = "fake_user"
        mock_get_spark_config.return_value = {"spark.app.name": "fake_app"}
        mock_run_docker_container.return_value = 0
        mock_get_aws_credentials.return_value = ("id", "secret")

        args = mock.MagicMock()
        args.aws_region = "fake_region"
        args.cluster = "fake_cluster"
        args.cmd = "pyspark"
        args.work_dir = "/fake_dir:/spark_driver"
        args.dry_run = True
        args.mrjob = False
        args.nvidia = False

        with mock.patch(
            "paasta_tools.utils.get_service_push_docker_registries",
            autospec=True,
            return_value=["fake-registry"],
        ):
            retcode = configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
            )

        assert retcode == 0
        mock_run_docker_container.assert_called_once_with(
            container_name="paasta_spark_run_fake_user_123",
            volumes=[
                "/h1:/c1:ro",
                "/h2:/c2:ro",
                "/fake_dir:/spark_driver:rw",
                "/etc/passwd:/etc/passwd:ro",
                "/etc/group:/etc/group:ro",
                "/nail/home:/nail/home:rw",
            ],
            environment={
                "PAASTA_SERVICE": "fake_service",
                "PAASTA_INSTANCE": "fake_instance",
                "PAASTA_CLUSTER": "fake_cluster",
                "PAASTA_INSTANCE_TYPE": "spark",
                "PAASTA_DEPLOY_GROUP": "fake_cluster.fake_instance",
                "PAASTA_DOCKER_IMAGE": "fake_service:fake_sha",
                "PAASTA_LAUNCHED_BY": mock.ANY,
                "AWS_ACCESS_KEY_ID": "id",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_DEFAULT_REGION": "fake_region",
                "SPARK_USER": "root",
                "SPARK_OPTS": "--conf spark.app.name=fake_app",
                "PAASTA_RESOURCE_CPUS": "0.25",
                "PAASTA_RESOURCE_DISK": "1024",
                "PAASTA_RESOURCE_MEM": "1024",
                "PAASTA_GIT_SHA": "fake_ser",
            },
            docker_img="fake-registry/fake-service",
            docker_cmd="pyspark --conf spark.app.name=fake_app",
            dry_run=True,
            nvidia=False,
        )

    def test_configure_and_run_docker_container_nvidia(
        self,
        mock_time,
        mock_run_docker_container,
        mock_get_spark_config,
        mock_get_username,
        mock_pick_random_port,
        mock_os_path_exists,
        mock_get_aws_credentials,
    ):
        mock_get_aws_credentials.return_value = ("id", "secret")
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.emit_resource_requirements", autospec=True
        ) as mock_emit_resource_requirements, mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_get_spark_config.return_value = {
                "spark.cores.max": 5,
                "spark.master": "mesos://spark.master",
            }
            args = mock.MagicMock(cmd="pyspark", nvidia=True)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
            )

            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["nvidia"]
            assert mock_emit_resource_requirements.called

    def test_configure_and_run_docker_container_mrjob(
        self,
        mock_time,
        mock_run_docker_container,
        mock_get_spark_config,
        mock_get_username,
        mock_pick_random_port,
        mock_os_path_exists,
        mock_get_aws_credentials,
    ):
        mock_get_aws_credentials.return_value = ("id", "secret")
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.emit_resource_requirements", autospec=True
        ) as mock_emit_resource_requirements, mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_get_spark_config.return_value = {
                "spark.cores.max": 5,
                "spark.master": "mesos://spark.master",
            }
            args = mock.MagicMock(cmd="python mrjob_wrapper.py", mrjob=True)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
            )

            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["docker_cmd"] == (
                "python mrjob_wrapper.py --spark-master=mesos://spark.master --jobconf spark.cores.max=5"
            )

            assert mock_emit_resource_requirements.called

    def test_suppress_clusterman_metrics_errors(
        self,
        mock_time,
        mock_run_docker_container,
        mock_get_spark_config,
        mock_get_username,
        mock_pick_random_port,
        mock_os_path_exists,
        mock_get_aws_credentials,
        mock_create_spark_config_str,
    ):
        mock_get_aws_credentials.return_value = ("id", "secret")

        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.emit_resource_requirements", autospec=True
        ) as mock_emit_resource_requirements, mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_emit_resource_requirements.side_effect = Boto3Error
            mock_create_spark_config_str.return_value = "--conf spark.cores.max=5"

            args = mock.MagicMock(
                suppress_clusterman_metrics_errors=False, cmd="pyspark"
            )
            with pytest.raises(Boto3Error):
                configure_and_run_docker_container(
                    args=args,
                    docker_img="fake-registry/fake-service",
                    instance_config=self.instance_config,
                    system_paasta_config=self.system_paasta_config,
                )

            # make sure we don't blow up when this setting is True
            args.suppress_clusterman_metrics_errors = True
            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
            )

    def test_dont_emit_metrics_for_inappropriate_commands(
        self,
        mock_time,
        mock_run_docker_container,
        mock_get_spark_config,
        mock_get_username,
        mock_pick_random_port,
        mock_os_path_exists,
        mock_get_aws_credentials,
        mock_create_spark_config_str,
    ):
        mock_get_aws_credentials.return_value = ("id", "secret")
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.emit_resource_requirements", autospec=True
        ) as mock_emit_resource_requirements, mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_create_spark_config_str.return_value = "--conf spark.cores.max=5"
            args = mock.MagicMock(cmd="bash", mrjob=False)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
            )
            assert not mock_emit_resource_requirements.called


def test_emit_resource_requirements(tmpdir):
    spark_config_dict = {
        "spark.executor.cores": "2",
        "spark.cores.max": "4",
        "spark.executor.memory": "4g",
        "spark.mesos.executor.memoryOverhead": "555",
        "spark.app.name": "paasta_spark_run_johndoe_2_3",
        "spark.mesos.constraints": "pool:cool-pool\\;other:value",
    }

    clusterman_yaml_contents = {
        "clusters": {"anywhere-prod": {"aws_region": "us-north-14"}}
    }
    clusterman_yaml_file_path = tmpdir.join("fake_clusterman.yaml")
    with open(clusterman_yaml_file_path, "w") as f:
        YAML().dump(clusterman_yaml_contents, f)

    with mock.patch(
        "paasta_tools.cli.cmds.spark_run.get_clusterman_metrics", autospec=True
    ), mock.patch(
        "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
    ) as mock_clusterman_metrics, mock.patch(
        "paasta_tools.cli.cmds.spark_run.CLUSTERMAN_YAML_FILE_PATH",
        clusterman_yaml_file_path,
        autospec=None,  # we're replacing this name, so we can't autospec
    ), mock.patch(
        "time.time", return_value=1234, autospec=True
    ):
        mock_clusterman_metrics.generate_key_with_dimensions.side_effect = lambda name, dims: (
            f'{name}|framework_name={dims["framework_name"]},webui_url={dims["webui_url"]}'
        )

        emit_resource_requirements(
            spark_config_dict, "anywhere-prod", "http://spark.yelp"
        )

        mock_clusterman_metrics.ClustermanMetricsBotoClient.assert_called_once_with(
            region_name="us-north-14", app_identifier="cool-pool"
        )
        metrics_writer = (
            mock_clusterman_metrics.ClustermanMetricsBotoClient.return_value.get_writer.return_value.__enter__.return_value
        )

        metric_key_template = "requested_{resource}|framework_name=paasta_spark_run_johndoe_2_3,webui_url=http://spark.yelp"

        expected_memory_request = (4 * 1024 + 555) * 2

        metrics_writer.send.assert_has_calls(
            [
                mock.call((metric_key_template.format(resource="cpus"), 1234, 4)),
                mock.call(
                    (
                        metric_key_template.format(resource="mem"),
                        1234,
                        expected_memory_request,
                    )
                ),
                mock.call(
                    (
                        metric_key_template.format(resource="disk"),
                        1234,
                        expected_memory_request,
                    )
                ),
            ],
            any_order=True,
        )


def test_get_docker_cmd_add_spark_conf_str():
    args = mock.Mock(cmd="pyspark -v", mrjob=False)
    instance_config = None
    spark_conf_str = "--conf spark.app.name=fake_app"

    docker_cmd = get_docker_cmd(args, instance_config, spark_conf_str)
    assert docker_cmd == "pyspark --conf spark.app.name=fake_app -v"


def test_get_docker_cmd_other_cmd():
    args = mock.Mock(cmd="bash", mrjob=False)
    instance_config = None
    spark_conf_str = "--conf spark.app.name=fake_app"

    assert get_docker_cmd(args, instance_config, spark_conf_str) == "bash"


def test_get_docker_cmd_mrjob():
    args = mock.Mock(cmd="python mrjob_wrapper.py", mrjob=True)
    instance_config = None
    spark_conf_str = "--jobconf spark.app.name=fake_app"

    expected_cmd = "python mrjob_wrapper.py --jobconf spark.app.name=fake_app"
    assert get_docker_cmd(args, instance_config, spark_conf_str) == expected_cmd
