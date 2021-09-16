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

from paasta_tools.cli.cmds import spark_run
from paasta_tools.cli.cmds.spark_run import configure_and_run_docker_container
from paasta_tools.cli.cmds.spark_run import get_docker_run_cmd
from paasta_tools.cli.cmds.spark_run import get_smart_paasta_instance_name
from paasta_tools.cli.cmds.spark_run import get_spark_app_name
from paasta_tools.cli.cmds.spark_run import sanitize_container_name
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


@pytest.mark.parametrize(
    "container_name,expected",
    [
        # name should always start with [a-zA-Z0-9]
        ("~!.abcde", "abcde"),
        # name with none supported chars will be replace with _
        ("to~be?or not to be!", "to_be_or_not_to_be_"),
    ],
)
def test_sanitize_container_name(container_name, expected):
    assert sanitize_container_name(container_name) == expected


@pytest.fixture
def mock_build_and_push_docker_image():
    with mock.patch.object(
        spark_run, "build_and_push_docker_image", return_value="built-docker-image"
    ) as m:
        yield m


@pytest.fixture
def mock_instance_config():
    instance_config = mock.Mock()
    instance_config.get_docker_url.return_value = "docker-image-from-instance-config"
    return instance_config


@pytest.fixture
def mock_run():
    with mock.patch.object(spark_run, "_run") as m:
        yield m


@pytest.mark.parametrize(
    "args,expected_output",
    [
        (argparse.Namespace(build=True, image=None), "built-docker-image"),
        (
            argparse.Namespace(build=False, image="docker-image-from-args"),
            "docker-image-from-args",
        ),
        (
            argparse.Namespace(build=False, image=None),
            "docker-image-from-instance-config",
        ),
    ],
)
def test_get_docker_image(
    args,
    expected_output,
    mock_instance_config,
    mock_build_and_push_docker_image,
    mock_run,
):
    mock_run.return_value = (0, "done")
    assert spark_run.get_docker_image(args, mock_instance_config) == expected_output


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


@pytest.fixture
def mock_create_spark_config_str():
    with mock.patch.object(spark_run, "create_spark_config_str") as m:
        yield m


@pytest.fixture
def mock_get_possible_launced_by_user_variable_from_env():
    with mock.patch.object(
        spark_run, "get_possible_launched_by_user_variable_from_env"
    ) as m:
        yield m


@pytest.mark.parametrize(
    "args,extra_expected",
    [
        (
            argparse.Namespace(
                cmd="jupyter-lab", aws_region="test-region", mrjob=False
            ),
            {
                "JUPYTER_RUNTIME_DIR": "/source/.jupyter",
                "JUPYTER_DATA_DIR": "/source/.jupyter",
                "JUPYTER_CONFIG_DIR": "/source/.jupyter",
            },
        ),
        (
            argparse.Namespace(
                cmd="history-server",
                aws_region="test-region",
                mrjob=False,
                spark_args="spark.history.fs.logDirectory=s3a://bucket",
                work_dir="/first:/second",
            ),
            {
                "SPARK_LOG_DIR": "/second",
                "SPARK_HISTORY_OPTS": "-Dspark.history.fs.logDirectory=s3a://bucket -Dspark.history.ui.port=1234",
                "SPARK_DAEMON_CLASSPATH": "/opt/spark/extra_jars/*",
                "SPARK_NO_DAEMONIZE": "true",
            },
        ),
        (
            argparse.Namespace(
                cmd="spark-submit job.py", aws_region="test-region", mrjob=True
            ),
            {},
        ),
    ],
)
@pytest.mark.parametrize(
    "aws,expected_aws",
    [
        ((None, None, None), {}),
        (
            ("access-key", "secret-key", "token"),
            {
                "AWS_ACCESS_KEY_ID": "access-key",
                "AWS_SECRET_ACCESS_KEY": "secret-key",
                "AWS_SESSION_TOKEN": "token",
                "AWS_DEFAULT_REGION": "test-region",
            },
        ),
    ],
)
def test_get_spark_env(
    args,
    extra_expected,
    aws,
    expected_aws,
    mock_get_possible_launced_by_user_variable_from_env,
):
    spark_conf_str = "--conf spark.ui.port=1234"
    expected_output = {
        "SPARK_USER": "root",
        "SPARK_OPTS": "--conf spark.ui.port=1234",
        "PAASTA_LAUNCHED_BY": mock_get_possible_launced_by_user_variable_from_env.return_value,
        "PAASTA_INSTANCE_TYPE": "spark",
        **extra_expected,
        **expected_aws,
    }
    assert spark_run.get_spark_env(args, spark_conf_str, aws, "1234") == expected_output


@pytest.mark.parametrize(
    "spark_args,expected",
    [
        (
            "spark.cores.max=1  spark.executor.memory=24g",
            {"spark.cores.max": "1", "spark.executor.memory": "24g"},
        ),
        ("spark.cores.max", None),
        (None, {}),
    ],
)
def test_parse_user_spark_args(spark_args, expected, capsys):
    if expected is not None:
        assert spark_run._parse_user_spark_args(spark_args) == expected
    else:
        with pytest.raises(SystemExit):
            spark_run._parse_user_spark_args(spark_args)
            assert (
                capsys.readouterr().err
                == "Spark option spark.cores.max is not in format option=value."
            )


@pytest.mark.parametrize("is_mrjob", [True, False])
def test_create_spark_config_str(is_mrjob):
    spark_opts = {
        "spark.master": "mesos://some-host:5050",
        "spark.executor.memory": "4g",
        "spark.max.cores": "10",
    }
    output = spark_run.create_spark_config_str(spark_opts, is_mrjob)
    if is_mrjob:
        assert output == (
            "--spark-master=mesos://some-host:5050 "
            "--jobconf spark.executor.memory=4g "
            "--jobconf spark.max.cores=10"
        )
    else:
        assert output == (
            "--conf spark.master=mesos://some-host:5050 "
            "--conf spark.executor.memory=4g "
            "--conf spark.max.cores=10"
        )


@pytest.fixture
def mock_get_docker_run_cmd():
    with mock.patch.object(spark_run, "get_docker_run_cmd") as m:
        m.return_value = ["docker", "run", "commands"]
        yield m


@pytest.fixture
def mock_os_execlpe():
    with mock.patch("os.execlpe", autospec=True) as m:
        yield m


@pytest.mark.parametrize("dry_run", [True, False])
def test_run_docker_container(
    mock_get_docker_run_cmd, mock_os_execlpe, dry_run, capsys
):
    container_name = "test-container-name"
    volumes = [{"hostPath": "/host", "containerPath": "/container", "mode": "RO"}]
    env = {"SPARK_OPTS": "--conf spark.cores.max=1"}
    docker_img = "docker-image"
    docker_cmd = "spark-submit --conf spark.cores.max=1"
    nvidia = False

    spark_run.run_docker_container(
        container_name=container_name,
        volumes=volumes,
        environment=env,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        dry_run=dry_run,
        nvidia=nvidia,
    )
    mock_get_docker_run_cmd.assert_called_once_with(
        container_name=container_name,
        volumes=volumes,
        env=env,
        docker_img=docker_img,
        docker_cmd=docker_cmd,
        nvidia=nvidia,
    )

    if dry_run:
        assert not mock_os_execlpe.called
        assert capsys.readouterr().out == '["docker", "run", "commands"]\n'

    else:
        mock_os_execlpe.assert_called_once_with(
            "paasta_docker_wrapper", "docker", "run", "commands"
        )


@mock.patch("paasta_tools.cli.cmds.spark_run.get_username", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.run_docker_container", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.spark_run.send_and_calculate_resources_cost",
    autospec=True,
    return_value=(10, {"cpus": 10, "mem": 1024}),
)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_webui_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.create_spark_config_str", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_docker_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_signalfx_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_history_url", autospec=True)
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

    @pytest.mark.parametrize(
        ["cluster_manager", "spark_args_volumes", "expected_volumes"],
        [
            (
                spark_run.CLUSTER_MANAGER_MESOS,
                {
                    "spark.mesos.executor.docker.volumes": "/mesos/volume:/mesos/volume:rw"
                },
                ["/mesos/volume:/mesos/volume:rw"],
            ),
            (
                spark_run.CLUSTER_MANAGER_K8S,
                {
                    "spark.kubernetes.executor.volumes.hostPath.0.mount.readOnly": "true",
                    "spark.kubernetes.executor.volumes.hostPath.0.mount.path": "/k8s/volume0",
                    "spark.kubernetes.executor.volumes.hostPath.0.options.path": "/k8s/volume0",
                    "spark.kubernetes.executor.volumes.hostPath.1.mount.readOnly": "false",
                    "spark.kubernetes.executor.volumes.hostPath.1.mount.path": "/k8s/volume1",
                    "spark.kubernetes.executor.volumes.hostPath.1.options.path": "/k8s/volume1",
                },
                ["/k8s/volume0:/k8s/volume0:ro", "/k8s/volume1:/k8s/volume1:rw"],
            ),
        ],
    )
    def test_configure_and_run_docker_container(
        self,
        mock_get_history_url,
        mock_et_signalfx_url,
        mock_get_docker_cmd,
        mock_create_spark_config_str,
        mock_get_webui_url,
        mock_send_and_calculate_resources_cost,
        mock_run_docker_container,
        mock_get_username,
        cluster_manager,
        spark_args_volumes,
        expected_volumes,
    ):
        mock_get_username.return_value = "fake_user"
        spark_conf = {
            "spark.app.name": "fake_app",
            "spark.ui.port": "1234",
            **spark_args_volumes,
        }
        mock_run_docker_container.return_value = 0

        args = mock.MagicMock()
        args.aws_region = "fake_region"
        args.cluster = "fake_cluster"
        args.cmd = "pyspark"
        args.work_dir = "/fake_dir:/spark_driver"
        args.dry_run = True
        args.mrjob = False
        args.nvidia = False
        args.cluster_manager = cluster_manager
        with mock.patch.object(
            self.instance_config, "get_env_dictionary", return_value={"env1": "val1"}
        ):
            retcode = configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=cluster_manager,
            )
        assert retcode == 0
        mock_run_docker_container.assert_called_once_with(
            container_name="fake_app",
            volumes=(
                expected_volumes
                + ["/fake_dir:/spark_driver:rw", "/nail/home:/nail/home:rw"]
            ),
            environment={
                "env1": "val1",
                "AWS_ACCESS_KEY_ID": "id",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_SESSION_TOKEN": "token",
                "AWS_DEFAULT_REGION": "fake_region",
                "SPARK_OPTS": mock_create_spark_config_str.return_value,
                "SPARK_USER": "root",
                "PAASTA_INSTANCE_TYPE": "spark",
                "PAASTA_LAUNCHED_BY": mock.ANY,
            },
            docker_img="fake-registry/fake-service",
            docker_cmd=mock_get_docker_cmd.return_value,
            dry_run=True,
            nvidia=False,
        )

    def test_configure_and_run_docker_container_nvidia(
        self,
        mock_get_history_url,
        mock_et_signalfx_url,
        mock_get_docker_cmd,
        mock_create_spark_config_str,
        mock_get_webui_url,
        mock_send_and_calculate_resources_cost,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            spark_conf = {
                "spark.cores.max": "5",
                "spark.master": "mesos://spark.master",
                "spark.ui.port": "1234",
                "spark.app.name": "fake app",
            }
            args = mock.MagicMock(cmd="pyspark", nvidia=True)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
            )

            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["nvidia"]
            assert mock_send_and_calculate_resources_cost.called

    def test_configure_and_run_docker_container_mrjob(
        self,
        mock_get_history_url,
        mock_et_signalfx_url,
        mock_get_docker_cmd,
        mock_create_spark_config_str,
        mock_get_webui_url,
        mock_send_and_calculate_resources_cost,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            spark_conf = {
                "spark.cores.max": 5,
                "spark.master": "mesos://spark.master",
                "spark.ui.port": "1234",
                "spark.app.name": "fake_app",
            }
            args = mock.MagicMock(cmd="python mrjob_wrapper.py", mrjob=True)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
            )

            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["docker_cmd"] == mock_get_docker_cmd.return_value

            assert mock_send_and_calculate_resources_cost.called

    def test_suppress_clusterman_metrics_errors(
        self,
        mock_get_history_url,
        mock_et_signalfx_url,
        mock_get_docker_cmd,
        mock_create_spark_config_str,
        mock_get_webui_url,
        mock_send_and_calculate_resources_cost,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_send_and_calculate_resources_cost.side_effect = Boto3Error
            mock_create_spark_config_str.return_value = "--conf spark.cores.max=5"
            spark_conf = {
                "spark.cores.max": 5,
                "spark.ui.port": "1234",
                "spark.app.name": "fake app",
            }
            args = mock.MagicMock(
                suppress_clusterman_metrics_errors=False, cmd="pyspark"
            )
            with pytest.raises(Boto3Error):
                configure_and_run_docker_container(
                    args=args,
                    docker_img="fake-registry/fake-service",
                    instance_config=self.instance_config,
                    system_paasta_config=self.system_paasta_config,
                    aws_creds=("id", "secret", "token"),
                    spark_conf=spark_conf,
                    cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
                )

            # make sure we don't blow up when this setting is True
            args.suppress_clusterman_metrics_errors = True
            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
            )

    def test_dont_emit_metrics_for_inappropriate_commands(
        self,
        mock_get_history_url,
        mock_et_signalfx_url,
        mock_get_docker_cmd,
        mock_create_spark_config_str,
        mock_get_webui_url,
        mock_send_and_calculate_resources_cost,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_create_spark_config_str.return_value = "--conf spark.cores.max=5"
            args = mock.MagicMock(cmd="bash", mrjob=False)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf={"spark.ui.port": "1234", "spark.app.name": "fake_app"},
                cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
            )
            assert not mock_send_and_calculate_resources_cost.called


@pytest.mark.parametrize(
    "cmd,expected_name",
    [
        # spark-submit use first batch name append user name and port
        (
            "spark-submit path/to/my-script.py --some-configs a.py",
            "paasta_my-script_fake_user",
        ),
        # spark-submit with env settings
        (
            "USER=TEST spark-submit path/to/my-script.py --some-configs a.py",
            "paasta_my-script_fake_user",
        ),
        # spark-submit that is unable to find .py script, use the default name
        # with user name and port
        ("spark-submit path/to/my-script.jar", "paasta_spark_run_fake_user"),
        # non jupyter-lab cmd use the default name and append user name and port
        ("pyspark", "paasta_spark_run_fake_user",),
        # jupyterlab we have a different name
        ("jupyter-lab", "paasta_jupyter_fake_user"),
    ],
)
def test_get_spark_app_name(cmd, expected_name):
    with mock.patch("paasta_tools.cli.cmds.spark_run.get_username", autospec=True) as m:
        m.return_value = "fake_user"
        assert get_spark_app_name(cmd) == expected_name


@pytest.mark.parametrize(
    "args,instance_config,spark_conf_str,expected",
    [
        # ensure add spark conf str
        (
            argparse.Namespace(cmd="pyspark -v", mrjob=False),
            None,
            "--conf spark.app.name=fake_app",
            "pyspark --conf spark.app.name=fake_app -v",
        ),
        # don't add spark_conf if it is other cmd
        (
            argparse.Namespace(cmd="bash", mrjob=False),
            None,
            "--conf spark.app.name=fake_app",
            "bash",
        ),
        # mrjob
        (
            argparse.Namespace(cmd="python mrjob_wrapper.py", mrjob=True),
            None,
            "--jobconf spark.app.name=fake_app",
            "python mrjob_wrapper.py --jobconf spark.app.name=fake_app",
        ),
    ],
)
def test_get_docker_cmd(args, instance_config, spark_conf_str, expected):
    assert spark_run.get_docker_cmd(args, instance_config, spark_conf_str) == expected


@mock.patch.object(spark_run, "validate_work_dir", autospec=True)
@mock.patch.object(spark_run, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "get_instance_config", autospec=True)
@mock.patch.object(spark_run, "get_aws_credentials", autospec=True)
@mock.patch.object(spark_run, "get_docker_image", autospec=True)
@mock.patch.object(spark_run, "get_spark_app_name", autospec=True)
@mock.patch.object(spark_run, "_parse_user_spark_args", autospec=True)
@mock.patch.object(spark_run, "get_spark_conf", autospec=True)
@mock.patch.object(spark_run, "configure_and_run_docker_container", autospec=True)
@mock.patch.object(spark_run, "get_smart_paasta_instance_name", autospec=True)
def test_paasta_spark_run(
    mock_get_smart_paasta_instance_name,
    mock_configure_and_run_docker_container,
    mock_get_spark_conf,
    mock_parse_user_spark_args,
    mock_get_spark_app_name,
    mock_get_docker_image,
    mock_get_aws_credentials,
    mock_get_instance_config,
    mock_load_system_paasta_config,
    mock_validate_work_dir,
):
    args = argparse.Namespace(
        work_dir="/tmp/local",
        cmd="spark-submit test.py",
        build=True,
        image=None,
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        pool="test-pool",
        yelpsoa_config_root="/path/to/soa",
        no_aws_credentials=False,
        aws_credentials_yaml="/path/to/creds",
        aws_profile=None,
        enable_k8s_autogen=True,
        spark_args="spark.cores.max=100 spark.executor.cores=10",
        cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
    )
    spark_run.paasta_spark_run(args)
    mock_validate_work_dir.assert_called_once_with("/tmp/local")
    mock_get_instance_config.assert_called_once_with(
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        load_deployments=False,
        soa_dir="/path/to/soa",
    )
    mock_get_aws_credentials.assert_called_once_with(
        service="test-service",
        no_aws_credentials=False,
        aws_credentials_yaml="/path/to/creds",
        profile_name=None,
    )
    mock_get_docker_image.assert_called_once_with(
        args, mock_get_instance_config.return_value
    )
    mock_get_spark_app_name.assert_called_once_with("spark-submit test.py")
    mock_parse_user_spark_args.assert_called_once_with(
        "spark.cores.max=100 spark.executor.cores=10"
    )
    mock_get_spark_conf.assert_called_once_with(
        cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
        spark_app_base_name=mock_get_spark_app_name.return_value,
        docker_img=mock_get_docker_image.return_value,
        user_spark_opts=mock_parse_user_spark_args.return_value,
        paasta_cluster="test-cluster",
        paasta_pool="test-pool",
        paasta_service="test-service",
        paasta_instance=mock_get_smart_paasta_instance_name.return_value,
        extra_volumes=mock_get_instance_config.return_value.get_volumes.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        needs_docker_cfg=False,
    )
    mock_configure_and_run_docker_container.assert_called_once_with(
        args,
        docker_img=mock_get_docker_image.return_value,
        instance_config=mock_get_instance_config.return_value,
        system_paasta_config=mock_load_system_paasta_config.return_value,
        spark_conf=mock_get_spark_conf.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        cluster_manager=spark_run.CLUSTER_MANAGER_MESOS,
    )
