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
import os

import mock
import pytest
from service_configuration_lib.spark_config import AWS_CREDENTIALS_DIR
from service_configuration_lib.spark_config import get_aws_credentials

from paasta_tools import spark_tools
from paasta_tools import utils
from paasta_tools.cli.cmds import spark_run
from paasta_tools.cli.cmds.spark_run import _should_get_resource_requirements
from paasta_tools.cli.cmds.spark_run import build_and_push_docker_image
from paasta_tools.cli.cmds.spark_run import configure_and_run_docker_container
from paasta_tools.cli.cmds.spark_run import DEFAULT_DOCKER_SHM_SIZE
from paasta_tools.cli.cmds.spark_run import DEFAULT_DRIVER_CORES_BY_SPARK
from paasta_tools.cli.cmds.spark_run import DEFAULT_DRIVER_MEMORY_BY_SPARK
from paasta_tools.cli.cmds.spark_run import get_docker_run_cmd
from paasta_tools.cli.cmds.spark_run import get_smart_paasta_instance_name
from paasta_tools.cli.cmds.spark_run import get_spark_app_name
from paasta_tools.cli.cmds.spark_run import sanitize_container_name
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InstanceConfigDict
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import SystemPaastaConfigDict


DUMMY_DOCKER_IMAGE_DIGEST = "MOCK-docker-dev.yelpcorp.com/paasta-spark-run-user@sha256:103ce91c65d42498ca61cdfe8d799fab8ab1c37dac58b743b49ced227bc7bc06"


@mock.patch(
    "paasta_tools.cli.cmds.spark_run.is_using_unprivileged_containers",
    lambda: False,
    autospec=None,
)
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
    docker_memory_limit = "2g"
    docker_shm_size = "1g"
    docker_cpu_limit = "2"

    actual = get_docker_run_cmd(
        container_name,
        volumes,
        env,
        docker_img,
        docker_cmd,
        nvidia,
        docker_memory_limit,
        docker_shm_size,
        docker_cpu_limit,
    )
    assert actual[-12:] == [
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
        instance="foo",
        cmd="USER blah spark-submit blah blah blah",
        mrjob=mrjob,
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
        instance="foo",
        cmd="spark-submit blah blah blah",
        mrjob=True,
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
                cmd="jupyter-lab",
                aws_region="test-region",
                mrjob=False,
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
                cmd="spark-submit job.py",
                aws_region="test-region",
                mrjob=True,
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
        "AWS_DEFAULT_REGION": "test-region",
        "KUBECONFIG": "/etc/kubernetes/spark.conf",
        **extra_expected,
        **expected_aws,
    }
    fake_system_paasta_config = SystemPaastaConfig(
        SystemPaastaConfigDict(
            {"allowed_pools": {"test-cluster": ["test-pool", "fake-pool"]}}
        ),
        "fake_dir",
    )
    assert (
        spark_run.get_spark_env(
            args,
            spark_conf_str,
            aws,
            "1234",
            system_paasta_config=fake_system_paasta_config,
        )
        == expected_output
    )


@pytest.mark.parametrize(
    "spark_args,expected",
    [
        (
            "spark.cores.max=1  spark.executor.memory=24g",
            {"spark.cores.max": "1", "spark.executor.memory": "24g"},
        ),
        (
            "spark.cores.max=1  spark.executor.memory=24g",
            {
                "spark.cores.max": "1",
                "spark.executor.memory": "24g",
            },
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
    output = spark_tools.create_spark_config_str(spark_opts, is_mrjob)
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
    with mock.patch(
        "paasta_tools.cli.cmds.spark_run.os.environ",
        {"env-2": "val-2"},
        autospec=None,
    ):
        spark_run.run_docker_container(
            container_name=container_name,
            volumes=volumes,
            environment=env,
            docker_img=docker_img,
            docker_cmd=docker_cmd,
            dry_run=dry_run,
            nvidia=nvidia,
            docker_memory_limit=DEFAULT_DRIVER_MEMORY_BY_SPARK,
            docker_shm_size=DEFAULT_DOCKER_SHM_SIZE,
            docker_cpu_limit=DEFAULT_DRIVER_CORES_BY_SPARK,
        )
        mock_get_docker_run_cmd.assert_called_once_with(
            container_name=container_name,
            volumes=volumes,
            env=env,
            docker_img=docker_img,
            docker_cmd=docker_cmd,
            nvidia=nvidia,
            docker_memory_limit=DEFAULT_DRIVER_MEMORY_BY_SPARK,
            docker_shm_size=DEFAULT_DOCKER_SHM_SIZE,
            docker_cpu_limit=DEFAULT_DRIVER_CORES_BY_SPARK,
        )
        if dry_run:
            assert not mock_os_execlpe.called
            assert capsys.readouterr().out == '["docker", "run", "commands"]\n'

        else:
            mock_os_execlpe.assert_called_once_with(
                "paasta_docker_wrapper",
                "docker",
                "run",
                "commands",
                {"env-2": "val-2", "SPARK_OPTS": "--conf spark.cores.max=1"},
            )


@mock.patch("paasta_tools.cli.cmds.spark_run.get_username", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.run_docker_container", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_webui_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.get_docker_cmd", autospec=True)
@mock.patch("paasta_tools.cli.cmds.spark_run.create_spark_config_str", autospec=True)
class TestConfigureAndRunDockerContainer:

    instance_config = InstanceConfig(
        cluster="fake_cluster",
        instance="fake_instance",
        service="fake_service",
        config_dict=InstanceConfigDict(
            {
                "extra_volumes": [
                    {"hostPath": "/h1", "containerPath": "/c1", "mode": "RO"}
                ]
            }
        ),
        branch_dict=BranchDictV2({"docker_image": "fake_service:fake_sha"}),
    )

    system_paasta_config = SystemPaastaConfig(
        SystemPaastaConfigDict(
            {"volumes": [{"hostPath": "/h2", "containerPath": "/c2", "mode": "RO"}]}
        ),
        "fake_dir",
    )

    @pytest.mark.parametrize(
        ["cluster_manager", "spark_args_volumes", "expected_volumes"],
        [
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
            (
                spark_run.CLUSTER_MANAGER_LOCAL,
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
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
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
        mock_create_spark_config_str.return_value = "testing spark opts string"
        mock_run_docker_container.return_value = 0

        args = mock.MagicMock()
        args.aws_region = "fake_region"
        args.cluster = "fake_cluster"
        args.cmd = "pyspark"
        args.work_dir = "/fake_dir:/spark_driver"
        args.dry_run = True
        args.mrjob = False
        args.nvidia = False
        args.enable_compact_bin_packing = False
        args.cluster_manager = cluster_manager
        args.docker_cpu_limit = False
        args.docker_memory_limit = False
        args.docker_shm_size = False
        args.tronfig = None
        args.job_id = None
        args.use_service_auth_token = False
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
                pod_template_path="unique-run",
            )
        assert retcode == 0
        mock_create_spark_config_str.assert_called_once_with(
            spark_config_dict=spark_conf,
            is_mrjob=args.mrjob,
        )
        mock_run_docker_container.assert_called_once_with(
            container_name="fake_app",
            volumes=(
                expected_volumes
                + [
                    "/fake_dir:/spark_driver:rw",
                    "/nail/home:/nail/home:rw",
                    "unique-run:unique-run:rw",
                    "/etc/kubernetes/spark.conf:/etc/kubernetes/spark.conf:ro",
                ]
            ),
            environment={
                "env1": "val1",
                "AWS_ACCESS_KEY_ID": "id",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_SESSION_TOKEN": "token",
                "AWS_DEFAULT_REGION": "fake_region",
                "SPARK_OPTS": "testing spark opts string",
                "SPARK_USER": "root",
                "PAASTA_INSTANCE_TYPE": "spark",
                "PAASTA_LAUNCHED_BY": mock.ANY,
                "KUBECONFIG": "/etc/kubernetes/spark.conf",
            },
            docker_img="fake-registry/fake-service",
            docker_cmd=mock_get_docker_cmd.return_value,
            dry_run=True,
            nvidia=False,
            docker_memory_limit="2g",
            docker_shm_size=DEFAULT_DOCKER_SHM_SIZE,
            docker_cpu_limit="1",
        )

    @pytest.mark.parametrize(
        ["cluster_manager", "spark_args_volumes", "expected_volumes"],
        [
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
            (
                spark_run.CLUSTER_MANAGER_LOCAL,
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
    def test_configure_and_run_docker_driver_resource_limits_config(
        self,
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
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
            "spark.driver.memory": "1g",
            "spark.driver.cores": "2",
            **spark_args_volumes,
        }
        mock_create_spark_config_str.return_value = "testing spark opts string"
        mock_run_docker_container.return_value = 0

        args = mock.MagicMock()
        args.aws_region = "fake_region"
        args.cluster = "fake_cluster"
        args.cmd = "pyspark"
        args.work_dir = "/fake_dir:/spark_driver"
        args.dry_run = True
        args.mrjob = False
        args.nvidia = False
        args.enable_compact_bin_packing = False
        args.cluster_manager = cluster_manager
        args.docker_cpu_limit = 3
        args.docker_memory_limit = "4g"
        args.docker_shm_size = "1g"
        args.use_service_auth_token = False
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
                pod_template_path="unique-run",
            )
        assert retcode == 0
        mock_create_spark_config_str.assert_called_once_with(
            spark_config_dict=spark_conf,
            is_mrjob=args.mrjob,
        )
        mock_run_docker_container.assert_called_once_with(
            container_name="fake_app",
            volumes=(
                expected_volumes
                + [
                    "/fake_dir:/spark_driver:rw",
                    "/nail/home:/nail/home:rw",
                    "unique-run:unique-run:rw",
                    "/etc/kubernetes/spark.conf:/etc/kubernetes/spark.conf:ro",
                ]
            ),
            environment={
                "env1": "val1",
                "AWS_ACCESS_KEY_ID": "id",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_SESSION_TOKEN": "token",
                "AWS_DEFAULT_REGION": "fake_region",
                "SPARK_OPTS": "testing spark opts string",
                "SPARK_USER": "root",
                "PAASTA_INSTANCE_TYPE": "spark",
                "PAASTA_LAUNCHED_BY": mock.ANY,
                "KUBECONFIG": "/etc/kubernetes/spark.conf",
            },
            docker_img="fake-registry/fake-service",
            docker_cmd=mock_get_docker_cmd.return_value,
            dry_run=True,
            nvidia=False,
            docker_memory_limit="4g",
            docker_shm_size="1g",
            docker_cpu_limit=3,
        )

    @pytest.mark.parametrize(
        ["cluster_manager", "spark_args_volumes", "expected_volumes"],
        [
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
            (
                spark_run.CLUSTER_MANAGER_LOCAL,
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
    def test_configure_and_run_docker_driver_resource_limits(
        self,
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
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
            "spark.driver.memory": "1g",
            "spark.driver.cores": "2",
            **spark_args_volumes,
        }
        mock_create_spark_config_str.return_value = "testing spark opts string"
        mock_run_docker_container.return_value = 0

        args = mock.MagicMock()
        args.aws_region = "fake_region"
        args.cluster = "fake_cluster"
        args.cmd = "pyspark"
        args.work_dir = "/fake_dir:/spark_driver"
        args.dry_run = True
        args.mrjob = False
        args.nvidia = False
        args.enable_compact_bin_packing = False
        args.cluster_manager = cluster_manager
        args.docker_cpu_limit = False
        args.docker_memory_limit = False
        args.docker_shm_size = False
        args.use_service_auth_token = False
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
                pod_template_path="unique-run",
            )

        assert retcode == 0
        mock_create_spark_config_str.assert_called_once_with(
            spark_config_dict=spark_conf,
            is_mrjob=args.mrjob,
        )
        mock_run_docker_container.assert_called_once_with(
            container_name="fake_app",
            volumes=(
                expected_volumes
                + [
                    "/fake_dir:/spark_driver:rw",
                    "/nail/home:/nail/home:rw",
                    "unique-run:unique-run:rw",
                    "/etc/kubernetes/spark.conf:/etc/kubernetes/spark.conf:ro",
                ]
            ),
            environment={
                "env1": "val1",
                "AWS_ACCESS_KEY_ID": "id",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_SESSION_TOKEN": "token",
                "AWS_DEFAULT_REGION": "fake_region",
                "SPARK_OPTS": "testing spark opts string",
                "SPARK_USER": "root",
                "PAASTA_INSTANCE_TYPE": "spark",
                "PAASTA_LAUNCHED_BY": mock.ANY,
                "KUBECONFIG": "/etc/kubernetes/spark.conf",
            },
            docker_img="fake-registry/fake-service",
            docker_cmd=mock_get_docker_cmd.return_value,
            dry_run=True,
            nvidia=False,
            docker_memory_limit="2g",
            docker_shm_size=DEFAULT_DOCKER_SHM_SIZE,
            docker_cpu_limit="2",
        )

    def test_configure_and_run_docker_container_nvidia(
        self,
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            spark_conf = {
                "spark.cores.max": "5",
                "spark.executor.cores": 1,
                "spark.executor.memory": "2g",
                "spark.master": "mesos://spark.master",
                "spark.ui.port": "1234",
                "spark.app.name": "fake app",
                "spark.executorEnv.PAASTA_CLUSTER": "test-cluster",
            }
            args = mock.MagicMock(
                cmd="pyspark", nvidia=True, use_service_auth_token=False
            )

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
                pod_template_path="unique-run",
            )

            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["nvidia"]

    def test_configure_and_run_docker_container_mrjob(
        self,
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            spark_conf = {
                "spark.cores.max": 5,
                "spark.executor.cores": 1,
                "spark.executor.memory": "2g",
                "spark.master": "mesos://spark.master",
                "spark.ui.port": "1234",
                "spark.app.name": "fake_app",
                "spark.executorEnv.PAASTA_CLUSTER": "test-cluster",
            }
            args = mock.MagicMock(
                cmd="python mrjob_wrapper.py", mrjob=True, use_service_auth_token=False
            )

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
                pod_template_path="unique-run",
            )

            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["docker_cmd"] == mock_get_docker_cmd.return_value

    def test_dont_emit_metrics_for_inappropriate_commands(
        self,
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
        mock_run_docker_container,
        mock_get_username,
    ):
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            mock_create_spark_config_str.return_value = "--conf spark.cores.max=5"
            args = mock.MagicMock(cmd="bash", mrjob=False, use_service_auth_token=False)

            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf={"spark.ui.port": "1234", "spark.app.name": "fake_app"},
                cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
                pod_template_path="unique-run",
            )

    @mock.patch("paasta_tools.cli.cmds.spark_run.get_service_auth_token", autospec=True)
    def test_configure_and_run_docker_container_auth_token(
        self,
        mock_get_service_auth_token,
        mock_create_spark_config_str,
        mock_get_docker_cmd,
        mock_get_webui_url,
        mock_run_docker_container,
        mock_get_username,
    ):
        mock_get_service_auth_token.return_value = "foobar"
        with mock.patch(
            "paasta_tools.cli.cmds.spark_run.clusterman_metrics", autospec=True
        ):
            spark_conf = {
                "spark.cores.max": "5",
                "spark.executor.cores": 1,
                "spark.executor.memory": "2g",
                "spark.master": "mesos://spark.master",
                "spark.ui.port": "1234",
                "spark.app.name": "fake app",
                "spark.executorEnv.PAASTA_CLUSTER": "test-cluster",
            }
            args = mock.MagicMock(
                cmd="pyspark",
                use_service_auth_token=True,
            )
            configure_and_run_docker_container(
                args=args,
                docker_img="fake-registry/fake-service",
                instance_config=self.instance_config,
                system_paasta_config=self.system_paasta_config,
                aws_creds=("id", "secret", "token"),
                spark_conf=spark_conf,
                cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
                pod_template_path="unique-run",
            )
            args, kwargs = mock_run_docker_container.call_args
            assert kwargs["environment"]["YELP_SVC_AUTHZ_TOKEN"] == "foobar"


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
        (
            "pyspark",
            "paasta_spark_run_fake_user",
        ),
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
@mock.patch.object(utils, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "get_instance_config", autospec=True)
@mock.patch.object(spark_run, "get_aws_credentials", autospec=True)
@mock.patch.object(spark_run, "get_docker_image", autospec=True)
@mock.patch.object(spark_run, "get_spark_app_name", autospec=True)
@mock.patch.object(spark_run, "_parse_user_spark_args", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.spark_run.spark_config.SparkConfBuilder", autospec=True
)
@mock.patch.object(spark_run, "configure_and_run_docker_container", autospec=True)
@mock.patch.object(spark_run, "get_smart_paasta_instance_name", autospec=True)
def test_paasta_spark_run_bash(
    mock_get_smart_paasta_instance_name,
    mock_configure_and_run_docker_container,
    mock_spark_conf_builder,
    mock_parse_user_spark_args,
    mock_get_spark_app_name,
    mock_get_docker_image,
    mock_get_aws_credentials,
    mock_get_instance_config,
    mock_load_system_paasta_config_spark_run,
    mock_load_system_paasta_config_utils,
    mock_validate_work_dir,
):
    args = argparse.Namespace(
        work_dir="/tmp/local",
        cmd="/bin/bash",
        build=True,
        image=None,
        enable_compact_bin_packing=False,
        disable_compact_bin_packing=False,
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        pool="test-pool",
        yelpsoa_config_root="/path/to/soa",
        aws_credentials_yaml="/path/to/creds",
        aws_profile=None,
        spark_args="spark.cores.max=100 spark.executor.cores=10",
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        timeout_job_runtime="1m",
        enable_dra=False,
        aws_region="test-region",
        force_spark_resource_configs=False,
        assume_aws_role=None,
        aws_role_duration=3600,
        k8s_server_address=None,
        tronfig=None,
        job_id=None,
        use_web_identity=False,
        uses_bulkdata=True,
    )
    mock_load_system_paasta_config_utils.return_value.get_kube_clusters.return_value = (
        {}
    )
    mock_load_system_paasta_config_spark_run.return_value.get_cluster_aliases.return_value = (
        {}
    )
    mock_load_system_paasta_config_spark_run.return_value.get_pools_for_cluster.return_value = [
        "test-pool"
    ]
    mock_load_system_paasta_config_spark_run.return_value.get_eks_cluster_aliases.return_value = {
        "test-cluster": "test-cluster"
    }
    mock_get_docker_image.return_value = DUMMY_DOCKER_IMAGE_DIGEST
    mock_spark_conf_builder.return_value.get_spark_conf.return_value = {
        "spark.kubernetes.executor.podTemplateFile": "/test/pod-template.yaml",
    }
    spark_run.paasta_spark_run(args)
    mock_validate_work_dir.assert_called_once_with("/tmp/local")
    assert args.cmd == "/bin/bash"
    mock_get_instance_config.assert_called_once_with(
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        load_deployments=False,
        soa_dir="/path/to/soa",
    )
    mock_get_aws_credentials.assert_called_once_with(
        service="test-service",
        aws_credentials_yaml="/path/to/creds",
        profile_name=None,
        assume_aws_role_arn=None,
        session_duration=3600,
        use_web_identity=False,
    )
    mock_get_docker_image.assert_called_once_with(
        args, mock_get_instance_config.return_value
    )
    mock_get_spark_app_name.assert_called_once_with("/bin/bash")
    mock_parse_user_spark_args.assert_called_once_with(
        "spark.cores.max=100 spark.executor.cores=10"
    )
    mock_spark_conf_builder.return_value.get_spark_conf.assert_called_once_with(
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        spark_app_base_name=mock_get_spark_app_name.return_value,
        docker_img=DUMMY_DOCKER_IMAGE_DIGEST,
        user_spark_opts=mock_parse_user_spark_args.return_value,
        paasta_cluster="test-cluster",
        paasta_pool="test-pool",
        paasta_service="test-service",
        paasta_instance=mock_get_smart_paasta_instance_name.return_value,
        extra_volumes=mock_get_instance_config.return_value.get_volumes.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        aws_region="test-region",
        force_spark_resource_configs=False,
        use_eks=True,
        k8s_server_address=None,
    )
    mock_spark_conf = mock_spark_conf_builder.return_value.get_spark_conf.return_value
    mock_configure_and_run_docker_container.assert_called_once_with(
        args,
        docker_img=DUMMY_DOCKER_IMAGE_DIGEST,
        instance_config=mock_get_instance_config.return_value,
        system_paasta_config=mock_load_system_paasta_config_spark_run.return_value,
        spark_conf=mock_spark_conf,
        aws_creds=mock_get_aws_credentials.return_value,
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        pod_template_path="/test/pod-template.yaml",
        extra_driver_envs=dict(),
    )


@mock.patch.object(spark_run, "validate_work_dir", autospec=True)
@mock.patch.object(utils, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "get_instance_config", autospec=True)
@mock.patch.object(spark_run, "get_aws_credentials", autospec=True)
@mock.patch.object(spark_run, "get_docker_image", autospec=True)
@mock.patch.object(spark_run, "get_spark_app_name", autospec=True)
@mock.patch.object(spark_run, "auto_add_timeout_for_spark_job", autospec=True)
@mock.patch.object(spark_run, "_parse_user_spark_args", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.spark_run.spark_config.SparkConfBuilder", autospec=True
)
@mock.patch.object(spark_run, "configure_and_run_docker_container", autospec=True)
@mock.patch.object(spark_run, "get_smart_paasta_instance_name", autospec=True)
def test_paasta_spark_run(
    mock_get_smart_paasta_instance_name,
    mock_configure_and_run_docker_container,
    mock_spark_conf_builder,
    mock_parse_user_spark_args,
    mock_auto_add_timeout_for_spark_job,
    mock_get_spark_app_name,
    mock_get_docker_image,
    mock_get_aws_credentials,
    mock_get_instance_config,
    mock_load_system_paasta_config_spark_run,
    mock_load_system_paasta_config_utils,
    mock_validate_work_dir,
):
    args = argparse.Namespace(
        work_dir="/tmp/local",
        cmd="USER=test spark-submit test.py",
        build=True,
        image=None,
        enable_compact_bin_packing=False,
        disable_compact_bin_packing=False,
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        pool="test-pool",
        yelpsoa_config_root="/path/to/soa",
        aws_credentials_yaml="/path/to/creds",
        aws_profile=None,
        spark_args="spark.cores.max=100 spark.executor.cores=10",
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        timeout_job_runtime="1m",
        enable_dra=True,
        aws_region="test-region",
        force_spark_resource_configs=False,
        assume_aws_role=None,
        aws_role_duration=3600,
        k8s_server_address=None,
        tronfig=None,
        job_id=None,
        use_web_identity=False,
        uses_bulkdata=True,
    )
    mock_load_system_paasta_config_utils.return_value.get_kube_clusters.return_value = (
        {}
    )
    mock_load_system_paasta_config_spark_run.return_value.get_cluster_aliases.return_value = (
        {}
    )
    mock_load_system_paasta_config_spark_run.return_value.get_pools_for_cluster.return_value = [
        "test-pool"
    ]
    mock_load_system_paasta_config_spark_run.return_value.get_eks_cluster_aliases.return_value = {
        "test-cluster": "test-cluster"
    }
    mock_get_docker_image.return_value = DUMMY_DOCKER_IMAGE_DIGEST
    mock_auto_add_timeout_for_spark_job.return_value = (
        "USER=test timeout 1m spark-submit test.py"
    )
    mock_spark_conf_builder.return_value.get_spark_conf.return_value = {
        "spark.kubernetes.executor.podTemplateFile": "/test/pod-template.yaml",
    }
    spark_run.paasta_spark_run(args)
    mock_validate_work_dir.assert_called_once_with("/tmp/local")
    assert args.cmd == "USER=test timeout 1m spark-submit test.py"
    mock_get_instance_config.assert_called_once_with(
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        load_deployments=False,
        soa_dir="/path/to/soa",
    )
    mock_get_aws_credentials.assert_called_once_with(
        service="test-service",
        aws_credentials_yaml="/path/to/creds",
        profile_name=None,
        assume_aws_role_arn=None,
        session_duration=3600,
        use_web_identity=False,
    )
    mock_get_docker_image.assert_called_once_with(
        args, mock_get_instance_config.return_value
    )
    mock_get_spark_app_name.assert_called_once_with("USER=test spark-submit test.py")
    mock_parse_user_spark_args.assert_called_once_with(
        "spark.cores.max=100 spark.executor.cores=10"
    )
    mock_spark_conf_builder.return_value.get_spark_conf.assert_called_once_with(
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        spark_app_base_name=mock_get_spark_app_name.return_value,
        docker_img=DUMMY_DOCKER_IMAGE_DIGEST,
        user_spark_opts=mock_parse_user_spark_args.return_value,
        paasta_cluster="test-cluster",
        paasta_pool="test-pool",
        paasta_service="test-service",
        paasta_instance=mock_get_smart_paasta_instance_name.return_value,
        extra_volumes=mock_get_instance_config.return_value.get_volumes.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        aws_region="test-region",
        force_spark_resource_configs=False,
        use_eks=True,
        k8s_server_address=None,
    )
    mock_configure_and_run_docker_container.assert_called_once_with(
        args,
        docker_img=DUMMY_DOCKER_IMAGE_DIGEST,
        instance_config=mock_get_instance_config.return_value,
        system_paasta_config=mock_load_system_paasta_config_spark_run.return_value,
        spark_conf=mock_spark_conf_builder.return_value.get_spark_conf.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        pod_template_path="/test/pod-template.yaml",
        extra_driver_envs=dict(),
    )


@mock.patch.object(spark_run, "validate_work_dir", autospec=True)
@mock.patch.object(utils, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "get_instance_config", autospec=True)
@mock.patch.object(spark_run, "get_aws_credentials", autospec=True)
@mock.patch.object(spark_run, "get_docker_image", autospec=True)
@mock.patch.object(spark_run, "get_spark_app_name", autospec=True)
@mock.patch.object(spark_run, "_parse_user_spark_args", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.spark_run.spark_config.SparkConfBuilder", autospec=True
)
@mock.patch.object(spark_run, "configure_and_run_docker_container", autospec=True)
@mock.patch.object(spark_run, "get_smart_paasta_instance_name", autospec=True)
def test_paasta_spark_run_pyspark(
    mock_get_smart_paasta_instance_name,
    mock_configure_and_run_docker_container,
    mock_spark_conf_builder,
    mock_parse_user_spark_args,
    mock_get_spark_app_name,
    mock_get_docker_image,
    mock_get_aws_credentials,
    mock_get_instance_config,
    mock_load_system_paasta_config_spark_run,
    mock_load_system_paasta_config_utils,
    mock_validate_work_dir,
):
    args = argparse.Namespace(
        work_dir="/tmp/local",
        cmd="pyspark",
        build=True,
        image=None,
        enable_compact_bin_packing=False,
        disable_compact_bin_packing=False,
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        pool="test-pool",
        yelpsoa_config_root="/path/to/soa",
        aws_credentials_yaml="/path/to/creds",
        aws_profile=None,
        spark_args="spark.cores.max=70 spark.executor.cores=10",
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        timeout_job_runtime="1m",
        enable_dra=False,
        aws_region="test-region",
        force_spark_resource_configs=False,
        assume_aws_role=None,
        aws_role_duration=3600,
        k8s_server_address=None,
        tronfig=None,
        job_id=None,
        use_web_identity=False,
        uses_bulkdata=True,
    )
    mock_load_system_paasta_config_utils.return_value.get_kube_clusters.return_value = (
        {}
    )
    mock_load_system_paasta_config_spark_run.return_value.get_spark_use_eks_default.return_value = (
        False
    )
    mock_load_system_paasta_config_spark_run.return_value.get_cluster_aliases.return_value = (
        {}
    )
    mock_load_system_paasta_config_spark_run.return_value.get_pools_for_cluster.return_value = [
        "test-pool"
    ]
    mock_load_system_paasta_config_spark_run.return_value.get_eks_cluster_aliases.return_value = {
        "test-cluster": "test-cluster"
    }

    mock_get_docker_image.return_value = DUMMY_DOCKER_IMAGE_DIGEST
    mock_spark_conf_builder.return_value.get_spark_conf.return_value = {
        "spark.kubernetes.executor.podTemplateFile": "/test/pod-template.yaml",
    }

    spark_run.paasta_spark_run(args)
    mock_validate_work_dir.assert_called_once_with("/tmp/local")
    assert args.cmd == "pyspark"
    mock_get_instance_config.assert_called_once_with(
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        load_deployments=False,
        soa_dir="/path/to/soa",
    )
    mock_get_aws_credentials.assert_called_once_with(
        service="test-service",
        aws_credentials_yaml="/path/to/creds",
        profile_name=None,
        assume_aws_role_arn=None,
        session_duration=3600,
        use_web_identity=False,
    )
    mock_get_docker_image.assert_called_once_with(
        args, mock_get_instance_config.return_value
    )
    mock_get_spark_app_name.assert_called_once_with("pyspark")
    mock_parse_user_spark_args.assert_called_once_with(
        "spark.cores.max=70 spark.executor.cores=10",
    )
    mock_spark_conf_builder.return_value.get_spark_conf.assert_called_once_with(
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        spark_app_base_name=mock_get_spark_app_name.return_value,
        docker_img=DUMMY_DOCKER_IMAGE_DIGEST,
        user_spark_opts=mock_parse_user_spark_args.return_value,
        paasta_cluster="test-cluster",
        paasta_pool="test-pool",
        paasta_service="test-service",
        paasta_instance=mock_get_smart_paasta_instance_name.return_value,
        extra_volumes=mock_get_instance_config.return_value.get_volumes.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        aws_region="test-region",
        force_spark_resource_configs=False,
        use_eks=True,
        k8s_server_address=None,
    )
    mock_configure_and_run_docker_container.assert_called_once_with(
        args,
        docker_img=DUMMY_DOCKER_IMAGE_DIGEST,
        instance_config=mock_get_instance_config.return_value,
        system_paasta_config=mock_load_system_paasta_config_spark_run.return_value,
        spark_conf=mock_spark_conf_builder.return_value.get_spark_conf.return_value,
        aws_creds=mock_get_aws_credentials.return_value,
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        pod_template_path="/test/pod-template.yaml",
        extra_driver_envs=dict(),
    )


@mock.patch.object(spark_run, "validate_work_dir", autospec=True)
@mock.patch.object(utils, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "load_system_paasta_config", autospec=True)
@mock.patch.object(spark_run, "get_instance_config", autospec=True)
@mock.patch.object(spark_run, "get_aws_credentials", autospec=True)
@mock.patch.object(spark_run, "get_docker_image", autospec=True)
@mock.patch.object(spark_run, "get_spark_app_name", autospec=True)
@mock.patch.object(spark_run, "auto_add_timeout_for_spark_job", autospec=True)
@mock.patch.object(spark_run, "_parse_user_spark_args", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.spark_run.spark_config.SparkConfBuilder", autospec=True
)
@mock.patch.object(spark_run, "configure_and_run_docker_container", autospec=True)
@mock.patch.object(spark_run, "get_smart_paasta_instance_name", autospec=True)
@pytest.mark.parametrize(
    "spark_run_arg_uses_bulkdata,instance_config_uses_bulkdata,expected",
    [
        (True, True, True),
        (True, False, True),
        (False, True, True),
        (False, False, False),
    ],
)
def test_paasta_spark_run_uses_bulkdata(
    mock_get_smart_paasta_instance_name,
    mock_configure_and_run_docker_container,
    mock_spark_conf_builder,
    mock_parse_user_spark_args,
    mock_auto_add_timeout_for_spark_job,
    mock_get_spark_app_name,
    mock_get_docker_image,
    mock_get_aws_credentials,
    mock_get_instance_config,
    mock_load_system_paasta_config_spark_run,
    mock_load_system_paasta_config_utils,
    mock_validate_work_dir,
    spark_run_arg_uses_bulkdata,
    instance_config_uses_bulkdata,
    expected,
):
    args = argparse.Namespace(
        work_dir="/tmp/local",
        cmd="USER=test spark-submit test.py",
        build=True,
        image=None,
        enable_compact_bin_packing=False,
        disable_compact_bin_packing=False,
        service="test-service",
        instance="test-instance",
        cluster="test-cluster",
        pool="test-pool",
        yelpsoa_config_root="/path/to/soa",
        aws_credentials_yaml="/path/to/creds",
        aws_profile=None,
        spark_args="spark.cores.max=100 spark.executor.cores=10",
        cluster_manager=spark_run.CLUSTER_MANAGER_K8S,
        timeout_job_runtime="1m",
        enable_dra=True,
        aws_region="test-region",
        force_spark_resource_configs=False,
        assume_aws_role=None,
        aws_role_duration=3600,
        k8s_server_address=None,
        tronfig=None,
        job_id=None,
        use_web_identity=False,
        uses_bulkdata=spark_run_arg_uses_bulkdata,
    )
    mock_load_system_paasta_config_spark_run.return_value.get_pools_for_cluster.return_value = [
        "test-pool"
    ]

    mock_get_instance_config.return_value.config_dict = {
        "uses_bulkdata": instance_config_uses_bulkdata
    }

    spark_run.paasta_spark_run(args)

    assert (
        mock_get_instance_config.return_value.config_dict["uses_bulkdata"] == expected
    )


@pytest.mark.parametrize(
    "docker_cmd, is_mrjob, expected",
    (
        # normal mesos cases
        ("spark-submit FOO", False, True),
        ("spark-shell FOO", False, True),
        ("pyspark FOO", False, True),
        # mesos, but wrong command
        ("spark-nope FOO", False, False),
        # mrjob
        ("FOO", True, True),
    ),
)
def test__should_get_resource_requirements(docker_cmd, is_mrjob, expected):
    assert _should_get_resource_requirements(docker_cmd, is_mrjob) is expected


@mock.patch.object(spark_run, "makefile_responds_to", autospec=True)
@mock.patch.object(spark_run, "paasta_cook_image", autospec=True)
@mock.patch.object(spark_run, "get_username", autospec=True)
def test_build_and_push_docker_image_unprivileged_output_format(
    mock_get_username,
    mock_paasta_cook_image,
    mock_makefile_responds_to,
    mock_run,
):
    args = mock.MagicMock(
        docker_registry="MOCK-docker-dev.yelpcorp.com",
        autospec=True,
    )
    mock_makefile_responds_to.return_value = True
    mock_paasta_cook_image.return_value = 0
    mock_run.side_effect = [
        (0, None),
        (
            0,
            (
                "Using default tag: latest\n"
                "The push refers to repository [MOCK-docker-dev.yelpcorp.com/paasta-spark-run-user:latest]\n"
                "latest: digest: sha256:103ce91c65d42498ca61cdfe8d799fab8ab1c37dac58b743b49ced227bc7bc06"
            ),
        ),
        (0, None),
    ]
    mock_get_username.return_value = "user"
    docker_image_digest = build_and_push_docker_image(args)
    assert DUMMY_DOCKER_IMAGE_DIGEST == docker_image_digest


@mock.patch.object(spark_run, "makefile_responds_to", autospec=True)
@mock.patch.object(spark_run, "paasta_cook_image", autospec=True)
@mock.patch.object(spark_run, "get_username", autospec=True)
def test_build_and_push_docker_image_privileged_output_format(
    mock_get_username,
    mock_paasta_cook_image,
    mock_makefile_responds_to,
    mock_run,
):
    args = mock.MagicMock(
        docker_registry="MOCK-docker-dev.yelpcorp.com",
        autospec=True,
    )
    mock_makefile_responds_to.return_value = True
    mock_paasta_cook_image.return_value = 0
    mock_run.side_effect = [
        (0, None),
        (
            0,
            (
                "Using default tag: latest\n"
                "The push refers to repository [MOCK-docker-dev.yelpcorp.com/paasta-spark-run-user:latest]\n"
                "latest: digest: sha256:103ce91c65d42498ca61cdfe8d799fab8ab1c37dac58b743b49ced227bc7bc06 size: 1337"
            ),
        ),
        (0, None),
    ]
    mock_get_username.return_value = "user"
    docker_image_digest = build_and_push_docker_image(args)
    assert DUMMY_DOCKER_IMAGE_DIGEST == docker_image_digest


@mock.patch.object(spark_run, "makefile_responds_to", autospec=True)
@mock.patch.object(spark_run, "paasta_cook_image", autospec=True)
@mock.patch.object(spark_run, "get_username", autospec=True)
def test_build_and_push_docker_image_unexpected_output_format(
    mock_get_username,
    mock_paasta_cook_image,
    mock_makefile_responds_to,
    mock_run,
):
    args = mock.MagicMock(
        docker_registry="MOCK-docker-dev.yelpcorp.com",
        autospec=True,
    )
    mock_makefile_responds_to.return_value = True
    mock_paasta_cook_image.return_value = 0
    mock_run.side_effect = [
        (0, None),
        (
            0,
            (
                "Using default tag: latest\n"
                "The push refers to repository [MOCK-docker-dev.yelpcorp.com/paasta-spark-run-user:latest]\n"
                "the regex will not match this"
            ),
        ),
        (0, None),
    ]
    with pytest.raises(ValueError) as e:
        build_and_push_docker_image(args)
    assert "Could not determine digest from output" in str(e.value)


def test_get_aws_credentials():
    with mock.patch.dict(
        os.environ,
        {
            "AWS_WEB_IDENTITY_TOKEN_FILE": "./some-file.txt",
            "AWS_ROLE_ARN": "some-role-for-test",
        },
    ), mock.patch(
        "service_configuration_lib.spark_config.open",
        mock.mock_open(read_data="token-content"),
        autospec=False,
    ), mock.patch(
        "service_configuration_lib.spark_config.boto3.client",
        autospec=False,
    ) as boto3_client:
        get_aws_credentials(
            service="some-service",
            use_web_identity=True,
        )
    boto3_client.assert_called_once_with("sts")
    boto3_client.return_value.assume_role_with_web_identity.assert_called_once_with(
        DurationSeconds=3600,
        RoleArn="some-role-for-test",
        RoleSessionName=mock.ANY,
        WebIdentityToken="token-content",
    )


@mock.patch("service_configuration_lib.spark_config.use_aws_profile", autospec=False)
@mock.patch("service_configuration_lib.spark_config.Session", autospec=True)
def test_get_aws_credentials_session(mock_boto3_session, mock_use_aws_profile):
    # prioritize session over `profile_name` if both are provided
    session = mock_boto3_session()

    get_aws_credentials(
        service="some-service",
        session=session,
        profile_name="some-profile",
    )

    mock_use_aws_profile.assert_called_once_with(session=session)
    session.assert_not_called()


@mock.patch("service_configuration_lib.spark_config.use_aws_profile", autospec=False)
@mock.patch("service_configuration_lib.spark_config.Session", autospec=True)
def test_get_aws_credentials_profile(mock_boto3_session, mock_use_aws_profile):
    # prioritize `profile_name` over `service` if both are provided
    profile_name = "some-profile"

    get_aws_credentials(service="some-service", profile_name=profile_name)

    mock_use_aws_profile.assert_called_once_with(profile_name=profile_name)


@mock.patch("service_configuration_lib.spark_config.use_aws_profile", autospec=False)
@mock.patch("os.path.exists", autospec=False)
@mock.patch(
    "service_configuration_lib.spark_config._load_aws_credentials_from_yaml",
    autospec=True,
)
def test_get_aws_credentials_boto_cfg(
    mock_load_aws_credentials_from_yaml, mock_os_path_exists, mock_use_aws_profile
):
    # use `service` if profile_name is not provided
    service_name = "some-service"

    get_aws_credentials(
        service=service_name,
    )

    credentials_path = f"{AWS_CREDENTIALS_DIR}{service_name}.yaml"
    mock_os_path_exists.return_value = True

    mock_load_aws_credentials_from_yaml.assert_called_once_with(credentials_path)
    mock_use_aws_profile.assert_not_called()


@mock.patch("service_configuration_lib.spark_config.use_aws_profile", autospec=False)
@mock.patch("service_configuration_lib.spark_config.Session", autospec=True)
def test_get_aws_credentials_default_profile(mock_boto3_session, mock_use_aws_profile):
    # use `default` profile if no valid options are provided
    get_aws_credentials(
        service="spark",
    )

    mock_use_aws_profile.assert_called_once_with()
