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

from paasta_tools.cli.cmds.spark_run import configure_and_run_docker_container
from paasta_tools.cli.cmds.spark_run import get_docker_run_cmd
from paasta_tools.cli.cmds.spark_run import get_spark_configuration
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import SystemPaastaConfig


@mock.patch('paasta_tools.cli.cmds.spark_run.os.geteuid', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.os.getegid', autospec=True)
def test_get_docker_run_cmd(
    mock_getegid,
    mock_geteuid,
):
    mock_geteuid.return_value = 1234
    mock_getegid.return_value = 100

    container_name = 'fake_name'
    volumes = ['v1:v1:rw', 'v2:v2:rw']
    env = {'k1': 'v1', 'k2': 'v2'}
    docker_img = 'fake-registry/fake-service'
    docker_cmd = 'pyspark'

    actual = get_docker_run_cmd(
        container_name,
        volumes,
        env,
        docker_img,
        docker_cmd,
    )

    assert actual[6:] == [
        '--user=1234:100',
        '--name=fake_name',
        '--env', 'k1=v1', '--env', 'k2=v2',
        '--volume=v1:v1:rw', '--volume=v2:v2:rw',
        'fake-registry/fake-service',
        'pyspark',
    ]


@mock.patch('paasta_tools.cli.cmds.spark_run.Session.get_credentials', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.find_mesos_leader', autospec=True)
def test_get_spark_configuration(
    mock_find_mesos_leader,
    mock_get_credentials,
):
    mock_find_mesos_leader.return_value = 'fake_leader'
    mock_get_credentials.return_value = mock.MagicMock(access_key='id', secret_key='secret')
    args = mock.MagicMock()
    args.cluster = 'fake_cluster'

    actual = get_spark_configuration(
        args=args,
        container_name='fake_name',
        spark_ui_port=123,
        docker_img='fake-registry/fake-service',
        system_paasta_config=SystemPaastaConfig(
            {"cluster_fqdn_format": "paasta-{cluster:s}.something"},
            'fake_dir',
        ),
    )
    assert actual['AWS_ACCESS_KEY_ID'] == 'id'
    assert actual['AWS_SECRET_ACCESS_KEY'] == 'secret'
    assert actual['SPARK_MASTER'] == 'mesos://fake_leader:5050'


@mock.patch('paasta_tools.cli.cmds.spark_run.os.path.exists', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.os.getcwd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.get_username', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.get_spark_configuration', autospec=True)
@mock.patch('paasta_tools.cli.cmds.spark_run.run_docker_container', autospec=True)
def test_configure_and_run_docker_container(
    mock_run_docker_container,
    mock_get_spark_configuration,
    mock_get_username,
    mock_pick_random_port,
    mock_getcwd,
    mock_os_path_exists,
):
    mock_pick_random_port.return_value = 123
    mock_getcwd.return_value = 'fake_cwd'
    mock_get_username.return_value = 'fake_user'
    mock_get_spark_configuration.return_value = {'APP_NAME': 'fake_app'}
    mock_run_docker_container.return_value = 0

    args = mock.MagicMock()
    args.cluster = 'fake_cluster'
    args.cmd = 'pyspark'
    args.dry_run = True

    retcode = configure_and_run_docker_container(
        args=args,
        docker_img='fake-registry/fake-service',
        instance_config=InstanceConfig(
            cluster='fake_cluster',
            instance='fake_instance',
            service='fake_service',
            config_dict={
                'extra_volumes': [{
                    "hostPath": "/h1",
                    "containerPath": "/c1",
                    "mode": "RO",
                }],
            },
            branch_dict={'docker_image': 'fake_service:fake_sha'},
        ),
        system_paasta_config=SystemPaastaConfig(
            {
                'volumes': [{
                    "hostPath": "/h2",
                    "containerPath": "/c2",
                    "mode": "RO",
                }],
            },
            'fake_dir',
        ),
    )

    assert retcode == 0
    mock_run_docker_container.assert_called_once_with(
        container_name='paasta_spark_run_fake_user_123',
        volumes=[
            '/h1:/c1:ro',
            '/h2:/c2:ro',
            'fake_cwd:/spark_client:rw',
            '/etc/passwd:/etc/passwd:ro',
            '/etc/group:/etc/group:ro',
        ],
        environment={
            'PAASTA_SERVICE': 'fake_service',
            'PAASTA_INSTANCE': 'fake_instance',
            'PAASTA_CLUSTER': 'fake_cluster',
            'PAASTA_DEPLOY_GROUP': 'fake_cluster.fake_instance',
            'PAASTA_DOCKER_IMAGE': 'fake_service:fake_sha',
            'APP_NAME': 'fake_app',
        },
        docker_img='fake-registry/fake-service',
        docker_cmd='pyspark',
        dry_run=True,
    )
