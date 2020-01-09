import mock
import pytest
from ruamel.yaml import YAML

from paasta_tools.spark_tools import _load_aws_credentials_from_yaml
from paasta_tools.spark_tools import DEFAULT_SPARK_SERVICE
from paasta_tools.spark_tools import get_aws_credentials
from paasta_tools.spark_tools import get_default_event_log_dir
from paasta_tools.spark_tools import get_spark_resource_requirements


def test_load_aws_credentials_from_yaml(tmpdir):
    fake_access_key_id = "fake_access_key_id"
    fake_secret_access_key = "fake_secret_access_key"
    yaml_file = tmpdir.join("test.yaml")
    yaml_file.write(
        f'aws_access_key_id: "{fake_access_key_id}"\n'
        f'aws_secret_access_key: "{fake_secret_access_key}"'
    )

    aws_access_key_id, aws_secret_access_key = _load_aws_credentials_from_yaml(
        yaml_file
    )
    assert aws_access_key_id == fake_access_key_id
    assert aws_secret_access_key == fake_secret_access_key


def test_creds_disabled():
    credentials = get_aws_credentials(no_aws_credentials=True)
    assert credentials == (None, None)


@mock.patch("paasta_tools.spark_tools._load_aws_credentials_from_yaml", autospec=True)
def test_yaml_provided(mock_load_aws_credentials_from_yaml):
    credentials = get_aws_credentials(aws_credentials_yaml="credentials.yaml")

    mock_load_aws_credentials_from_yaml.assert_called_once_with("credentials.yaml")
    assert credentials == mock_load_aws_credentials_from_yaml.return_value


@mock.patch("paasta_tools.spark_tools.os.path.exists", autospec=True)
@mock.patch("paasta_tools.spark_tools._load_aws_credentials_from_yaml", autospec=True)
def test_service_provided_no_yaml(
    mock_load_aws_credentials_from_yaml, mock_os,
):
    mock_os.return_value = True
    credentials = get_aws_credentials(service="service_name")

    mock_load_aws_credentials_from_yaml.assert_called_once_with(
        "/etc/boto_cfg/service_name.yaml"
    )
    assert credentials == mock_load_aws_credentials_from_yaml.return_value


@mock.patch("paasta_tools.spark_tools.Session.get_credentials", autospec=True)
@mock.patch("paasta_tools.spark_tools._load_aws_credentials_from_yaml", autospec=True)
def test_use_default_creds(mock_load_aws_credentials_from_yaml, mock_get_credentials):
    args = mock.Mock(
        no_aws_credentials=False,
        aws_credentials_yaml=None,
        service=DEFAULT_SPARK_SERVICE,
    )
    mock_get_credentials.return_value = mock.MagicMock(
        access_key="id", secret_key="secret"
    )
    credentials = get_aws_credentials(args)

    assert credentials == ("id", "secret")


@mock.patch("paasta_tools.spark_tools.os", autospec=True)
@mock.patch("paasta_tools.spark_tools.Session.get_credentials", autospec=True)
def test_service_provided_fallback_to_default(mock_get_credentials, mock_os):
    args = mock.Mock(
        no_aws_credentials=False, aws_credentials_yaml=None, service="service_name"
    )
    mock_os.path.exists.return_value = False
    mock_get_credentials.return_value = mock.MagicMock(
        access_key="id", secret_key="secret"
    )
    credentials = get_aws_credentials(args)

    assert credentials == ("id", "secret")


class TestStuff:
    dev_account_id = "12345"
    dev_log_dir = "s3a://dev/log/path"

    other_account_id = "23456"
    other_log_dir = "s3a://other/log/path"

    unrecognized_account_id = "34567"

    @pytest.fixture(autouse=True)
    def mock_account_id(self):
        with mock.patch("paasta_tools.spark_tools.boto3.client", autospec=True) as m:
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

        with mock.patch(
            "paasta_tools.spark_tools.DEFAULT_SPARK_RUN_CONFIG",
            spark_run_file,
            autospec=None,
        ):
            yield spark_run_file

    @pytest.mark.parametrize(
        "account_id,expected_dir",
        [
            (dev_account_id, dev_log_dir),
            (other_account_id, other_log_dir),
            ("34567", None),
            (None, None),
        ],
    )
    def test_get_default_event_log_dir(self, mock_account_id, account_id, expected_dir):
        mock_account_id.return_value = account_id
        assert (
            get_default_event_log_dir(
                access_key="test_access_key", secret_key="test_secret_key"
            )
            == expected_dir
        )


def test_get_spark_resource_requirements(tmpdir):
    spark_config_dict = {
        "spark.executor.cores": "2",
        "spark.cores.max": "4",
        "spark.executor.memory": "4g",
        "spark.mesos.executor.memoryOverhead": "555",
        "spark.app.name": "paasta_spark_run_johndoe_2_3",
        "spark.mesos.constraints": "pool:cool-pool\\;other:value",
    }

    clusterman_yaml_file_path = tmpdir.join("fake_clusterman.yaml")
    expected_memory_request = (4 * 1024 + 555) * 2
    metric_key_template = "requested_{resource}|framework_name=paasta_spark_run_johndoe_2_3,webui_url=http://spark.yelp"
    with mock.patch(
        "paasta_tools.spark_tools.get_clusterman_metrics", autospec=True
    ), mock.patch(
        "paasta_tools.spark_tools.clusterman_metrics", autospec=True
    ) as mock_clusterman_metrics, mock.patch(
        "paasta_tools.spark_tools.CLUSTERMAN_YAML_FILE_PATH",
        clusterman_yaml_file_path,
        autospec=None,  # we're replacing this name, so we can't autospec
    ):
        mock_clusterman_metrics.generate_key_with_dimensions.side_effect = lambda name, dims: (
            f'{name}|framework_name={dims["framework_name"]},webui_url={dims["webui_url"]}'
        )
        resources = get_spark_resource_requirements(
            spark_config_dict, "http://spark.yelp"
        )

    assert resources == {
        metric_key_template.format(resource="cpus"): 4,
        metric_key_template.format(resource="mem"): expected_memory_request,
        metric_key_template.format(resource="disk"): expected_memory_request,
    }
