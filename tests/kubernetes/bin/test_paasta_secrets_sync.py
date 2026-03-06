from typing import Optional
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.bin.paasta_secrets_sync import _get_dict_signature
from paasta_tools.kubernetes.bin.paasta_secrets_sync import (
    get_services_to_k8s_namespaces_to_allowlist,
)
from paasta_tools.kubernetes.bin.paasta_secrets_sync import main
from paasta_tools.kubernetes.bin.paasta_secrets_sync import parse_args
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_all_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_boto_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_crypto_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_datastore_credentials
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_ssm_secrets
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.utils import DEFAULT_SOA_DIR


@pytest.fixture(scope="session", autouse=True)
def mock_time_sleep():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.time.sleep",
        autospec=True,
    ) as _fixture:
        yield _fixture


def test_parse_args():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.argparse.ArgumentParser",
        autospec=True,
    ):
        assert parse_args()


def test_main():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.parse_args", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.metrics_lib.system_timer",
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.KubeClient", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.sync_all_secrets",
        autospec=True,
    ) as mock_sync_all_secrets:
        mock_sync_all_secrets.return_value = True
        with pytest.raises(SystemExit) as e:
            main()
            assert e.value.code == 0
        mock_sync_all_secrets.return_value = False
        with pytest.raises(SystemExit) as e:
            main()
            assert e.value.code == 1


def test_sync_all_secrets():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.sync_secrets", autospec=True
    ) as mock_sync_secrets, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.ensure_namespace",
        autospec=True,
    ):
        services_to_k8s_namespaces_to_allowlist = {
            "foo": {"paastasvc-foo": None},
            "bar": {"paastasvc-foo": {"barsecret"}},
        }

        mock_sync_secrets.side_effect = [True, True]
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces_to_allowlist=services_to_k8s_namespaces_to_allowlist,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="./vault-token",
        )

        mock_sync_secrets.side_effect = [True, False]
        assert not sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces_to_allowlist=services_to_k8s_namespaces_to_allowlist,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="./vault-token",
        )

        mock_sync_secrets.side_effect = None
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces_to_allowlist=services_to_k8s_namespaces_to_allowlist,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="./vault-token",
        )


def test_sync_shared():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_config_loader, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_service_instance_list",
        autospec=True,
    ) as mock_get_service_instance_list:
        kube_client = mock.Mock()
        kube_client.core = mock.MagicMock()

        # If no services besides _shared are passed, we should get an empty dict, as
        # get_services_to_k8s_namespaces_to_allowlist only adds shared secrets that are used by the other services
        # listed.
        assert (
            get_services_to_k8s_namespaces_to_allowlist(
                ["_shared"],
                "fake_cluster",
                "/fake/soa/dir",
                kube_client,
            )
            == {}
        )

        def fake_config_loader_init(
            service: str,
            soa_dir: str = DEFAULT_SOA_DIR,
            load_deployments: bool = True,
        ):
            loader = mock.Mock()
            if service == "foo":
                loader.instance_configs.return_value = [
                    KubernetesDeploymentConfig(
                        service="foo",
                        instance="a",
                        cluster="fake_cluster",
                        config_dict={
                            "namespace": "paastasvc-foo",
                            "env": {
                                "A": "SECRET(foo-secret)",
                                "B": "SHARED_SECRET(shared_secret1)",
                            },
                        },
                        soa_dir=soa_dir,
                        branch_dict=None,
                    ),
                ]
            elif service == "bar":
                loader.instance_configs.return_value = [
                    KubernetesDeploymentConfig(
                        service="bar",
                        instance="a",
                        cluster="fake_cluster",
                        config_dict={
                            "namespace": "paastasvc-bar",
                            "env": {
                                "A": "SECRET(bar-secret1)",
                                "B": "SECRET(bar-secret2)",
                                "C": "SHARED_SECRET(shared_secret2)",
                            },
                        },
                        soa_dir=soa_dir,
                        branch_dict=None,
                    ),
                    KubernetesDeploymentConfig(
                        service="bar",
                        instance="b",
                        cluster="fake_cluster",
                        config_dict={
                            "namespace": "paasta",
                            "env": {
                                "A": "SECRET(bar-secret2)",
                                "B": "SECRET(bar-secret3)",
                                "C": "SHARED_SECRET(shared_secret3)",
                            },
                        },
                        soa_dir=soa_dir,
                        branch_dict=None,
                    ),
                ]
            elif service == "flink-service":
                loader.instance_configs.return_value = []
            else:
                raise ValueError(
                    f"only services foo and bar are expected here, got {service}"
                )

            return loader

        mock_config_loader.side_effect = fake_config_loader_init

        def fake_get_service_instance_list(
            service: str,
            cluster: Optional[str] = None,
            instance_type: str = None,
            soa_dir: str = DEFAULT_SOA_DIR,
        ):
            if instance_type == "flink" and service == "flink-service":
                return ["flink-service"]
            else:
                return []

        mock_get_service_instance_list.side_effect = fake_get_service_instance_list

        assert get_services_to_k8s_namespaces_to_allowlist(
            ["_shared", "foo", "bar", "flink-service"],
            "fake_cluster",
            "/fake/soa/dir",
            kube_client,
        ) == {
            "_shared": {
                "paastasvc-foo": {"shared_secret1"},
                "paastasvc-bar": {"shared_secret2"},
                "paasta": {"shared_secret3"},
                "paasta-flinks": None,
            },
            "foo": {
                "paastasvc-foo": {"foo-secret"},
            },
            "bar": {
                "paastasvc-bar": {"bar-secret1", "bar-secret2"},
                "paasta": {"bar-secret2", "bar-secret3"},
            },
            "flink-service": {
                "paasta-flinks": None,
            },
        }


@pytest.fixture
def paasta_secrets_patches():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_provider",
        autospec=True,
    ) as mock_get_secret_provider, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.os.scandir", autospec=True
    ) as mock_scandir, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.open",
        create=True,
        autospec=None,
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_signature",
        autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret", autospec=True
    ) as mock_create_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret_signature",
        autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret", autospec=True
    ) as mock_update_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret_signature",
        autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.json.load", autospec=True
    ), mock.patch(
        "os.path.isdir", autospec=True, return_value=True
    ), mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config",
        autospec=True,
    ):

        yield (
            mock_get_secret_provider,
            mock_scandir,
            mock_get_kubernetes_secret_signature,
            mock_create_secret,
            mock_create_kubernetes_secret_signature,
            mock_update_secret,
            mock_update_kubernetes_secret_signature,
        )


namespaces = [
    "paasta",
    "tron",
]


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_empty_soa_dir(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_scandir.return_value.__enter__.return_value = []

    assert sync_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        namespace=namespace,
        vault_token_file="./vault-token",
        secret_allowlist=None,
    )


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_soa_no_json_files(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_scandir.return_value.__enter__.return_value = [
        mock.Mock(path="some_non_json_file")
    ]

    assert sync_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        namespace=namespace,
        vault_token_file="./vault-token",
        secret_allowlist=None,
    )


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_signatures_match(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_get_secret_provider.return_value = mock.Mock(
        get_secret_signature_from_data=mock.Mock(return_value="123abc"),
        decrypt_secret_raw=mock.Mock(return_value=b""),
    )
    mock_file = mock.Mock(path="./some_file.json")
    mock_file.name = "some_file.json"  # have to set separately because of Mock argument
    mock_scandir.return_value.__enter__.return_value = [mock_file]

    mock_get_kubernetes_secret_signature.return_value = "123abc"
    assert sync_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        namespace=namespace,
        vault_token_file="./vault-token",
        secret_allowlist=None,
    )
    assert mock_get_kubernetes_secret_signature.called
    _, kwargs = mock_get_kubernetes_secret_signature.call_args
    assert kwargs.get("namespace") == namespace
    assert not mock_create_secret.called
    assert not mock_update_secret.called
    assert not mock_create_kubernetes_secret_signature.called
    assert not mock_update_kubernetes_secret_signature.called


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_signature_changed(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches
    mock_get_secret_provider.return_value = mock.Mock(
        get_secret_signature_from_data=mock.Mock(return_value="123abc"),
        decrypt_secret_raw=mock.Mock(return_value=b""),
    )

    mock_file = mock.Mock(path="./some_file.json")
    mock_file.name = "some_file.json"  # have to set separately because of Mock argument
    mock_scandir.return_value.__enter__.return_value = [mock_file]

    mock_get_kubernetes_secret_signature.return_value = "123def"
    assert sync_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        namespace=namespace,
        vault_token_file="./vault-token",
        secret_allowlist=None,
    )
    assert mock_get_kubernetes_secret_signature.called
    assert not mock_create_secret.called
    assert mock_update_secret.called
    _, kwargs = mock_update_secret.call_args_list[-1]
    assert kwargs.get("namespace") == namespace
    assert not mock_create_kubernetes_secret_signature.called
    assert mock_update_kubernetes_secret_signature.called
    _, kwargs = mock_update_kubernetes_secret_signature.call_args_list[-1]
    assert kwargs.get("namespace") == namespace


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_not_exist(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_get_secret_provider.return_value = mock.Mock(
        get_secret_signature_from_data=mock.Mock(return_value="abc"),
        decrypt_secret_raw=mock.Mock(return_value=b""),
    )

    mock_file = mock.Mock(path="./some_file.json")
    mock_file.name = "some_file.json"  # have to set separately because of Mock argument
    mock_scandir.return_value.__enter__.return_value = [mock_file]

    mock_get_kubernetes_secret_signature.return_value = None

    assert sync_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        namespace=namespace,
        vault_token_file="./vault-token",
        secret_allowlist=None,
    )
    assert mock_get_kubernetes_secret_signature.called
    assert mock_create_secret.called
    _, kwargs = mock_create_secret.call_args_list[-1]
    assert kwargs.get("namespace") == namespace
    assert not mock_update_secret.called
    assert mock_create_kubernetes_secret_signature.called
    _, kwargs = mock_create_kubernetes_secret_signature.call_args_list[-1]
    assert kwargs.get("namespace") == namespace
    assert not mock_update_kubernetes_secret_signature.called


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_exists_but_no_signature(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_get_secret_provider.return_value = mock.Mock(
        get_secret_signature_from_data=mock.Mock(return_value="abc"),
        decrypt_secret_raw=mock.Mock(return_value=b""),
    )

    mock_file = mock.Mock(path="./some_file.json")
    mock_file.name = "some_file.json"  # have to set separately because of Mock argument
    mock_scandir.return_value.__enter__.return_value = [mock_file]

    mock_get_kubernetes_secret_signature.return_value = None

    mock_create_secret.side_effect = ApiException(409)
    assert sync_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        namespace=namespace,
        vault_token_file="./vault-token",
        secret_allowlist=None,
    )
    assert mock_get_kubernetes_secret_signature.called
    assert mock_create_secret.called
    assert mock_update_secret.called  # previously did not update
    assert mock_create_kubernetes_secret_signature.called
    assert not mock_update_kubernetes_secret_signature.called


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_secret_api_exception(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_get_secret_provider.return_value = mock.Mock(
        get_secret_signature_from_data=mock.Mock(return_value="abc"),
        decrypt_secret_raw=mock.Mock(return_value=b""),
    )

    mock_file = mock.Mock(path="./some_file.json")
    mock_file.name = "some_file.json"  # have to set separately because of Mock argument
    mock_scandir.return_value.__enter__.return_value = [mock_file]

    mock_get_kubernetes_secret_signature.return_value = None

    mock_create_secret.side_effect = ApiException(404)
    with pytest.raises(ApiException):
        assert sync_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
            vault_token_file="./vault-token",
            secret_allowlist=None,
        )


@pytest.mark.parametrize("namespace", namespaces)
def test_sync_secrets_only_does_allowlisted_files(paasta_secrets_patches, namespace):
    (
        mock_get_secret_provider,
        mock_scandir,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
    ) = paasta_secrets_patches

    mock_get_secret_provider.return_value = mock.Mock(
        get_secret_signature_from_data=mock.Mock(return_value="abc"),
        decrypt_secret_raw=mock.Mock(return_value=b""),
    )

    mock_file1 = mock.Mock(path="./some_file1.json")
    mock_file1.name = (
        "some_file1.json"  # have to set separately because of Mock argument
    )
    mock_file2 = mock.Mock(path="./some_file2.json")
    mock_file2.name = (
        "some_file2.json"  # have to set separately because of Mock argument
    )
    mock_scandir.return_value.__enter__.return_value = [mock_file1, mock_file2]

    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_or_update_k8s_secret",
        autospec=True,
    ) as mock_create_or_update:
        assert sync_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
            vault_token_file="./vault-token",
            secret_allowlist={"some_file1"},
        )

        # It should only sync some_file1, not some_file2 because that's not in the allowlist.
        assert mock_create_or_update.call_count == 1
        assert (
            mock_create_or_update.call_args_list[0][1]["secret_name"]
            == f"{namespace}-secret-universe-some--file1"
        )


@pytest.fixture
def boto_keys_patches():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.open",
        create=True,
        autospec=None,
        return_value=mock.MagicMock(),
    ) as mock_open, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_signature",
        autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret",
        autospec=True,
    ) as mock_create_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret_signature",
        autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret",
        autospec=True,
    ) as mock_update_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret_signature",
        autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_config_loader, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config",
        autospec=True,
    ):
        yield (
            mock_open,
            mock_open.return_value.__enter__.return_value,
            mock_get_kubernetes_secret_signature,
            mock_create_secret,
            mock_create_kubernetes_secret_signature,
            mock_update_secret,
            mock_update_kubernetes_secret_signature,
            mock_config_loader,
            mock_config_loader.return_value.instance_configs,
        )


def test_sync_boto_secrets_create(boto_keys_patches):
    (
        mock_open,
        mock_open_handle,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = boto_keys_patches

    config_dict = {"boto_keys": ["scribereader"]}
    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict=config_dict,
        branch_dict=None,
        soa_dir="/nail/blah",
    )
    mock_config_loader_instances.return_value = [deployment]

    expected_secret_data = {
        "scribereader-sh": "ZmlsZTE=",
        "scribereader-yaml": "ZmlsZTI=",
        "scribereader-json": "ZmlsZTM=",
        "scribereader-cfg": "ZmlsZTQ=",
    }

    mock_open_handle.read.side_effect = [
        "file1",
        "file2",
        "file3",
        "file4",
        "eksfile1",
        "eksfile2",
        "eksfile3",
        "eksfile4",
    ]
    mock_get_kubernetes_secret_signature.return_value = None
    assert sync_boto_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        soa_dir="/nail/blah",
    )
    assert mock_create_secret.called
    assert not mock_update_secret.called
    call_args = mock_create_secret.call_args_list
    assert call_args[0][1]["secret_data"] == expected_secret_data
    assert mock_create_kubernetes_secret_signature.called


def test_sync_boto_secrets_update(boto_keys_patches):
    (
        mock_open,
        mock_open_handle,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = boto_keys_patches

    config_dict = {"boto_keys": ["scribereader"]}
    expected_secret_data = {
        "scribereader-sh": "ZmlsZTE=",
        "scribereader-yaml": "ZmlsZTI=",
        "scribereader-json": "ZmlsZTM=",
        "scribereader-cfg": "ZmlsZTQ=",
    }

    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict=config_dict,
        branch_dict=None,
        soa_dir="/nail/blah",
    )
    mock_config_loader_instances.return_value = [deployment]

    mock_open_handle.read.side_effect = [
        "file1",
        "file2",
        "file3",
        "file4",
        "eksfile1",
        "eksfile2",
        "eksfile3",
        "eksfile4",
    ]
    mock_get_kubernetes_secret_signature.return_value = "1235abc"
    assert sync_boto_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        soa_dir="/nail/blah",
    )
    assert mock_update_secret.called
    call_args = mock_update_secret.call_args_list
    assert call_args[0][1]["secret_data"] == expected_secret_data
    assert mock_update_kubernetes_secret_signature.called


def test_sync_boto_secrets_noop(boto_keys_patches):
    (
        mock_open,
        mock_open_handle,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = boto_keys_patches

    mock_open_handle.read.side_effect = [
        "file1",
        "file2",
        "file3",
        "file4",
        "eksfile1",
        "eksfile2",
        "eksfile3",
        "eksfile4",
    ]
    mock_get_kubernetes_secret_signature.return_value = (
        "4c3da4da5d97294f69527dc92c2b930ce127522c"
    )

    assert sync_boto_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        soa_dir="/nail/blah",
    )
    assert not mock_update_secret.called
    assert not mock_create_secret.called


def test_sync_boto_secrets_exists_but_no_signature(boto_keys_patches):
    (
        mock_open,
        mock_open_handle,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = boto_keys_patches

    config_dict = {"boto_keys": ["scribereader"]}

    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict=config_dict,
        branch_dict=None,
        soa_dir="/nail/blah",
    )
    mock_config_loader_instances.return_value = [deployment]

    mock_open_handle.read.side_effect = [
        "file1",
        "file2",
        "file3",
        "file4",
        "eksfile1",
        "eksfile2",
        "eksfile3",
        "eksfile4",
    ]
    mock_get_kubernetes_secret_signature.return_value = None
    mock_create_secret.side_effect = ApiException(409)

    assert sync_boto_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        soa_dir="/nail/blah",
    )
    assert mock_get_kubernetes_secret_signature.called
    assert mock_create_secret.called
    assert mock_update_secret.called
    assert mock_create_kubernetes_secret_signature.called
    assert not mock_update_kubernetes_secret_signature.called


@pytest.fixture
def crypto_keys_patches():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_provider",
        autospec=True,
    ) as provider, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_signature",
        autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret",
        autospec=True,
    ) as mock_create_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret_signature",
        autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret",
        autospec=True,
    ) as mock_update_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret_signature",
        autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_config_loader, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config",
        autospec=True,
    ):
        yield (
            provider,
            mock_get_kubernetes_secret_signature,
            mock_create_secret,
            mock_create_kubernetes_secret_signature,
            mock_update_secret,
            mock_update_kubernetes_secret_signature,
            mock_config_loader,
            mock_config_loader.return_value.instance_configs,
        )


@pytest.fixture
def datastore_credentials_patches():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_provider",
        autospec=True,
    ) as mock_get_secret_provider, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_signature",
        autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret",
        autospec=True,
    ) as mock_create_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret_signature",
        autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret",
        autospec=True,
    ) as mock_update_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret_signature",
        autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_config_loader, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config",
        autospec=True,
    ):
        yield (
            mock_get_kubernetes_secret_signature,
            mock_create_secret,
            mock_create_kubernetes_secret_signature,
            mock_update_secret,
            mock_update_kubernetes_secret_signature,
            mock_config_loader,
            mock_config_loader.return_value.instance_configs,
            mock_get_secret_provider,
        )


@pytest.mark.parametrize(
    "config_dict, expected_keys_in_secrets_data",
    [
        (
            {"datastore_credentials": {"mysql": ["credential1", "credential2"]}},
            [
                "secrets-datastore-mysql-credential1",
                "secrets-datastore-mysql-credential2",
            ],
        ),
        (
            {
                "datastore_credentials": {
                    "mysql": ["credential1", "credential2"],
                    "cassandra": ["credential3"],
                }
            },
            [
                "secrets-datastore-mysql-credential1",
                "secrets-datastore-mysql-credential2",
                "secrets-datastore-cassandra-credential3",
            ],
        ),
        ({"datastore_credentials": {}}, []),
        ({}, []),
    ],
)
def test_sync_datastore_secrets(
    datastore_credentials_patches,
    config_dict,
    expected_keys_in_secrets_data,
):
    (
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
        mock_get_secret_provider,
    ) = datastore_credentials_patches
    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict=config_dict,
        branch_dict=None,
        soa_dir="/nail/blah",
    )
    mock_get_secret_provider.return_value.get_data_from_vault_path.return_value = {
        "mock-credential-user1": "username",
        "mock-credential-password1": "password",
    }
    mock_config_loader_instances.return_value = [deployment]
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_or_update_k8s_secret",
        autospec=True,
    ) as mock_create_or_update:
        assert sync_datastore_credentials(
            kube_client=mock.Mock(),
            cluster="mega-cluster",
            service="my-service",
            secret_provider_name="faulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="/.vault-token",
        )
        # kwargs contains the calls to mock_create_or_update. check the secret_data in the lambda
        _, _, kwargs = mock_create_or_update.mock_calls[0]
        secret_data = kwargs["get_secret_data"]()
        assert len(secret_data) == len(expected_keys_in_secrets_data)
        for key in expected_keys_in_secrets_data:
            assert key in secret_data


@pytest.fixture()
def vault_key_versions():
    return [
        {
            "key": "foo",
            "key_name": "fake-key",
            "key_version": 1,
        },
        {
            "key": "foo",
            "key_name": "fake-key",
            "key_version": 2,
        },
    ]


@pytest.fixture()
def vault_key_versions_as_k8s_secret():
    return (
        "W3sia2V5IjogImZvbyIsICJrZXlfbmFtZSI6ICJ"
        "mYWtlLWtleSIsICJrZXlfdmVyc2lvbiI6IDF9LC"
        "B7ImtleSI6ICJmb28iLCAia2V5X25hbWUiOiAiZ"
        "mFrZS1rZXkiLCAia2V5X3ZlcnNpb24iOiAyfV0="
    )


def test_sync_crypto_secrets_create(
    crypto_keys_patches, vault_key_versions, vault_key_versions_as_k8s_secret
):
    (
        provider,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = crypto_keys_patches

    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict={"crypto_keys": {"decrypt": ["fake-key"]}},
        branch_dict=None,
        soa_dir="/nail/blah",
    )

    mock_config_loader_instances.return_value = [deployment]

    provider.return_value.get_key_versions.return_value = vault_key_versions

    mock_get_kubernetes_secret_signature.return_value = None
    assert sync_crypto_secrets(
        kube_client=mock.Mock(),
        cluster="pentos-devc",
        service="u2",
        secret_provider_name="faulty",
        vault_cluster_config={},
        soa_dir="/blah/blah",
        vault_token_file="/.vault-token",
    )

    assert mock_create_secret.called
    assert not mock_update_secret.called
    call_args = mock_create_secret.call_args_list
    assert call_args[0][1]["secret_data"] == {
        "private-fake-key": vault_key_versions_as_k8s_secret
    }
    assert mock_create_kubernetes_secret_signature.called


def test_sync_crypto_secrets_update(
    crypto_keys_patches, vault_key_versions, vault_key_versions_as_k8s_secret
):
    (
        provider,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = crypto_keys_patches

    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict={"crypto_keys": {"encrypt": ["fake-key"]}},
        branch_dict=None,
        soa_dir="/nail/blah",
    )

    mock_config_loader_instances.return_value = [deployment]
    provider.return_value.get_key_versions.return_value = vault_key_versions

    mock_get_kubernetes_secret_signature.return_value = "dummy-signature"
    assert sync_crypto_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="faulty",
        vault_cluster_config={},
        soa_dir="/blah/blah",
        vault_token_file="/.vault-token",
    )
    assert mock_update_secret.called
    call_args = mock_update_secret.call_args_list
    assert call_args[0][1]["secret_data"] == {
        "public-fake-key": vault_key_versions_as_k8s_secret
    }
    assert mock_update_kubernetes_secret_signature.called


def test_sync_crypto_secrets_noop(
    crypto_keys_patches, vault_key_versions, vault_key_versions_as_k8s_secret
):
    (
        provider,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = crypto_keys_patches

    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict={"crypto_keys": {"encrypt": ["fake-key"]}},
        branch_dict=None,
        soa_dir="/nail/blah",
    )

    mock_config_loader_instances.return_value = [deployment]
    provider.return_value.get_key_versions.return_value = vault_key_versions

    mock_get_kubernetes_secret_signature.return_value = (
        "c8ec57c05617ec23d93fea883817140c249408fe"
    )

    assert sync_crypto_secrets(
        kube_client=mock.Mock(),
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        vault_token_file="/.vault-token",
    )
    assert mock_get_kubernetes_secret_signature.return_value == _get_dict_signature(
        {"public-fake-key": vault_key_versions_as_k8s_secret}
    )
    assert not mock_update_secret.called
    assert not mock_create_secret.called


def test_sync_crypto_secrets_exist_but_no_signature(
    crypto_keys_patches, vault_key_versions, vault_key_versions_as_k8s_secret
):
    (
        provider,
        mock_get_kubernetes_secret_signature,
        mock_create_secret,
        mock_create_kubernetes_secret_signature,
        mock_update_secret,
        mock_update_kubernetes_secret_signature,
        mock_config_loader,
        mock_config_loader_instances,
    ) = crypto_keys_patches

    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict={"crypto_keys": {"encrypt": ["fake-key"]}},
        branch_dict=None,
        soa_dir="/nail/blah",
    )

    mock_config_loader_instances.return_value = [deployment]
    provider.return_value.get_key_versions.return_value = vault_key_versions

    mock_get_kubernetes_secret_signature.return_value = None
    mock_create_secret.side_effect = ApiException(409)

    assert sync_crypto_secrets(
        kube_client=mock.Mock,
        cluster="westeros-prod",
        service="universe",
        secret_provider_name="vaulty",
        vault_cluster_config={},
        soa_dir="/nail/blah",
        vault_token_file="/.vault-token",
    )
    assert mock_get_kubernetes_secret_signature.called
    assert mock_create_secret.called
    assert mock_update_secret.called
    assert mock_create_kubernetes_secret_signature.called
    assert not mock_update_kubernetes_secret_signature.called


@pytest.fixture
def ssm_patches():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_config_loader, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_config, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.boto3",
        autospec=True,
    ) as mock_boto3, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_or_update_k8s_secret",
        autospec=True,
    ) as mock_create_or_update:
        yield (
            mock_config_loader,
            mock_load_system_config,
            mock_boto3,
            mock_create_or_update,
        )


def test_sync_ssm_secrets_happy_path(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    mock_load_system_config.return_value.get_kube_clusters.return_value = {
        "my-cluster": {"aws_region": "us-west-2"}
    }

    mock_sts_client = mock.MagicMock()
    mock_ssm_client = mock.MagicMock()
    mock_boto3.client.side_effect = [mock_sts_client, mock_ssm_client]

    mock_sts_client.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "fake_key",
            "SecretAccessKey": "fake_secret",
            "SessionToken": "fake_token",
        }
    }

    mock_ssm_client.get_parameter.return_value = {
        "Parameter": {"Value": "super_secret_value"}
    }

    mock_instance_config = mock.MagicMock(
        config_dict={
            "ssm_secrets": [
                {"source": "/aws/parameter/path", "secret_name": "ENV_VAR_NAME"}
            ]
        }
    )
    mock_instance_config.get_namespace.return_value = "paastasvc-my-service"
    mock_instance_config.get_sanitised_deployment_name.return_value = "service-instance"
    mock_config_loader.return_value.instance_configs.return_value = [
        mock_instance_config
    ]

    result = sync_ssm_secrets(
        kube_client=mock.Mock(),
        cluster="my-cluster",
        service="my-service",
        soa_dir="/fake/dir",
        namespace="paastasvc-my-service",
    )

    assert result is True

    # Verify Boto3 calls
    mock_boto3.client.assert_any_call("sts", region_name="us-west-2")
    mock_sts_client.assume_role.assert_called_with(
        RoleArn="arn:aws:iam::123456789012:role/paasta-secrets-sync",
        RoleSessionName="PaastaSecretsSync",
    )
    mock_ssm_client.get_parameter.assert_called_with(
        Name="/aws/parameter/path", WithDecryption=True
    )

    # Verify K8s Secret Creation
    assert mock_create_or_update.called
    _, kwargs = mock_create_or_update.call_args
    assert kwargs["service"] == "my-service"
    assert kwargs["namespace"] == "paastasvc-my-service"

    # "super_secret_value" in base64 is "c3VwZXJfc2VjcmV0X3ZhbHVl"
    secret_data = kwargs["get_secret_data"]()
    assert secret_data == {"ENV_VAR_NAME": "c3VwZXJfc2VjcmV0X3ZhbHVl"}

    expected_signature = _get_dict_signature(secret_data)
    assert kwargs["secret_signature"] == expected_signature


def test_sync_ssm_secrets_no_region_raises_error(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    # Empty cluster config
    mock_load_system_config.return_value.get_kube_clusters.return_value = {}

    with pytest.raises(RuntimeError) as excinfo:
        sync_ssm_secrets(
            kube_client=mock.Mock(),
            cluster="my-cluster",
            service="my-service",
            soa_dir="/fake/dir",
            namespace="paastasvc-my-service",
        )
    assert "Unable to determine AWS region" in str(excinfo.value)


def test_sync_ssm_secrets_client_error(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    mock_load_system_config.return_value.get_kube_clusters.return_value = {
        "my-cluster": {"aws_region": "us-west-2"}
    }
    mock_sts_client = mock.MagicMock()
    mock_ssm_client = mock.MagicMock()
    mock_boto3.client.side_effect = [mock_sts_client, mock_ssm_client]
    mock_sts_client.get_caller_identity.return_value = {"Account": "123"}

    mock_instance_config = mock.MagicMock(
        config_dict={
            "ssm_secrets": [{"source": "/aws/bad/path", "secret_name": "BAD_SECRET"}]
        }
    )
    mock_instance_config.get_namespace.return_value = "paastasvc-my-service"
    mock_config_loader.return_value.instance_configs.return_value = [
        mock_instance_config
    ]

    # Force ClientError
    mock_ssm_client.get_parameter.side_effect = ClientError(
        {"Error": {"Code": "ParameterNotFound", "Message": "Not Found"}}, "GetParameter"
    )

    result = sync_ssm_secrets(
        kube_client=mock.Mock(),
        cluster="my-cluster",
        service="my-service",
        soa_dir="/fake/dir",
        namespace="paastasvc-my-service",
    )

    # Should return False on failure, but handle the exception gracefully
    assert result is False
    assert not mock_create_or_update.called


def test_sync_ssm_secrets_api_exception(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    mock_load_system_config.return_value.get_kube_clusters.return_value = {
        "my-cluster": {"aws_region": "us-west-2"}
    }
    mock_sts_client = mock.MagicMock()
    mock_ssm_client = mock.MagicMock()
    mock_boto3.client.side_effect = [mock_sts_client, mock_ssm_client]
    mock_sts_client.get_caller_identity.return_value = {"Account": "123"}

    mock_ssm_client.get_parameter.return_value = {"Parameter": {"Value": "val"}}

    mock_instance_config = mock.MagicMock(
        config_dict={"ssm_secrets": [{"source": "/aws/path", "secret_name": "SECRET"}]}
    )
    mock_instance_config.get_namespace.return_value = "paastasvc-my-service"
    mock_instance_config.get_sanitised_deployment_name.return_value = (
        "my-service-instance"
    )

    mock_config_loader.return_value.instance_configs.return_value = [
        mock_instance_config
    ]

    # Kubernetes Failure
    mock_create_or_update.side_effect = ApiException(status=500)

    result = sync_ssm_secrets(
        kube_client=mock.Mock(),
        cluster="my-cluster",
        service="my-service",
        soa_dir="/fake/dir",
        namespace="paastasvc-my-service",
    )

    assert result is False
    assert mock_create_or_update.called


def test_sync_ssm_secrets_empty_secrets(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    mock_load_system_config.return_value.get_kube_clusters.return_value = {
        "my-cluster": {"aws_region": "us-west-2"}
    }
    mock_sts_client = mock.MagicMock()
    mock_ssm_client = mock.MagicMock()
    mock_boto3.client.side_effect = [mock_sts_client, mock_ssm_client]
    mock_sts_client.get_caller_identity.return_value = {"Account": "123"}

    # empty ssm_secrets
    mock_instance_config = mock.MagicMock(config_dict={"ssm_secrets": []})
    mock_instance_config.get_namespace.return_value = "paastasvc-my-service"
    mock_config_loader.return_value.instance_configs.return_value = [
        mock_instance_config
    ]

    result = sync_ssm_secrets(
        kube_client=mock.Mock(),
        cluster="my-cluster",
        service="my-service",
        soa_dir="/fake/dir",
        namespace="paastasvc-my-service",
    )

    assert result is True
    assert not mock_ssm_client.get_parameter.called
    assert not mock_create_or_update.called


def test_sync_ssm_secrets_no_secrets_configured(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    mock_load_system_config.return_value.get_kube_clusters.return_value = {
        "my-cluster": {"aws_region": "us-west-2"}
    }
    mock_sts_client = mock.MagicMock()
    mock_ssm_client = mock.MagicMock()
    mock_boto3.client.side_effect = [mock_sts_client, mock_ssm_client]
    mock_sts_client.get_caller_identity.return_value = {"Account": "123"}

    # ssm_secrets omitted entirely
    mock_instance_config = mock.MagicMock(config_dict={})
    mock_instance_config.get_namespace.return_value = "paastasvc-my-service"
    mock_config_loader.return_value.instance_configs.return_value = [
        mock_instance_config
    ]

    result = sync_ssm_secrets(
        kube_client=mock.Mock(),
        cluster="my-cluster",
        service="my-service",
        soa_dir="/fake/dir",
        namespace="paastasvc-my-service",
    )

    assert result is True
    assert not mock_ssm_client.get_parameter.called
    assert not mock_create_or_update.called


def test_sync_ssm_secrets_skips_non_matching_namespaces(ssm_patches):
    (
        mock_config_loader,
        mock_load_system_config,
        mock_boto3,
        mock_create_or_update,
    ) = ssm_patches

    mock_load_system_config.return_value.get_kube_clusters.return_value = {
        "my-cluster": {"aws_region": "us-west-2"}
    }
    mock_sts_client = mock.Mock()
    mock_ssm_client = mock.Mock()
    mock_boto3.client.side_effect = [mock_sts_client, mock_ssm_client]
    mock_sts_client.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "fake_key",
            "SecretAccessKey": "fake_secret",
            "SessionToken": "fake_token",
        }
    }
    mock_ssm_client.get_parameter.return_value = {"Parameter": {"Value": "val"}}

    # Instance 1: Default paasta service namespace
    instance_1 = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance-1",
        cluster="mega-cluster",
        config_dict={
            "ssm_secrets": [{"source": "/aws/path1", "secret_name": "SECRET1"}]
        },
        branch_dict=None,
        soa_dir="/nail/blah",
    )

    # Instance 2: Overridden namespace (should be skipped)
    instance_2 = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance-2",
        cluster="mega-cluster",
        config_dict={
            "namespace": "default",
            "ssm_secrets": [{"source": "/aws/path2", "secret_name": "SECRET1"}],
        },
        branch_dict=None,
        soa_dir="/nail/blah",
    )

    mock_config_loader.return_value.instance_configs.side_effect = [
        [instance_1, instance_2],
        [],
    ]

    sync_ssm_secrets(
        kube_client=mock.Mock(),
        cluster="my-cluster",
        service="my-service",
        soa_dir="/fake/dir",
        namespace="paastasvc-my-service",
    )

    # Ensure we only fetched the secret for the matching instance
    mock_ssm_client.get_parameter.assert_called_once_with(
        Name="/aws/path1", WithDecryption=True
    )

    # Should only create K8s secret for instance 1
    assert mock_create_or_update.call_count == 1
    assert mock_create_or_update.call_args[1]["namespace"] == "paastasvc-my-service"
