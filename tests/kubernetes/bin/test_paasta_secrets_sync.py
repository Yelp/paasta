import mock
import pytest
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.bin.paasta_secrets_sync import _get_dict_signature
from paasta_tools.kubernetes.bin.paasta_secrets_sync import (
    get_services_to_k8s_namespaces,
)
from paasta_tools.kubernetes.bin.paasta_secrets_sync import main
from paasta_tools.kubernetes.bin.paasta_secrets_sync import parse_args
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_all_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_boto_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_crypto_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_secrets
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.utils import PaastaNotConfiguredError


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
    ):
        services_to_k8s_namespaces = {
            "foo": {"paastasvc-foo"},
            "bar": {"paastasvc-foo"},
        }

        mock_sync_secrets.side_effect = [True, True]
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces=services_to_k8s_namespaces,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="./vault-token",
        )

        mock_sync_secrets.side_effect = [True, False]
        assert not sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces=services_to_k8s_namespaces,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="./vault-token",
        )

        mock_sync_secrets.side_effect = None
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces=services_to_k8s_namespaces,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            vault_token_file="./vault-token",
        )


def test_sync_shared():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ):
        kube_client = mock.Mock()
        kube_client.core = mock.MagicMock()
        # _shared does no actual lookup, and as such works without cluster
        # we just need to ensure it returns non-empty namespaces
        assert get_services_to_k8s_namespaces(["_shared"], "", "", kube_client) != {}
        try:
            assert get_services_to_k8s_namespaces(["_foo"], "", "", kube_client) == {}
        except PaastaNotConfiguredError:
            # this check can only be done if /etc/paasta... exists and has a cluster
            # which is not the case on GHA and devboxes, hence we accept a failure
            pass


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
    mock_scandir.return_value.__enter__.return_value = [
        mock.Mock(path="some_file.json")
    ]

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
    )
    assert mock_get_kubernetes_secret_signature.called
    _, kwargs = mock_get_kubernetes_secret_signature.call_args_list[-1]
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

    mock_scandir.return_value.__enter__.return_value = [
        mock.Mock(path="some_file.json")
    ]

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

    mock_scandir.return_value.__enter__.return_value = [
        mock.Mock(path="some_file.json")
    ]

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

    mock_scandir.return_value.__enter__.return_value = [
        mock.Mock(path="some_file.json")
    ]

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

    mock_scandir.return_value.__enter__.return_value = [
        mock.Mock(path="some_file.json")
    ]

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
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.time.sleep",
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

    mock_open_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
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

    mock_open_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
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

    mock_open_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
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

    mock_open_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
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
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.time.sleep",
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
