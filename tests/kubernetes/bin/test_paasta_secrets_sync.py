from collections import defaultdict

import mock
import pytest
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.bin.paasta_secrets_sync import main
from paasta_tools.kubernetes.bin.paasta_secrets_sync import parse_args
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_all_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_boto_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_secrets
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.utils import get_instance_type_to_k8s_namespace


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
        services_to_k8s_namespaces = defaultdict(set)
        services_to_k8s_namespaces["foo"].add(
            get_instance_type_to_k8s_namespace()["kubernetes"]
        )
        services_to_k8s_namespaces["bar"].add(
            get_instance_type_to_k8s_namespace()["kubernetes"]
        )

        mock_sync_secrets.side_effect = [True, True]
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces=services_to_k8s_namespaces,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
        )

        mock_sync_secrets.side_effect = [True, False]
        assert not sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces=services_to_k8s_namespaces,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
        )

        mock_sync_secrets.side_effect = None
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster="westeros-prod",
            services_to_k8s_namespaces=services_to_k8s_namespaces,
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
        )


@pytest.mark.parametrize("namespace", [None, "tron"])
def test_sync_secrets(namespace):
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
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_kubernetes_secret_signature",
        autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret", autospec=True
    ) as mock_create_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_kubernetes_secret_signature",
        autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret", autospec=True
    ) as mock_update_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_kubernetes_secret_signature",
        autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.json.load", autospec=True
    ), mock.patch(
        "os.path.isdir", autospec=True, return_value=True
    ):
        mock_scandir.return_value.__enter__.return_value = []
        mock_client = mock.Mock()
        assert sync_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
        )

        mock_scandir.return_value.__enter__.return_value = [mock.Mock(path="some_file")]
        mock_client = mock.Mock()
        assert sync_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
        )

        mock_get_secret_provider.return_value = mock.Mock(
            get_secret_signature_from_data=mock.Mock(return_value="123abc")
        )
        mock_scandir.return_value.__enter__.return_value = [
            mock.Mock(path="some_file.json")
        ]
        mock_client = mock.Mock()
        mock_get_kubernetes_secret_signature.return_value = "123abc"
        assert sync_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
        )
        assert mock_get_kubernetes_secret_signature.called
        _, kwargs = mock_get_kubernetes_secret_signature.call_args_list[-1]
        assert kwargs.get("namespace") == namespace
        assert not mock_create_secret.called
        assert not mock_update_secret.called
        assert not mock_create_kubernetes_secret_signature.called
        assert not mock_update_kubernetes_secret_signature.called

        mock_get_kubernetes_secret_signature.return_value = "123def"
        assert sync_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
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
        mock_update_kubernetes_secret_signature.reset_mock()
        mock_update_secret.reset_mock()

        mock_get_kubernetes_secret_signature.return_value = None
        assert sync_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
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
        mock_update_kubernetes_secret_signature.reset_mock()
        mock_update_secret.reset_mock()

        mock_create_secret.side_effect = ApiException(409)
        assert sync_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace=namespace,
        )
        assert mock_get_kubernetes_secret_signature.called
        assert mock_create_secret.called
        assert not mock_update_secret.called
        assert mock_create_kubernetes_secret_signature.called
        assert not mock_update_kubernetes_secret_signature.called

        mock_create_secret.side_effect = ApiException(404)
        with pytest.raises(ApiException):
            assert sync_secrets(
                kube_client=mock_client,
                cluster="westeros-prod",
                service="universe",
                secret_provider_name="vaulty",
                vault_cluster_config={},
                soa_dir="/nail/blah",
                namespace=namespace,
            )


def test_sync_boto_secrets():
    with mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.open",
        create=True,
        autospec=None,
    ) as mock_open, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.get_kubernetes_secret_signature",
        autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_plaintext_dict_secret",
        autospec=True,
    ) as mock_create_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.create_kubernetes_secret_signature",
        autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_plaintext_dict_secret",
        autospec=True,
    ) as mock_update_secret, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.update_kubernetes_secret_signature",
        autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_config_loader, mock.patch(
        "paasta_tools.kubernetes.bin.paasta_secrets_sync.time.sleep", autospec=True,
    ):

        mock_client = mock.Mock()
        config_dict = {"boto_keys": ["scribereader"]}
        deployment = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        mock_loader = mock.MagicMock()
        mock_loader.instance_configs.return_value = [deployment]
        mock_config_loader.return_value = mock_loader
        mock_open.return_value = mock.MagicMock()
        mock_handle = mock_open.return_value.__enter__.return_value

        expected_secret_data = {
            "scribereader-sh": "ZmlsZTE=",
            "scribereader-yaml": "ZmlsZTI=",
            "scribereader-json": "ZmlsZTM=",
            "scribereader-cfg": "ZmlsZTQ=",
        }
        expected_signature = "4c3da4da5d97294f69527dc92c2b930ce127522c"

        # New secret
        mock_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
        mock_get_kubernetes_secret_signature.return_value = None
        assert sync_boto_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace="paasta",
        )
        assert mock_create_secret.called
        assert not mock_update_secret.called
        call_args = mock_create_secret.call_args_list
        assert call_args[0][1]["secret_data"] == expected_secret_data
        assert mock_create_kubernetes_secret_signature.called

        # Update secret
        mock_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
        mock_get_kubernetes_secret_signature.return_value = "1235abc"
        assert sync_boto_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace="paasta",
        )
        assert mock_update_secret.called
        call_args = mock_update_secret.call_args_list
        assert call_args[0][1]["secret_data"] == expected_secret_data
        assert mock_update_kubernetes_secret_signature.called

        # No changes needed
        mock_handle.read.side_effect = ["file1", "file2", "file3", "file4"]
        mock_get_kubernetes_secret_signature.return_value = expected_signature
        mock_update_secret.reset_mock()
        mock_create_secret.reset_mock()
        assert sync_boto_secrets(
            kube_client=mock_client,
            cluster="westeros-prod",
            service="universe",
            secret_provider_name="vaulty",
            vault_cluster_config={},
            soa_dir="/nail/blah",
            namespace="paasta",
        )
        assert not mock_update_secret.called
        assert not mock_create_secret.called
