import mock
import pytest
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.bin.paasta_secrets_sync import main
from paasta_tools.kubernetes.bin.paasta_secrets_sync import parse_args
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_all_secrets
from paasta_tools.kubernetes.bin.paasta_secrets_sync import sync_secrets


def test_parse_args():
    with mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.argparse.ArgumentParser', autospec=True,
    ):
        assert parse_args()


def test_main():
    with mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.parse_args', autospec=True,
    ), mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.load_system_paasta_config', autospec=True,
    ), mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.KubeClient', autospec=True,
    ), mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.sync_all_secrets', autospec=True,
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
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.sync_secrets', autospec=True,
    ) as mock_sync_secrets:
        mock_sync_secrets.side_effect = [True, True]
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster='westeros-prod',
            service_list=['foo', 'bar'],
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )

        mock_sync_secrets.side_effect = [True, False]
        assert not sync_all_secrets(
            kube_client=mock.Mock(),
            cluster='westeros-prod',
            service_list=['foo', 'bar'],
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )

        mock_sync_secrets.side_effect = None
        assert sync_all_secrets(
            kube_client=mock.Mock(),
            cluster='westeros-prod',
            service_list=['foo', 'bar'],
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )


def test_sync_secrets():
    with mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.get_secret_provider', autospec=True,
    ) as mock_get_secret_provider, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.os.scandir', autospec=True,
    ) as mock_scandir, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.open', create=True, autospec=False,
    ), mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.get_kubernetes_secret_signature', autospec=True,
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.create_secret', autospec=True,
    ) as mock_create_secret, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.create_kubernetes_secret_signature', autospec=True,
    ) as mock_create_kubernetes_secret_signature, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.update_secret', autospec=True,
    ) as mock_update_secret, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.update_kubernetes_secret_signature', autospec=True,
    ) as mock_update_kubernetes_secret_signature, mock.patch(
        'paasta_tools.kubernetes.bin.paasta_secrets_sync.json.load', autospec=True,
    ):
        mock_scandir.return_value.__enter__.return_value = []
        mock_client = mock.Mock()
        assert sync_secrets(
            kube_client=mock_client,
            cluster='westeros-prod',
            service='universe',
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )

        mock_scandir.return_value.__enter__.return_value = [mock.Mock(path='some_file')]
        mock_client = mock.Mock()
        assert sync_secrets(
            kube_client=mock_client,
            cluster='westeros-prod',
            service='universe',
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )

        mock_get_secret_provider.return_value = mock.Mock(
            get_secret_signature_from_data=mock.Mock(return_value='123abc'),
        )
        mock_scandir.return_value.__enter__.return_value = [mock.Mock(path='some_file.json')]
        mock_client = mock.Mock()
        mock_get_kubernetes_secret_signature.return_value = '123abc'
        assert sync_secrets(
            kube_client=mock_client,
            cluster='westeros-prod',
            service='universe',
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )
        assert mock_get_kubernetes_secret_signature.called
        assert not mock_create_secret.called
        assert not mock_update_secret.called
        assert not mock_create_kubernetes_secret_signature.called
        assert not mock_update_kubernetes_secret_signature.called

        mock_get_kubernetes_secret_signature.return_value = '123def'
        assert sync_secrets(
            kube_client=mock_client,
            cluster='westeros-prod',
            service='universe',
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )
        assert mock_get_kubernetes_secret_signature.called
        assert not mock_create_secret.called
        assert mock_update_secret.called
        assert not mock_create_kubernetes_secret_signature.called
        assert mock_update_kubernetes_secret_signature.called
        mock_update_kubernetes_secret_signature.reset_mock()
        mock_update_secret.reset_mock()

        mock_get_kubernetes_secret_signature.return_value = None
        assert sync_secrets(
            kube_client=mock_client,
            cluster='westeros-prod',
            service='universe',
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
        )
        assert mock_get_kubernetes_secret_signature.called
        assert mock_create_secret.called
        assert not mock_update_secret.called
        assert mock_create_kubernetes_secret_signature.called
        assert not mock_update_kubernetes_secret_signature.called
        mock_update_kubernetes_secret_signature.reset_mock()
        mock_update_secret.reset_mock()

        mock_create_secret.side_effect = ApiException(409)
        assert sync_secrets(
            kube_client=mock_client,
            cluster='westeros-prod',
            service='universe',
            secret_provider_name='vaulty',
            vault_cluster_config={},
            soa_dir='/nail/blah',
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
                cluster='westeros-prod',
                service='universe',
                secret_provider_name='vaulty',
                vault_cluster_config={},
                soa_dir='/nail/blah',
            )
