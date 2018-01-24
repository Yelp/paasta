import mock
from pytest import raises

from paasta_tools.secret_providers.vault import SecretProvider


def test_secret_provider():
    SecretProvider(
        soa_dir='/nail/blah',
        service_name='universe',
        cluster_name='mesosstage',
    )


def test_decrypt_environment():
    with mock.patch(
        'paasta_tools.secret_providers.vault.get_vault_client', autospec=False,
    ) as mock_get_vault_client, mock.patch(
        'paasta_tools.secret_providers.vault.get_secret_name_from_ref', autospec=True,
    ) as mock_get_secret_name_from_ref, mock.patch(
        'paasta_tools.secret_providers.vault.get_plaintext', autospec=False,
    ) as mock_get_plaintext:
        mock_get_plaintext.return_value = ('SECRETSQUIRREL',)
        sp = SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_name='mesosstage',
        )
        mock_env = {
            'MY_VAR': 'SECRET(test-secret)',
            'ANOTHER_VAR': 'SECRET(another-secret)',
        }
        mock_get_secret_name_from_ref.return_value = "secret_name"
        ret = sp.decrypt_environment(
            environment=mock_env,
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
            vault_cluster_config={'mesosstage': 'devc'},
            some='kwarg',
        )
        mock_get_secret_name_from_ref.assert_has_calls(
            [mock.call('SECRET(test-secret)'), mock.call('SECRET(another-secret)')],
        )
        mock_get_vault_client.assert_called_with(
            ecosystem='devc',
            num_uses=2,
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
        )

        expected = {
            'MY_VAR': 'SECRETSQUIRREL',
            'ANOTHER_VAR': 'SECRETSQUIRREL',
        }
        assert ret == expected

        with raises(KeyError):
            sp.decrypt_environment(
                environment=mock_env,
                vault_auth_method='ldap',
                vault_token_file='/nail/blah',
                vault_cluster_config={'westeros-prod': 'devc'},
                some='kwarg',
            )
