import mock
from pytest import raises

from paasta_tools.secret_providers.vault import SecretProvider


def test_secret_provider():
    SecretProvider(
        soa_dir='/nail/blah',
        service_name='universe',
        cluster_names=['mesosstage'],
    )


def test_decrypt_environment():
    with mock.patch(
        'paasta_tools.secret_providers.vault.get_vault_client', autospec=False,
    ) as mock_get_vault_client, mock.patch(
        'paasta_tools.secret_providers.vault.get_secret_name_from_ref', autospec=True,
    ) as mock_get_secret_name_from_ref, mock.patch(
        'paasta_tools.secret_providers.vault.get_plaintext', autospec=False,
    ) as mock_get_plaintext:
        mock_get_plaintext.return_value = b'SECRETSQUIRREL'
        sp = SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_names=['mesosstage', ],
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
            vault_cluster_config={'mesosstage': 'devc'},
        )
        mock_env = {
            'MY_VAR': 'SECRET(test-secret)',
            'ANOTHER_VAR': 'SECRET(another-secret)',
        }
        mock_get_secret_name_from_ref.return_value = "secret_name"
        ret = sp.decrypt_environment(
            environment=mock_env,
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


def test_get_vault_ecosystems_for_clusters():
    sp = SecretProvider(
        soa_dir='/nail/blah',
        service_name='universe',
        cluster_names=['mesosstage', 'devc', 'prod'],
        vault_auth_method='ldap',
        vault_token_file='/nail/blah',
        vault_cluster_config={
            'mesosstage': 'devc',
            'devc': 'devc',
            'prod': 'prod',
        },
    )
    assert sorted(sp.get_vault_ecosystems_for_clusters()) == sorted(['devc', 'prod'])

    sp = SecretProvider(
        soa_dir='/nail/blah',
        service_name='universe',
        cluster_names=['mesosstage', 'devc', 'prod1'],
        vault_auth_method='ldap',
        vault_token_file='/nail/blah',
        vault_cluster_config={
            'mesosstage': 'devc',
            'devc': 'devc',
            'prod': 'prod',
        },
    )
    with raises(KeyError):
        sp.get_vault_ecosystems_for_clusters()


def test_write_secret():
    with mock.patch(
        'paasta_tools.secret_providers.vault.TempGpgKeyring', autospec=False,
    ), mock.patch(
        'paasta_tools.secret_providers.vault.get_vault_client', autospec=False,
    ) as mock_get_client, mock.patch(
        'paasta_tools.secret_providers.vault.encrypt_secret', autospec=False,
    ) as mock_encrypt_secret, mock.patch(
        'paasta_tools.secret_providers.read_service_configuration', autospec=False,
    ) as mock_read_service_configuration, mock.patch(
        'paasta_tools.secret_providers.vault.getpass', autospec=True,
    ):
        mock_read_service_configuration.return_value = {}
        sp = SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_names=['mesosstage'],
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
            vault_cluster_config={
                'mesosstage': 'devc',
            },
        )
        sp.write_secret(
            action='add',
            secret_name='mysecret',
            plaintext=b"SECRETSQUIRREL",
        )
        assert mock_get_client.called
        mock_encrypt_secret.assert_called_with(
            client=mock_get_client.return_value,
            action="add",
            ecosystem="devc",
            secret_name="mysecret",
            plaintext=b"SECRETSQUIRREL",
            service_name='universe',
            soa_dir='/nail/blah',
            transit_key='paasta',
        )

        mock_read_service_configuration.return_value = {'encryption_key': 'special-key'}
        sp = SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_names=['mesosstage'],
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
            vault_cluster_config={
                'mesosstage': 'devc',
            },
        )
        sp.write_secret(
            action='add',
            secret_name='mysecret',
            plaintext=b"SECRETSQUIRREL",
        )
        assert mock_get_client.called
        mock_encrypt_secret.assert_called_with(
            client=mock_get_client.return_value,
            action="add",
            ecosystem="devc",
            secret_name="mysecret",
            plaintext=b"SECRETSQUIRREL",
            service_name='universe',
            soa_dir='/nail/blah',
            transit_key='special-key',
        )


def test_decrypt_secret():
    with mock.patch(
        'paasta_tools.secret_providers.vault.get_vault_client', autospec=False,
    ) as mock_get_vault_client, mock.patch(
        'paasta_tools.secret_providers.vault.get_plaintext', autospec=False,
    ) as mock_get_plaintext, mock.patch(
        'paasta_tools.secret_providers.vault.getpass', autospec=True,
    ):
        mock_get_plaintext.return_value = b'SECRETSQUIRREL'
        sp = SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_names=['mesosstage', ],
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
            vault_cluster_config={'mesosstage': 'devc'},
        )
        assert sp.decrypt_secret('mysecret') == 'SECRETSQUIRREL'
        assert mock_get_vault_client.called
        mock_get_plaintext.assert_called_with(
            client=mock_get_vault_client.return_value,
            path='/nail/blah/universe/secrets/mysecret.json',
            env='devc',
            cache_enabled=False,
            cache_key=None,
            cache_dir=None,
            context='universe',
        )


def test_decrypt_secret_raw():
    with mock.patch(
        'paasta_tools.secret_providers.vault.get_vault_client', autospec=False,
    ) as mock_get_vault_client, mock.patch(
        'paasta_tools.secret_providers.vault.get_plaintext', autospec=False,
    ) as mock_get_plaintext, mock.patch(
        'paasta_tools.secret_providers.vault.getpass', autospec=True,
    ):
        mock_get_plaintext.return_value = b'SECRETSQUIRREL'
        sp = SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_names=['mesosstage', ],
            vault_auth_method='ldap',
            vault_token_file='/nail/blah',
            vault_cluster_config={'mesosstage': 'devc'},
        )
        assert sp.decrypt_secret_raw('mysecret') == b'SECRETSQUIRREL'
        assert mock_get_vault_client.called
        mock_get_plaintext.assert_called_with(
            client=mock_get_vault_client.return_value,
            path='/nail/blah/universe/secrets/mysecret.json',
            env='devc',
            cache_enabled=False,
            cache_key=None,
            cache_dir=None,
            context='universe',
        )
