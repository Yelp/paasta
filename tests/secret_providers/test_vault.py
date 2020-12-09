import mock
from pytest import fixture
from pytest import raises

from paasta_tools.secret_providers.vault import SecretProvider


@fixture
def mock_secret_provider():
    with mock.patch(
        "paasta_tools.secret_providers.vault.SecretProvider.get_vault_ecosystems_for_clusters",
        autospec=True,
        return_value=["devc"],
    ), mock.patch(
        "paasta_tools.secret_providers.vault.get_vault_client", autospec=True
    ):
        return SecretProvider(
            soa_dir="/nail/blah",
            service_name="universe",
            cluster_names=["mesosstage"],
            vault_auth_method="token",
        )


def test_secret_provider(mock_secret_provider):
    assert mock_secret_provider.ecosystems == ["devc"]
    assert mock_secret_provider.clients["devc"]


def test_decrypt_environment(mock_secret_provider):
    with mock.patch(
        "paasta_tools.secret_providers.vault.get_secret_name_from_ref", autospec=True
    ) as mock_get_secret_name_from_ref, mock.patch(
        "paasta_tools.secret_providers.vault.get_plaintext", autospec=False
    ) as mock_get_plaintext:
        mock_get_plaintext.return_value = b"SECRETSQUIRREL"
        mock_env = {
            "MY_VAR": "SECRET(test-secret)",
            "ANOTHER_VAR": "SECRET(another-secret)",
        }
        mock_get_secret_name_from_ref.return_value = "secret_name"
        ret = mock_secret_provider.decrypt_environment(
            environment=mock_env, some="kwarg"
        )
        mock_get_secret_name_from_ref.assert_has_calls(
            [mock.call("SECRET(test-secret)"), mock.call("SECRET(another-secret)")]
        )

        expected = {"MY_VAR": "SECRETSQUIRREL", "ANOTHER_VAR": "SECRETSQUIRREL"}
        assert ret == expected


def test_get_vault_ecosystems_for_clusters(mock_secret_provider):
    mock_secret_provider.cluster_names = ["mesosstage", "devc", "prod"]
    mock_secret_provider.vault_cluster_config = {
        "mesosstage": "devc",
        "devc": "devc",
        "prod": "prod",
    }
    assert sorted(mock_secret_provider.get_vault_ecosystems_for_clusters()) == sorted(
        ["devc", "prod"]
    )

    mock_secret_provider.cluster_names = ["mesosstage", "devc", "prod1"]
    with raises(KeyError):
        mock_secret_provider.get_vault_ecosystems_for_clusters()


def test_write_secret(mock_secret_provider):
    with mock.patch(
        "paasta_tools.secret_providers.vault.TempGpgKeyring", autospec=False
    ), mock.patch(
        "paasta_tools.secret_providers.vault.encrypt_secret", autospec=False
    ) as mock_encrypt_secret:
        mock_secret_provider.write_secret(
            action="add",
            secret_name="mysecret",
            plaintext=b"SECRETSQUIRREL",
            cross_environment_motivation="because ...",
        )
        mock_encrypt_secret.assert_called_with(
            client=mock_secret_provider.clients["devc"],
            action="add",
            ecosystem="devc",
            secret_name="mysecret",
            plaintext=b"SECRETSQUIRREL",
            service_name="universe",
            soa_dir="/nail/blah",
            transit_key="paasta",
            cross_environment_motivation="because ...",
        )

        mock_secret_provider.encryption_key = "special-key"
        mock_secret_provider.write_secret(
            action="add", secret_name="mysecret", plaintext=b"SECRETSQUIRREL"
        )
        mock_encrypt_secret.assert_called_with(
            client=mock_secret_provider.clients["devc"],
            action="add",
            ecosystem="devc",
            secret_name="mysecret",
            plaintext=b"SECRETSQUIRREL",
            service_name="universe",
            soa_dir="/nail/blah",
            transit_key="special-key",
            cross_environment_motivation=None,
        )


def test_decrypt_secret(mock_secret_provider):
    with mock.patch(
        "paasta_tools.secret_providers.vault.get_plaintext", autospec=False
    ) as mock_get_plaintext:
        mock_get_plaintext.return_value = b"SECRETSQUIRREL"
        assert mock_secret_provider.decrypt_secret("mysecret") == "SECRETSQUIRREL"
        mock_get_plaintext.assert_called_with(
            client=mock_secret_provider.clients["devc"],
            path="/nail/blah/universe/secrets/mysecret.json",
            env="devc",
            cache_enabled=False,
            cache_key=None,
            cache_dir=None,
            context="universe",
        )


def test_decrypt_secret_raw(mock_secret_provider):
    with mock.patch(
        "paasta_tools.secret_providers.vault.get_plaintext", autospec=False
    ) as mock_get_plaintext:
        mock_get_plaintext.return_value = b"SECRETSQUIRREL"
        assert mock_secret_provider.decrypt_secret_raw("mysecret") == b"SECRETSQUIRREL"
        mock_get_plaintext.assert_called_with(
            client=mock_secret_provider.clients["devc"],
            path="/nail/blah/universe/secrets/mysecret.json",
            env="devc",
            cache_enabled=False,
            cache_key=None,
            cache_dir=None,
            context="universe",
        )


def test_get_secret_signature_from_data(mock_secret_provider):
    with mock.patch(
        "paasta_tools.secret_providers.vault.get_plaintext", autospec=False
    ):
        assert not mock_secret_provider.get_secret_signature_from_data(
            {"environments": {"devc": {}}}
        )
        assert (
            mock_secret_provider.get_secret_signature_from_data(
                {"environments": {"devc": {"signature": "abc"}}}
            )
            == "abc"
        )


def test_get_secret_signature_from_data_missing(mock_secret_provider):
    mock_secret_provider.cluster_names = ["mesosstage", "devc", "prod"]
    mock_secret_provider.vault_cluster_config = {
        "mesosstage": "devc",
        "devc": "devc",
        "prod": "prod",
    }
    with mock.patch(
        "paasta_tools.secret_providers.vault.get_plaintext", autospec=False
    ):
        # Should not raise errors
        assert not mock_secret_provider.get_secret_signature_from_data(
            {"environments": {"westeros": {}}}
        )


def test_renew_issue_cert(mock_secret_provider):
    with mock.patch(
        "paasta_tools.secret_providers.vault.do_renew", autospec=True
    ) as mock_do_renew:
        mock_secret_provider.renew_issue_cert("paasta", "30m")
        assert mock_do_renew.called
