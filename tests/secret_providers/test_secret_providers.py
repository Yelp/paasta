from pytest import raises

from paasta_tools.secret_providers import SecretProvider


def test_secret_provider():
    SecretProvider(
        soa_dir="/nail/blah",
        service_name="universe",
        cluster_names=["mesosstage"],
        some="setting",
    )


def test_decrypt_environment():
    with raises(NotImplementedError):
        SecretProvider(
            soa_dir="/nail/blah", service_name="universe", cluster_names=["mesosstage"]
        ).decrypt_environment(environment={}, a="kwarg")


def test_write_secret():
    with raises(NotImplementedError):
        SecretProvider(
            soa_dir="/nail/blah", service_name="universe", cluster_names=["mesosstage"]
        ).write_secret(
            action="update", secret_name="whatididlastsummer", plaintext=b"noybw"
        )


def test_decrypt_secret():
    with raises(NotImplementedError):
        SecretProvider(
            soa_dir="/nail/blah", service_name="universe", cluster_names=["mesosstage"]
        ).decrypt_secret(secret_name="whatididlastsummer")
