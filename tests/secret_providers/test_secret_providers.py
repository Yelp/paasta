from pytest import raises

from paasta_tools.secret_providers import SecretProvider


def test_secret_provider():
    SecretProvider(
        soa_dir='/nail/blah',
        service_name='universe',
        cluster_name='mesosstage',
    )


def test_decrypt_environment():
    with raises(NotImplementedError):
        SecretProvider(
            soa_dir='/nail/blah',
            service_name='universe',
            cluster_name='mesosstage',
        ).decrypt_environment(environment={}, a='kwarg')
