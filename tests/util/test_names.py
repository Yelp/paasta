from pytest import raises

from paasta_tools.util import names


def test_compose_job_id_without_hashes():
    fake_service = "my_cool_service"
    fake_instance = "main"
    expected = "my_cool_service.main"
    actual = names.compose_job_id(fake_service, fake_instance)
    assert actual == expected


def test_compose_job_id_with_git_hash():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_git_hash = "git123abc"
    with raises(names.InvalidJobNameError):
        names.compose_job_id(fake_service, fake_instance, git_hash=fake_git_hash)


def test_compose_job_id_with_config_hash():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_config_hash = "config456def"
    with raises(names.InvalidJobNameError):
        names.compose_job_id(fake_service, fake_instance, config_hash=fake_config_hash)


def test_compose_job_id_with_hashes():
    fake_service = "my_cool_service"
    fake_instance = "main"
    fake_git_hash = "git123abc"
    fake_config_hash = "config456def"
    expected = "my_cool_service.main.git123abc.config456def"
    actual = names.compose_job_id(
        fake_service, fake_instance, fake_git_hash, fake_config_hash
    )
    assert actual == expected


def test_decompose_job_id_too_short():
    with raises(names.InvalidJobNameError):
        names.decompose_job_id("foo")


def test_decompose_job_id_without_hashes():
    fake_job_id = "my_cool_service.main"
    expected = ("my_cool_service", "main", None, None)
    actual = names.decompose_job_id(fake_job_id)
    assert actual == expected


def test_decompose_job_id_with_hashes():
    fake_job_id = "my_cool_service.main.git123abc.config456def"
    expected = ("my_cool_service", "main", "git123abc", "config456def")
    actual = names.decompose_job_id(fake_job_id)
    assert actual == expected


def test_long_job_id_to_short_job_id():
    assert (
        names.long_job_id_to_short_job_id("service.instance.git.config")
        == "service.instance"
    )
