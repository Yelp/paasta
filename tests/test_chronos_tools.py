import mock
from pytest import raises

import contextlib

import chronos_tools


def test_chronos_config_object_normal():
    fake_json_contents = {
        'user': 'fake_user',
        'password': 'fake_password',
    }
    fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
    assert fake_config.get_username() == 'fake_user'
    assert fake_config.get_password() == 'fake_password'


def test_chronos_config_object_no_user():
    fake_json_contents = {
        'password': 'fake_password',
    }
    fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
    with raises(chronos_tools.ChronosNotConfigured):
        fake_config.get_username()


def test_chronos_config_object_no_password():
    fake_json_contents = {
        'user': 'fake_user',
    }
    fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
    with raises(chronos_tools.ChronosNotConfigured):
        fake_config.get_password()


def test_load_chronos_config_good():
    expected = {'foo': 'bar'}
    file_mock = mock.MagicMock(spec=file)
    with contextlib.nested(
        mock.patch('chronos_tools.open', create=True, return_value=file_mock),
        mock.patch('json.load', autospec=True, return_value=expected)
    ) as (
        open_file_patch,
        json_patch
    ):
        assert chronos_tools.load_chronos_config() == expected
        open_file_patch.assert_called_once_with('/etc/paasta/chronos.json')
        json_patch.assert_called_once_with(file_mock.__enter__())


def test_load_chronos_config_bad():
    fake_path = '/dne'
    with contextlib.nested(
        mock.patch('chronos_tools.open', create=True, side_effect=IOError(2, 'a', 'b')),
    ) as (
        open_patch,
    ):
        with raises(chronos_tools.ChronosNotConfigured) as excinfo:
            chronos_tools.load_chronos_config(fake_path)
        assert str(excinfo.value) == "Could not load chronos config file b: a"


def test_get_chronos_client():
    with contextlib.nested(
        mock.patch('chronos.connect', autospec=True),
    ) as (
        mock_connect,
    ):
        fake_config = chronos_tools.ChronosConfig({'user': 'test', 'password': 'pass'}, '/fake/path')
        chronos_tools.get_chronos_client(fake_config)
        assert mock_connect.call_count == 1


def test_get_job_id():
    actual = chronos_tools.get_job_id('service', 'instance')
    assert actual == "service instance"
