import sys
from StringIO import StringIO
from subprocess import CalledProcessError

from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.promote_to_registry import paasta_promote_to_registry
from paasta_tools.paasta_cli.paasta_cli import parse_args
from paasta_tools.paasta_cli.utils import NoSuchService


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_promote_to_registry_service_not_found(
    mock_stdout,
    mock_validate_service_name,
):
    mock_validate_service_name.side_effect = NoSuchService(None)

    sys.argv = ['./paasta_cli', 'promote-to-registry']
    parsed_args = parse_args()
    expected_output = "%s\n" % NoSuchService.GUESS_ERROR_MSG

    with raises(SystemExit) as sys_exit:
        paasta_promote_to_registry(parsed_args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.subprocess', autospec=True)
def test_promote_to_registry_subprocess_fail(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.side_effect = [
        CalledProcessError(1, 'jenkins cmd 1'), 0]
    sys.argv = [
        './paasta_cli', 'promote-to-registry', '--service', 'unused', '--sha', 'unused',
    ]
    parsed_args = parse_args()

    with raises(CalledProcessError):
        paasta_promote_to_registry(parsed_args)


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.subprocess', autospec=True)
def test_promote_to_registry_success(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'promote-to-registry', '--service', 'unused', '--sha', 'unused',
    ]
    parsed_args = parse_args()
    assert paasta_promote_to_registry(parsed_args) is None


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.subprocess', autospec=True)
def test_promote_to_registry_success_with_opts(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'promote-to-registry', '--service', 'fake_service', '--sha', 'deadbeef',
    ]
    parsed_args = parse_args()
    assert paasta_promote_to_registry(parsed_args) is None
