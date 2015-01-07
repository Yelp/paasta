from StringIO import StringIO
from subprocess import CalledProcessError

from mock import MagicMock
from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.generate_pipeline \
    import paasta_generate_pipeline
from paasta_tools.paasta_cli.utils import NoSuchService


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_generate_pipeline_service_not_found(
        mock_stdout, mock_guess_service_name, mock_validate_service_name):
    # paasta generate cannot guess service name and none is provided

    mock_guess_service_name.return_value = 'not_a_service'
    mock_validate_service_name.side_effect = NoSuchService(None)

    args = MagicMock()
    args.service = None
    expected_output = "%s\n" % NoSuchService.GUESS_ERROR_MSG

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        paasta_generate_pipeline(args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.subprocess')
def test_generate_pipeline_subprocess1_fail_no_opt_args(
        mock_subprocess, mock_guess_service_name, mock_validate_service_name):
    # paasta generate fails on the first subprocess call
    # service name has to be guessed

    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    mock_subprocess.check_call.side_effect = [
        CalledProcessError(1, 'jenkins cmd 1'), 0]
    args = MagicMock()
    args.service = None
    with raises(CalledProcessError):
        paasta_generate_pipeline(args)


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.subprocess')
def test_generate_pipeline_subprocess2_fail_with_opt_args(
        mock_subprocess, mock_guess_service_name, mock_validate_service_name):
    # paasta generate fails on the second subprocess call
    # service name provided as arg

    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    mock_subprocess.check_call.side_effect = [
        0, CalledProcessError(1, 'jenkins cmd 2')]

    args = MagicMock()
    args.service = 'fake_service'
    with raises(CalledProcessError):
        paasta_generate_pipeline(args)


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.subprocess')
def test_generate_pipeline_success_no_opts(
        mock_subprocess, mock_guess_service_name, mock_validate_service_name):
    # paasta generate succeeds when service name must be guessed

    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    mock_subprocess.check_call.side_effect = [
        0, 0]

    args = MagicMock()
    args.service = None
    assert paasta_generate_pipeline(args) is None


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.subprocess')
def test_generate_pipeline_success_with_opts(
        mock_subprocess, mock_validate_service_name):
    # paasta generate succeeds when service name provided as arg

    mock_validate_service_name.return_value = None
    mock_subprocess.check_call.side_effect = [
        0, 0]

    args = MagicMock()
    args.service = 'fake_service'
    assert paasta_generate_pipeline(args) is None
