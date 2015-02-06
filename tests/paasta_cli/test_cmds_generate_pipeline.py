from StringIO import StringIO

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
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline._run')
def test_generate_pipeline_run_fails(
        mock_run,
        mock_validate_service_name):
    # paasta generate fails on the _run call

    mock_validate_service_name.return_value = None
    mock_run.return_value = (1, 'Big bad wolf')
    args = MagicMock()
    args.service = 'Fake servicename'
    with raises(SystemExit) as sys_exit:
        paasta_generate_pipeline(args)
    assert sys_exit.value.code == 1


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline._run')
def test_generate_pipeline_success(
        mock_run,
        mock_guess_service_name,
        mock_validate_service_name):
    # paasta generate succeeds when service name must be guessed

    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    mock_run.return_value = (0, 'Everything OK')

    args = MagicMock()
    args.service = None
    assert paasta_generate_pipeline(args) is None


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.guess_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline._run')
def test_generate_pipeline_success_no_opts(
        mock_run,
        mock_guess_service_name,
        mock_validate_service_name):
    # paasta generate succeeds when service name must be guessed

    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    mock_run.return_value = (0, 'Everything OK')

    args = MagicMock()
    args.service = None
    assert paasta_generate_pipeline(args) is None


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.generate_pipeline._run')
def test_generate_pipeline_success_with_opts(
        mock_run,
        mock_validate_service_name):
    # paasta generate succeeds when service name provided as arg

    mock_validate_service_name.return_value = None
    mock_run.return_value = (0, 'Everything OK')

    args = MagicMock()
    args.service = 'fake_service'
    assert paasta_generate_pipeline(args) is None
