import mock

from paasta_tools.paasta_cli.cmds.cook_image import paasta_cook_image


@mock.patch('paasta_tools.paasta_cli.cmds.cook_image.validate_service_name', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.cook_image.makefile_responds_to', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.cook_image._run', autospec=True)
def test_run_success(
    mock_run,
    mock_makefile_responds_to,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Output')
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True

    args = mock.MagicMock()
    args.service = 'fake_service'
    assert paasta_cook_image(args) is None
