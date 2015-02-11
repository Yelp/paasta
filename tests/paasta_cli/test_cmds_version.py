import mock

from paasta_tools.paasta_cli.cmds import version


def test_paasta_version():
    args = mock.MagicMock()
    version.paasta_version(args)
