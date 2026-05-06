from types import SimpleNamespace
from unittest.mock import patch

import pytest

from paasta_tools.cli.cmds.check_rollback_status import paasta_check_rollback_status
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.remote_git import LSRemoteException

FAKE_SHA = "abc123" * 7


@pytest.fixture
def fake_args():
    return SimpleNamespace(
        service="test_service",
        deploy_group="prod.main",
        commit=FAKE_SHA,
        soa_dir="/nail/etc/services",
    )


def test_no_rollback_tags(fake_args, capsys):
    with patch(
        "paasta_tools.cli.cmds.check_rollback_status.validate_service_name",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.get_git_url",
        autospec=True,
        return_value="git@git.yelpcorp.com:services/test_service",
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.list_remote_refs",
        autospec=True,
        return_value={
            "refs/tags/paasta-prod.main-20260420T120000-deploy": FAKE_SHA,
        },
    ):
        assert paasta_check_rollback_status(fake_args) == 0
        captured = capsys.readouterr()
        assert "OK" in captured.out


def test_rollback_tag_exists(fake_args, capsys):
    with patch(
        "paasta_tools.cli.cmds.check_rollback_status.validate_service_name",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.get_git_url",
        autospec=True,
        return_value="git@git.yelpcorp.com:services/test_service",
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.list_remote_refs",
        autospec=True,
        return_value={
            "refs/tags/paasta-prod.main-20260420T120000-deploy": "other_sha" * 5,
            "refs/tags/paasta-prod.main-20260420T130000-rollback": FAKE_SHA,
        },
    ):
        assert paasta_check_rollback_status(fake_args) == 1
        captured = capsys.readouterr()
        assert "ROLLED BACK" in captured.out
        assert "20260420T130000" in captured.out


def test_git_error(fake_args):
    with patch(
        "paasta_tools.cli.cmds.check_rollback_status.validate_service_name",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.get_git_url",
        autospec=True,
        return_value="git@git.yelpcorp.com:services/test_service",
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.list_remote_refs",
        autospec=True,
        side_effect=LSRemoteException("connection refused"),
    ):
        assert paasta_check_rollback_status(fake_args) == 2


def test_nonexistent_service(fake_args):
    fake_args.service = "nonexistent_service"

    with patch(
        "paasta_tools.cli.cmds.check_rollback_status.validate_service_name",
        autospec=True,
        side_effect=NoSuchService("nonexistent_service"),
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.get_git_url",
        autospec=True,
    ) as mock_get_git_url, patch(
        "paasta_tools.cli.cmds.check_rollback_status.list_remote_refs",
        autospec=True,
    ) as mock_list_remote_refs:
        assert paasta_check_rollback_status(fake_args) == 2
        assert not mock_get_git_url.called
        assert not mock_list_remote_refs.called


def test_ignores_rollback_tags_for_other_deploy_groups(fake_args):
    with patch(
        "paasta_tools.cli.cmds.check_rollback_status.validate_service_name",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.get_git_url",
        autospec=True,
        return_value="git@git.yelpcorp.com:services/test_service",
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.list_remote_refs",
        autospec=True,
        return_value={
            "refs/tags/paasta-staging.main-20260420T130000-rollback": FAKE_SHA,
        },
    ):
        assert paasta_check_rollback_status(fake_args) == 0


def test_image_version_in_tag(fake_args, capsys):
    with patch(
        "paasta_tools.cli.cmds.check_rollback_status.validate_service_name",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.get_git_url",
        autospec=True,
        return_value="git@git.yelpcorp.com:services/test_service",
    ), patch(
        "paasta_tools.cli.cmds.check_rollback_status.list_remote_refs",
        autospec=True,
        return_value={
            "refs/tags/paasta-prod.main+v1.2.3-20260420T130000-rollback": FAKE_SHA,
        },
    ):
        assert paasta_check_rollback_status(fake_args) == 1
        captured = capsys.readouterr()
        assert "ROLLED BACK" in captured.out
