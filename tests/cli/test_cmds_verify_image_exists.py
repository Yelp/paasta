from unittest import mock

from paasta_tools.cli.cmds.verify_image_exists import verify_image_exists


def test_image_found():
    with mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.is_docker_image_already_in_registry",
        autospec=True,
        return_value=True,
    ):
        assert verify_image_exists(service="fake_service", commit="abc1234") == 0


def test_image_not_found_no_wait():
    with mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.is_docker_image_already_in_registry",
        autospec=True,
        return_value=False,
    ):
        assert verify_image_exists(service="fake_service", commit="abc1234") == 1


def test_wait_polls_until_found():
    with mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.is_docker_image_already_in_registry",
        autospec=True,
        side_effect=[False, False, True],
    ), mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.time.sleep",
        autospec=True,
    ):
        assert (
            verify_image_exists(service="fake_service", commit="abc1234", wait=True)
            == 0
        )


def test_timeout_expires():
    with mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.is_docker_image_already_in_registry",
        autospec=True,
        return_value=False,
    ), mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.time.sleep",
        autospec=True,
    ), mock.patch(
        "paasta_tools.cli.cmds.verify_image_exists.time.time",
        autospec=True,
        # slightly brittle, but oh well.
        # first call is for setting the start_time, second one
        # is after the mocked sleep and is past the timeout
        side_effect=[0.0, 61.0],
    ):
        assert (
            verify_image_exists(
                service="fake_service", commit="abc1234", wait=True, timeout=60
            )
            == 1
        )
