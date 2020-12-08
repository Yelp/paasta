# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import json
import os
import stat
import sys
import warnings

import mock
import pytest
from pytest import raises

from paasta_tools import utils


def test_get_git_url_provided_by_serviceyaml():
    service = "giiiiiiiiiiit"
    expected = "git@some_random_host:foobar"
    with (
        mock.patch(
            "service_configuration_lib.read_service_configuration", autospec=True
        )
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {"git_url": expected}
        assert utils.get_git_url(service) == expected
        mock_read_service_configuration.assert_called_once_with(
            service, soa_dir=utils.DEFAULT_SOA_DIR
        )


def test_get_git_url_default():
    service = "giiiiiiiiiiit"
    expected = "git@github.yelpcorp.com:services/%s" % service
    with (
        mock.patch(
            "service_configuration_lib.read_service_configuration", autospec=True
        )
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {}
        assert utils.get_git_url(service) == expected
        mock_read_service_configuration.assert_called_once_with(
            service, soa_dir=utils.DEFAULT_SOA_DIR
        )


def test_format_log_line():
    input_line = "foo"
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_component = "build"
    fake_level = "debug"
    fake_now = "fake_now"
    expected = json.dumps(
        {
            "timestamp": fake_now,
            "level": fake_level,
            "cluster": fake_cluster,
            "service": fake_service,
            "instance": fake_instance,
            "component": fake_component,
            "message": input_line,
        },
        sort_keys=True,
    )
    with mock.patch("paasta_tools.utils._now", autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_log_line(
            level=fake_level,
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            component=fake_component,
            line=input_line,
        )
        assert actual == expected


def test_format_log_line_with_timestamp():
    input_line = "foo"
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_component = "build"
    fake_level = "debug"
    fake_timestamp = "fake_timestamp"
    expected = json.dumps(
        {
            "timestamp": fake_timestamp,
            "level": fake_level,
            "cluster": fake_cluster,
            "service": fake_service,
            "instance": fake_instance,
            "component": fake_component,
            "message": input_line,
        },
        sort_keys=True,
    )
    actual = utils.format_log_line(
        fake_level,
        fake_cluster,
        fake_service,
        fake_instance,
        fake_component,
        input_line,
        timestamp=fake_timestamp,
    )
    assert actual == expected


def test_format_log_line_rejects_invalid_components():
    with raises(utils.NoSuchLogComponent):
        utils.format_log_line(
            level="debug",
            cluster="fake_cluster",
            service="fake_service",
            instance="fake_instance",
            line="fake_line",
            component="BOGUS_COMPONENT",
        )


def test_format_audit_log_line_no_details():
    fake_user = "fake_user"
    fake_hostname = "fake_hostname"
    fake_action = "mark-for-deployment"
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_now = "fake_now"

    expected_dict = {
        "timestamp": fake_now,
        "cluster": fake_cluster,
        "service": fake_service,
        "instance": fake_instance,
        "user": fake_user,
        "host": fake_hostname,
        "action": fake_action,
        "action_details": {},
    }

    expected = json.dumps(expected_dict, sort_keys=True)

    with mock.patch("paasta_tools.utils._now", autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_audit_log_line(
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            user=fake_user,
            host=fake_hostname,
            action=fake_action,
        )
        assert actual == expected


def test_format_audit_log_line_with_details():
    fake_user = "fake_user"
    fake_hostname = "fake_hostname"
    fake_action = "mark-for-deployment"
    fake_action_details = {"sha": "deadbeef"}
    fake_cluster = "fake_cluster"
    fake_service = "fake_service"
    fake_instance = "fake_instance"
    fake_now = "fake_now"

    expected_dict = {
        "timestamp": fake_now,
        "cluster": fake_cluster,
        "service": fake_service,
        "instance": fake_instance,
        "user": fake_user,
        "host": fake_hostname,
        "action": fake_action,
        "action_details": fake_action_details,
    }

    expected = json.dumps(expected_dict, sort_keys=True)

    with mock.patch("paasta_tools.utils._now", autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = utils.format_audit_log_line(
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            user=fake_user,
            host=fake_hostname,
            action=fake_action,
            action_details=fake_action_details,
        )
        assert actual == expected


try:
    from utils import ScribeLogWriter

    def test_ScribeLogWriter_log_raise_on_unknown_level():
        with raises(utils.NoSuchLogLevel):
            ScribeLogWriter().log("fake_service", "fake_line", "build", "BOGUS_LEVEL")

    def test_ScribeLogWriter_logs_audit_messages():
        slw = ScribeLogWriter(scribe_disable=True)
        mock_clog = mock.Mock()
        slw.clog = mock_clog

        user = "fake_user"
        host = "fake_hostname"
        action = "mark-for-deployment"
        action_details = {"sha": "deadbeef"}
        service = "fake_service"
        cluster = "fake_cluster"
        instance = "fake_instance"

        expected_log_name = utils.AUDIT_LOG_STREAM
        expected_line = utils.format_audit_log_line(
            user=user,
            host=host,
            action=action,
            action_details=action_details,
            service=service,
            cluster=cluster,
            instance=instance,
        )

        slw.log_audit(
            user=user,
            host=host,
            action=action,
            action_details=action_details,
            service=service,
            cluster=cluster,
            instance=instance,
        )

        assert mock_clog.log_line.call_count == 1
        assert mock_clog.log_line.called_once_with(expected_log_name, expected_line)


except ImportError:
    warnings.warn("ScribeLogWriter is unavailable")


def test_get_log_name_for_service():
    service = "foo"
    expected = "stream_paasta_%s" % service
    assert utils.get_log_name_for_service(service) == expected


@pytest.yield_fixture
def umask_022():
    old_umask = os.umask(0o022)
    yield
    os.umask(old_umask)


def test_atomic_file_write_itest(umask_022, tmpdir):
    target_file_name = tmpdir.join("test_atomic_file_write_itest.txt").strpath

    with open(target_file_name, "w") as f_before:
        f_before.write("old content")

    with utils.atomic_file_write(target_file_name) as f_new:
        f_new.write("new content")

        with open(target_file_name) as f_existing:
            # While in the middle of an atomic_file_write, the existing
            # file should still contain the old content, and should not
            # be truncated, etc.
            assert f_existing.read() == "old content"

    with open(target_file_name) as f_done:
        # once we're done, the content should be in place.
        assert f_done.read() == "new content"

    file_stat = os.stat(target_file_name)
    assert stat.S_ISREG(file_stat.st_mode)
    assert stat.S_IMODE(file_stat.st_mode) == 0o0644


def test_configure_log():
    fake_log_writer_config = {"driver": "fake", "options": {"fake_arg": "something"}}
    with mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config().get_log_writer.return_value = (
            fake_log_writer_config
        )
        with mock.patch(
            "paasta_tools.utils.get_log_writer_class", autospec=True
        ) as mock_get_log_writer_class:
            utils.configure_log()
            mock_get_log_writer_class.assert_called_once_with("fake")
            mock_get_log_writer_class("fake").assert_called_once_with(
                fake_arg="something"
            )


def test_build_docker_image_name():
    registry_url = "fake_registry"
    upstream_job_name = "a_really_neat_service"
    expected = f"{registry_url}/services-{upstream_job_name}"
    with mock.patch(
        "paasta_tools.util.config_loading.get_service_docker_registry",
        autospec=True,
        return_value=registry_url,
    ):
        actual = utils.build_docker_image_name(upstream_job_name)
    assert actual == expected


@mock.patch("paasta_tools.util.config_loading.build_docker_image_name", autospec=True)
def test_build_docker_tag(mock_build_docker_image_name):
    upstream_job_name = "foo"
    upstream_git_commit = "bar"
    mock_build_docker_image_name.return_value = "fake-registry/services-foo"
    expected = f"fake-registry/services-foo:paasta-{upstream_git_commit}"
    actual = utils.build_docker_tag(upstream_job_name, upstream_git_commit)
    assert actual == expected


@mock.patch("paasta_tools.util.config_loading.load_system_paasta_config", autospec=True)
@mock.patch("paasta_tools.util.config_loading.build_docker_image_name", autospec=True)
def test_check_docker_image_false(mock_build_docker_image_name, _):
    mock_build_docker_image_name.return_value = "fake-registry/services-foo"
    fake_app = "fake_app"
    fake_commit = "fake_commit"
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch(
        "paasta_tools.utils.get_docker_client", autospec=True
    ) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.images.return_value = [
            {
                "Created": 1425430339,
                "VirtualSize": 250344331,
                "ParentId": "1111",
                "RepoTags": [docker_tag],
                "Id": "ef978820f195dede62e206bbd41568463ab2b79260bc63835a72154fe7e196a2",
                "Size": 0,
            }
        ]
        assert utils.check_docker_image("test_service", "tag2") is False


@mock.patch("paasta_tools.util.config_loading.load_system_paasta_config", autospec=True)
@mock.patch("paasta_tools.util.config_loading.build_docker_image_name", autospec=True)
def test_check_docker_image_true(mock_build_docker_image_name, _):
    fake_app = "fake_app"
    fake_commit = "fake_commit"
    mock_build_docker_image_name.return_value = "fake-registry/services-foo"
    docker_tag = utils.build_docker_tag(fake_app, fake_commit)
    with mock.patch(
        "paasta_tools.utils.get_docker_client", autospec=True
    ) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.images.return_value = [
            {
                "Created": 1425430339,
                "VirtualSize": 250344331,
                "ParentId": "1111",
                "RepoTags": [docker_tag],
                "Id": "ef978820f195dede62e206bbd41568463ab2b79260bc63835a72154fe7e196a2",
                "Size": 0,
            }
        ]
        assert utils.check_docker_image(fake_app, fake_commit) is True


def test_remove_ansi_escape_sequences():
    plain_string = "blackandwhite"
    colored_string = "\033[34m" + plain_string + "\033[0m"
    assert utils.remove_ansi_escape_sequences(colored_string) == plain_string


def test_color_text():
    expected = f"{utils.PaastaColors.RED}hi{utils.PaastaColors.DEFAULT}"
    actual = utils.PaastaColors.color_text(utils.PaastaColors.RED, "hi")
    assert actual == expected


def test_color_text_nested():
    expected = "{}red{}blue{}red{}".format(
        utils.PaastaColors.RED,
        utils.PaastaColors.BLUE,
        utils.PaastaColors.DEFAULT + utils.PaastaColors.RED,
        utils.PaastaColors.DEFAULT,
    )
    actual = utils.PaastaColors.color_text(
        utils.PaastaColors.RED, "red%sred" % utils.PaastaColors.blue("blue")
    )
    assert actual == expected


def test_get_running_mesos_docker_containers():

    fake_container_data = [
        {
            "Status": "Up 2 hours",
            "Names": ["/mesos-legit.e1ad42eb-3ed7-4c9b-8711-aff017ef55a5"],
            "Id": "05698f4156c4f30c8dcd747f7724b14c9af7771c9a4b96fdd6aa37d6419a12a3",
        },
        {
            "Status": "Up 3 days",
            "Names": [
                "/definitely_not_meeeeesos-.6d2fb3aa-2fef-4f98-8fed-df291481e91f"
            ],
            "Id": "ae66e2c3fe3c4b2a7444212592afea5cc6a4d8ca70ee595036b19949e00a257c",
        },
    ]

    with mock.patch(
        "paasta_tools.utils.get_docker_client", autospec=True
    ) as mock_docker:
        docker_client = mock_docker.return_value
        docker_client.containers.return_value = fake_container_data
        assert len(utils.get_running_mesos_docker_containers()) == 1


def test_run_cancels_timer_thread_on_keyboard_interrupt():
    mock_process = mock.Mock()
    mock_timer_object = mock.Mock()
    with mock.patch(
        "paasta_tools.utils.Popen", autospec=True, return_value=mock_process
    ), mock.patch(
        "paasta_tools.utils.threading.Timer",
        autospec=True,
        return_value=mock_timer_object,
    ):
        mock_process.stdout.readline.side_effect = KeyboardInterrupt
        with raises(KeyboardInterrupt):
            utils._run("sh echo foo", timeout=10)
        assert mock_timer_object.cancel.call_count == 1


def test_run_returns_when_popen_fails():
    fake_exception = OSError(1234, "fake error")
    with mock.patch(
        "paasta_tools.utils.Popen", autospec=True, side_effect=fake_exception
    ):
        return_code, output = utils._run("nonexistant command", timeout=10)
    assert return_code == 1234
    assert "fake error" in output


def test_is_under_replicated_ok():
    num_available = 1
    expected_count = 1
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_zero():
    num_available = 1
    expected_count = 0
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (False, float(100))


def test_is_under_replicated_critical():
    num_available = 0
    expected_count = 1
    crit_threshold = 50
    actual = utils.is_under_replicated(num_available, expected_count, crit_threshold)
    assert actual == (True, float(0))


def test_terminal_len():
    assert len("some text") == utils.terminal_len(utils.PaastaColors.red("some text"))


def test_format_table():
    actual = utils.format_table(
        [["looooong", "y", "z"], ["a", "looooong", "c"], ["j", "k", "looooong"]]
    )
    expected = [
        "looooong  y         z",
        "a         looooong  c",
        "j         k         looooong",
    ]
    assert actual == expected
    assert ["a     b     c"] == utils.format_table([["a", "b", "c"]], min_spacing=5)


def test_format_table_with_interjected_lines():
    actual = utils.format_table(
        [
            ["looooong", "y", "z"],
            "interjection",
            ["a", "looooong", "c"],
            "unicode interjection",
            ["j", "k", "looooong"],
        ]
    )
    expected = [
        "looooong  y         z",
        "interjection",
        "a         looooong  c",
        "unicode interjection",
        "j         k         looooong",
    ]
    assert actual == expected


def test_format_table_all_strings():
    actual = utils.format_table(["foo", "bar", "baz"])
    expected = ["foo", "bar", "baz"]
    assert actual == expected


def test_parse_timestamp():
    actual = utils.parse_timestamp("19700101T000000")
    expected = datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0)
    assert actual == expected


def test_null_log_writer():
    """Basic smoke test for NullLogWriter"""
    lw = utils.NullLogWriter(driver="null")
    lw.log("fake_service", "fake_line", "build", "BOGUS_LEVEL")
    lw.log_audit("fake_user", "fake_hostname", "fake_action", service="fake_service")


class TestFileLogWriter:
    def test_smoke(self):
        """Smoke test for FileLogWriter"""
        fw = utils.FileLogWriter("/dev/null")
        fw.log("fake_service", "fake_line", "build", "BOGUS_LEVEL")
        fw.log_audit(
            "fake_user", "fake_hostname", "fake_action", service="fake_service"
        )

    def test_format_path(self):
        """Test the path formatting for FileLogWriter"""
        fw = utils.FileLogWriter(
            "/logs/{service}/{component}/{level}/{cluster}/{instance}"
        )
        expected = "/logs/a/b/c/d/e"
        assert expected == fw.format_path("a", "b", "c", "d", "e")

    def test_maybe_flock(self):
        """Make sure we flock and unflock when flock=True"""
        with mock.patch("paasta_tools.util.lock.fcntl", autospec=True) as mock_fcntl:
            fw = utils.FileLogWriter("/dev/null", flock=True)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                mock_fcntl.flock.assert_called_once_with(
                    mock_file.fileno(), mock_fcntl.LOCK_EX
                )
                mock_fcntl.flock.reset_mock()

            mock_fcntl.flock.assert_called_once_with(
                mock_file.fileno(), mock_fcntl.LOCK_UN
            )

    def test_maybe_flock_flock_false(self):
        """Make sure we don't flock/unflock when flock=False"""
        with mock.patch("paasta_tools.util.lock.fcntl", autospec=True) as mock_fcntl:
            fw = utils.FileLogWriter("/dev/null", flock=False)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                assert mock_fcntl.flock.call_count == 0

            assert mock_fcntl.flock.call_count == 0

    def test_log_makes_exactly_one_write_call(self):
        """We want to make sure that log() makes exactly one call to write, since that's how we ensure atomicity."""
        fake_file = mock.Mock()
        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file, __exit__=lambda _self, t, v, tb: None
        )

        fake_line = "text" * 1000000

        with mock.patch(
            "paasta_tools.utils.io.FileIO", return_value=fake_contextmgr, autospec=True
        ) as mock_FileIO:
            fw = utils.FileLogWriter("/dev/null", flock=False)

            with mock.patch(
                "paasta_tools.utils.format_log_line",
                return_value=fake_line,
                autospec=True,
            ) as fake_fll:
                fw.log(
                    "service",
                    "line",
                    "component",
                    level="level",
                    cluster="cluster",
                    instance="instance",
                )

            fake_fll.assert_called_once_with(
                "level", "cluster", "service", "instance", "component", "line"
            )

            mock_FileIO.assert_called_once_with("/dev/null", mode=fw.mode, closefd=True)
            fake_file.write.assert_called_once_with(f"{fake_line}\n".encode("UTF-8"))

    def test_write_raises_IOError(self):
        fake_file = mock.Mock()
        fake_file.write.side_effect = IOError("hurp durp")

        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file, __exit__=lambda _self, t, v, tb: None
        )

        fake_line = "line"

        with mock.patch(
            "paasta_tools.utils.io.FileIO", return_value=fake_contextmgr, autospec=True
        ), mock.patch("builtins.print", autospec=True) as mock_print, mock.patch(
            "paasta_tools.utils.format_log_line", return_value=fake_line, autospec=True
        ):
            fw = utils.FileLogWriter("/dev/null", flock=False)
            fw.log(
                service="service",
                line="line",
                component="build",
                level="level",
                cluster="cluster",
                instance="instance",
            )

        mock_print.assert_called_once_with(mock.ANY, file=sys.stderr)

        # On python3, they merged IOError and OSError. Once paasta is fully py3, replace mock.ANY above with the OSError
        # message below.
        assert mock_print.call_args[0][0] in {
            "Could not log to /dev/null: IOError: hurp durp -- would have logged: line\n",
            "Could not log to /dev/null: OSError: hurp durp -- would have logged: line\n",
        }


def test_function_composition():
    def func_one(count):
        return count + 1

    def func_two(count):
        return count + 1

    composed_func = utils.compose(func_one, func_two)
    assert composed_func(0) == 2


def test_mean():
    iterable = [1.0, 2.0, 3.0]
    assert utils.mean(iterable) == 2.0


def test_prompt_pick_one_happy():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(return_value="choiceA"))
        assert utils.prompt_pick_one(["choiceA"], "test") == "choiceA"


def test_prompt_pick_one_quit():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(return_value=(None, "quit")))
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_keyboard_interrupt():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(side_effect=KeyboardInterrupt))
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_eoferror():
    with mock.patch(
        "paasta_tools.utils.sys.stdin", autospec=True
    ) as mock_stdin, mock.patch(
        "paasta_tools.utils.choice.Menu", autospec=True
    ) as mock_menu:
        mock_stdin.isatty.return_value = True
        mock_menu.return_value = mock.Mock(ask=mock.Mock(side_effect=EOFError))
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_exits_no_tty():
    with mock.patch("paasta_tools.utils.sys.stdin", autospec=True) as mock_stdin:
        mock_stdin.isatty.return_value = False
        with raises(SystemExit):
            utils.prompt_pick_one(["choiceA", "choiceB"], "test")


def test_prompt_pick_one_exits_no_choices():
    with mock.patch("paasta_tools.utils.sys.stdin", autospec=True) as mock_stdin:
        mock_stdin.isatty.return_value = True
        with raises(SystemExit):
            utils.prompt_pick_one([], "test")
