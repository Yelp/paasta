import socket
from contextlib import contextmanager

import mock
import pytest

from paasta_tools import docker_wrapper
from paasta_tools import docker_wrapper_imports


@contextmanager
def patch_environ(envs):
    with mock.patch(
        "paasta_tools.docker_wrapper.os.environ", new=envs, autospec=None
    ) as environ:
        yield environ


class ImmutableDict(dict):
    def __setitem__(self, key, value):
        pass


class TestParseEnvArgs:
    @pytest.fixture(autouse=True)
    def mock_empty_os_environ(self):
        # Patch `os.environ` to contain a not changeable empty set of environment variables.
        with patch_environ(ImmutableDict()) as environ:
            yield environ

    def test_empty(self):
        env = docker_wrapper.parse_env_args(["docker"])
        assert env == {}

    @pytest.mark.parametrize(
        "args",
        [
            ["docker", "-e", "key=value"],
            ["docker", "-e=key=value"],
            ["docker", "-te", "key=value"],  # -e can be combined with other short args
            [
                "docker",
                "-et",
                "key=value",
            ],  # -t takes no additional parameters so docker allows this order
        ],
    )
    def test_short(self, args):
        env = docker_wrapper.parse_env_args(args)
        assert env == {"key": "value"}

    @pytest.mark.parametrize(
        "args,envs",
        [
            (["docker", "-e", "key"], {"key": "value"}),
            (["docker", "-e=key"], {"key": "value"}),
            (["docker", "-te", "key"], {"key": "value"}),
            (["docker", "-et", "key"], {"key": "value"}),
        ],
    )
    def test_short_with_envs(self, args, envs):
        with patch_environ(envs):
            env = docker_wrapper.parse_env_args(args)
            assert env == {"key": "value"}

    @pytest.mark.parametrize(
        "args", [["docker", "--env", "key=value"], ["docker", "--env=key=value"]]
    )
    def test_long(self, args):
        env = docker_wrapper.parse_env_args(args)
        assert env == {"key": "value"}

    @pytest.mark.parametrize(
        "args,envs",
        [
            (["docker", "--env", "key"], {"key": "value"}),
            (["docker", "--env=key=value"], {"key": "value"}),
        ],
    )
    def test_long_with_envs(self, args, envs):
        with patch_environ(envs):
            env = docker_wrapper.parse_env_args(args)
            assert env == {"key": "value"}

    @pytest.mark.parametrize(
        "args",
        [
            [
                "docker",
                "--eep",
                "key=value",
            ],  # ignore -e in double dashes unless its --env
            ["docker", "-a", "key=value"],  # only -e should matter
            ["docker", "-"],  # just don't crash
        ],
    )
    def test_short_invalid(self, args):
        env = docker_wrapper.parse_env_args(args)
        assert env == {}

    def test_mixed_short_long(self):
        env = docker_wrapper.parse_env_args(
            ["docker", "-e", "foo=bar", "--env=apple=banana", "--env", "c=d"]
        )
        assert env == {"foo": "bar", "apple": "banana", "c": "d"}

    def test_multiple_equals(self):
        env = docker_wrapper.parse_env_args(["docker", "-e", "foo=bar=cat"])
        assert env == {"foo": "bar=cat"}

    def test_dupe(self):
        env = docker_wrapper.parse_env_args(
            ["docker", "-e", "foo=bar", "-e", "foo=cat"]
        )
        assert env == {"foo": "cat"}

    def test_empty_value(self):
        env = docker_wrapper.parse_env_args(["docker", "-e", "foo=", "--env=bar="])
        assert env == {"foo": "", "bar": ""}

    def test_file_equals(self, mock_env_file):
        env = docker_wrapper.parse_env_args(["docker", f"--env-file={mock_env_file}"])
        assert env == {"fileKeyA": "fileValueA", "fileKeyB": "fileValueB"}

    def test_file(self, mock_env_file):
        env = docker_wrapper.parse_env_args(["docker", "--env-file", mock_env_file])
        assert env == {"fileKeyA": "fileValueA", "fileKeyB": "fileValueB"}

    def test_two_files(self, mock_env_file, mock_env_file2):
        env = docker_wrapper.parse_env_args(
            ["docker", "--env-file", mock_env_file, "--env-file", mock_env_file2]
        )
        assert env == {
            "fileKeyA": "fileValueA",
            "fileKeyB": "fileValueB",
            "fileKeyC": "fileValueC",
        }

    def test_file_and_short(self, mock_env_file):
        env = docker_wrapper.parse_env_args(
            ["docker", "--env-file", mock_env_file, "-e", "foo=bar"]
        )
        assert env == {"foo": "bar", "fileKeyA": "fileValueA", "fileKeyB": "fileValueB"}

    @pytest.fixture
    def mock_env_file(self, tmpdir):
        env_file = tmpdir.join("env.txt")
        env_file.write("fileKeyA=fileValueA\nfileKeyB=fileValueB\n")
        return str(env_file)

    @pytest.fixture
    def mock_env_file2(self, tmpdir):
        env_file = tmpdir.join("env2.txt")
        env_file.write("fileKeyC=fileValueC\n")
        return str(env_file)


class TestCanAddHostname:
    def test_empty(self):
        assert docker_wrapper.can_add_hostname(["docker"]) is True

    @pytest.mark.parametrize(
        "args", [["docker", "-h"], ["docker", "-th"], ["docker", "-ht"]]
    )
    def test_short(self, args):
        assert docker_wrapper.can_add_hostname(args) is False

    @pytest.mark.parametrize(
        "args",
        [
            ["docker", "-e=foo=hhh"],
            ["docker", "--hhh"],
            ["docker", "-"],
            ["docker", "--net", "bridged"],
            ["docker", "--net"],
            ["docker", "--network", "bridged"],
            ["docker", "--network"],
        ],
    )
    def test_short_invalid(self, args):
        assert docker_wrapper.can_add_hostname(args) is True

    @pytest.mark.parametrize(
        "args",
        [
            ["docker", "--hostname", "foo"],
            ["docker", "--hostname=foo"],
            ["docker", "--net=host"],
            ["docker", "--net", "host"],
            ["docker", "--network=host"],
            ["docker", "--network", "host"],
            ["docker", "--hostname=foo", "--net=host"],
        ],
    )
    def test_long(self, args):
        assert docker_wrapper.can_add_hostname(args) is False


class TestCanAddMacAddress:
    def test_empty(self):
        assert docker_wrapper.can_add_mac_address(["docker"]) is False

    def test_run(self):
        assert docker_wrapper.can_add_mac_address(["docker", "run"]) is True

    def test_not_run(self):
        assert docker_wrapper.can_add_mac_address(["docker", "inspect"]) is False

    @pytest.mark.parametrize(
        "args",
        [
            ["docker", "--mac-address", "foo"],
            ["docker", "--mac-address=foo"],
            ["docker", "--net=host"],
            ["docker", "--net", "host"],
            ["docker", "--network=host"],
            ["docker", "--network", "host"],
            ["docker", "--mac-address=foo", "--net=host"],
        ],
    )
    def test_long(self, args):
        assert docker_wrapper.can_add_mac_address(args) is False


class TestGenerateHostname:
    def test_simple(self):
        hostname = docker_wrapper.generate_hostname_task_id(
            "first", "what.only.matters.is.lastpart"
        )
        assert hostname == "first-lastpart"

    def test_truncate(self):
        hostname = docker_wrapper.generate_hostname_task_id(
            "reallllllllllllllylooooooooooooooong",
            "reallyreallylongidsssssssssssssssssssssssss",
        )
        assert (
            hostname == "reallllllllllllllylooooooooooooooong-reallyreallylongidsssss"
        )
        assert len(hostname) == 60

    def test_symbols(self):
        hostname = docker_wrapper.generate_hostname_task_id(
            "first", "anything:can_do!s0me weird-stuff"
        )
        assert hostname == "first-anything-can-do-s0me-weird-stuff"

    def test_no_dashes_on_end(self):
        assert (
            docker_wrapper.generate_hostname_task_id("beep", "foobar-") == "beep-foobar"
        )

        hostname = docker_wrapper.generate_hostname_task_id(
            "reallllllllllllllylooooooooooooooong", "reallyreallylongid0123--abc"
        )
        assert hostname == "reallllllllllllllylooooooooooooooong-reallyreallylongid0123"
        assert len(hostname) == 59


@pytest.mark.parametrize(
    "input_args,expected_args",
    [
        (["docker", "ps"], ["docker", "ps"]),  # do not add it for non-run commands
        (  # add it after the first run arg
            ["docker", "run", "run"],
            ["docker", "run", "--hostname=myhostname", "run"],
        ),
        (  # ignore args before run
            ["docker", "--host", "/fake.sock", "run", "-t"],
            ["docker", "--host", "/fake.sock", "run", "--hostname=myhostname", "-t"],
        ),
    ],
)
def test_add_argument(input_args, expected_args):
    args = docker_wrapper.add_argument(input_args, "--hostname=myhostname")
    assert args == expected_args


class TestMain:
    @pytest.fixture(autouse=True)
    def mock_execlp(self):
        # always patch execlp so we don't actually exec
        with mock.patch.object(
            docker_wrapper.os, "execlp", autospec=True
        ) as mock_execlp:
            yield mock_execlp

    def test_marathon(self, mock_execlp):
        argv = [
            "docker",
            "run",
            "--env=MESOS_TASK_ID=paasta--canary.main.git332d4a22.config458863b1.0126a188-f944-11e6-bdfb-12abac3adf8c",
        ]
        with mock.patch.object(socket, "getfqdn", return_value="myhostname"):
            docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [
            mock.call(
                "docker",
                "docker",
                "run",
                "--hostname=myhostname-0126a188-f944-11e6-bdfb-12abac3adf8c",
                "-e=PAASTA_HOST=myhostname",
                "--env=MESOS_TASK_ID=paasta--canary.main.git332d4a22.config458863b1.0126a188-f944-11e6-bdfb-12abac3adf8c",
            )
        ]

    def test_env_not_present(self, mock_execlp):
        argv = ["docker", "run", "foobar"]
        docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [
            mock.call(
                "docker",
                "docker",
                "run",
                f"--hostname={socket.gethostname()}",
                f"-e=PAASTA_HOST={socket.getfqdn()}",
                "foobar",
            )
        ]

    def test_already_has_hostname(self, mock_execlp):
        argv = [
            "docker",
            "run",
            "--env=MESOS_TASK_ID=my-mesos-task-id",
            "--hostname=somehostname",
        ]
        with mock.patch.object(socket, "getfqdn", return_value="myhostname"):
            docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [
            mock.call(
                "docker",
                "docker",
                "run",
                "--env=MESOS_TASK_ID=my-mesos-task-id",
                "--hostname=somehostname",
            )
        ]

    def test_not_run(self, mock_execlp):
        argv = ["docker", "ps", "--env=MESOS_TASK_ID=my-mesos-task-id"]
        docker_wrapper.main(argv)
        assert mock_execlp.mock_calls == [
            mock.call("docker", "docker", "ps", "--env=MESOS_TASK_ID=my-mesos-task-id")
        ]

    @pytest.fixture
    def mock_mac_address(self):
        with mock.patch.object(
            docker_wrapper_imports,
            "reserve_unique_mac_address",
            return_value=("00:00:00:00:00:00", None),
        ) as mock_mac_address:
            yield mock_mac_address

    @pytest.fixture
    def mock_firewall_env_args(self):
        return [
            "--env=PAASTA_FIREWALL=1",
            "--env=PAASTA_SERVICE=myservice",
            "--env=PAASTA_INSTANCE=myinstance",
        ]

    @mock.patch.object(docker_wrapper_imports, "firewall_flock", autospec=True)
    @mock.patch.object(docker_wrapper_imports, "prepare_new_container", autospec=True)
    def test_mac_address(
        self,
        mock_prepare_new_container,
        mock_firewall_flock,
        mock_mac_address,
        mock_execlp,
        mock_firewall_env_args,
    ):
        argv = ["docker", "run"] + mock_firewall_env_args
        docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [
            mock.call(
                "docker",
                "docker",
                "run",
                "--mac-address=00:00:00:00:00:00",
                f"--hostname={socket.gethostname()}",
                f"-e=PAASTA_HOST={socket.getfqdn()}",
                *mock_firewall_env_args,
            )
        ]

        assert mock_firewall_flock.return_value.__enter__.called is True

        assert mock_prepare_new_container.mock_calls == [
            mock.call(
                docker_wrapper_imports.DEFAULT_SOA_DIR,
                docker_wrapper_imports.DEFAULT_SYNAPSE_SERVICE_DIR,
                "myservice",
                "myinstance",
                "00:00:00:00:00:00",
            )
        ]

    def test_mac_address_not_run(
        self, mock_mac_address, mock_execlp, mock_firewall_env_args
    ):
        argv = ["docker", "inspect"] + mock_firewall_env_args
        docker_wrapper.main(argv)

        assert mock_mac_address.call_count == 0
        assert mock_execlp.mock_calls == [
            mock.call("docker", "docker", "inspect", *mock_firewall_env_args)
        ]

    def test_mac_address_already_set(
        self, mock_mac_address, mock_execlp, mock_firewall_env_args
    ):
        argv = [
            "docker",
            "run",
            "--mac-address=12:34:56:78:90:ab",
        ] + mock_firewall_env_args
        docker_wrapper.main(argv)

        assert mock_mac_address.call_count == 0
        assert mock_execlp.mock_calls == [
            mock.call(
                "docker",
                "docker",
                "run",
                f"--hostname={socket.gethostname()}",
                f"-e=PAASTA_HOST={socket.getfqdn()}",
                "--mac-address=12:34:56:78:90:ab",
                *mock_firewall_env_args,
            )
        ]

    def test_mac_address_no_lockdir(
        self, capsys, mock_execlp, tmpdir, mock_firewall_env_args
    ):
        nonexistent = tmpdir.join("nonexistent")
        with mock.patch.object(docker_wrapper, "LOCK_DIRECTORY", str(nonexistent)):
            argv = ["docker", "run"] + mock_firewall_env_args
            docker_wrapper.main(argv)

            assert mock_execlp.mock_calls == [
                mock.call(
                    "docker",
                    "docker",
                    "run",
                    f"--hostname={socket.gethostname()}",
                    f"-e=PAASTA_HOST={socket.getfqdn()}",
                    *mock_firewall_env_args,
                )
            ]
            _, err = capsys.readouterr()
            assert err.startswith(
                "Unable to add mac address: [Errno 2] No such file or directory"
            )

    @mock.patch.object(
        docker_wrapper_imports,
        "firewall_flock",
        autospec=True,
        side_effect=Exception("Oh noes"),
    )
    @mock.patch.object(docker_wrapper_imports, "prepare_new_container", autospec=True)
    def test_prepare_new_container_error(
        self,
        mock_prepare_new_container,
        mock_firewall_flock,
        capsys,
        mock_mac_address,
        mock_execlp,
        mock_firewall_env_args,
    ):
        argv = ["docker", "run"] + mock_firewall_env_args
        docker_wrapper.main(argv)

        assert mock_execlp.mock_calls == [
            mock.call(
                "docker",
                "docker",
                "run",
                "--mac-address=00:00:00:00:00:00",
                f"--hostname={socket.gethostname()}",
                f"-e=PAASTA_HOST={socket.getfqdn()}",
                *mock_firewall_env_args,
            )
        ]
        _, err = capsys.readouterr()
        assert err.startswith("Unable to add firewall rules: Oh noes")
