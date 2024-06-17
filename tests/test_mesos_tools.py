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
import random
import socket

import asynctest
import docker
import mock
import requests
from pytest import mark
from pytest import raises

from paasta_tools import mesos
from paasta_tools import mesos_tools
from paasta_tools import utils
from paasta_tools.utils import PaastaColors


def test_filter_running_tasks():
    tasks = [
        {"id": 1, "state": "TASK_RUNNING", "framework": {"active": True}},
        {"id": 2, "state": "TASK_FAILED", "framework": {"active": True}},
    ]
    running = mesos_tools.filter_running_tasks(tasks)
    assert len(running) == 1
    assert running[0]["id"] == 1


def test_filter_not_running_tasks():
    tasks = [{"id": 1, "state": "TASK_RUNNING"}, {"id": 2, "state": "TASK_FAILED"}]
    not_running = mesos_tools.filter_not_running_tasks(tasks)
    assert len(not_running) == 1
    assert not_running[0]["id"] == 2


@mark.parametrize(
    "test_case",
    [[0, 0], [10, 1 + 10]],  # 1 running task, 10 non-running taks (truncated)
)
def test_status_mesos_tasks_verbose(test_case):
    tail_lines, expected_format_tail_call_count = test_case
    filter_string = "fake--service.fake--instance"

    with asynctest.patch(
        "paasta_tools.mesos_tools.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
        return_value=[{"id": filter_string}],
    ), asynctest.patch(
        "paasta_tools.mesos_tools.get_cached_list_of_not_running_tasks_from_frameworks",
        autospec=True,
    ) as get_cached_list_of_not_running_tasks_from_frameworks_patch, asynctest.patch(
        "paasta_tools.mesos_tools.format_running_mesos_task_row", autospec=True
    ) as format_running_mesos_task_row_patch, asynctest.patch(
        "paasta_tools.mesos_tools.format_non_running_mesos_task_row", autospec=True
    ) as format_non_running_mesos_task_row_patch, asynctest.patch(
        "paasta_tools.mesos_tools.format_stdstreams_tail_for_task", autospec=True
    ) as format_stdstreams_tail_for_task_patch:

        template_task_return = {
            "id": filter_string,
            "statuses": [{"timestamp": "##########"}],
            "state": "NOT_RUNNING",
        }
        non_running_mesos_tasks = []
        for _ in range(
            15
        ):  # exercise the code that sorts/truncates the list of non running tasks
            task_return = template_task_return.copy()
            task_return["statuses"][0]["timestamp"] = str(
                1457109986 + random.randrange(-60 * 60 * 24, 60 * 60 * 24)
            )
            non_running_mesos_tasks.append(task_return)
        get_cached_list_of_not_running_tasks_from_frameworks_patch.return_value = (
            non_running_mesos_tasks
        )

        format_running_mesos_task_row_patch.return_value = [
            "id",
            "host",
            "mem",
            "cpu",
            "time",
        ]
        format_non_running_mesos_task_row_patch.return_value = [
            "id",
            "host",
            "time",
            "state",
        ]
        format_stdstreams_tail_for_task_patch.return_value = ["tail"]

        actual = mesos_tools.status_mesos_tasks_verbose(
            filter_string=filter_string,
            get_short_task_id=mock.sentinel.get_short_task_id,
            tail_lines=tail_lines,
        )
        assert "Running Tasks" in actual
        assert "Non-Running Tasks" in actual
        format_running_mesos_task_row_patch.assert_called_once_with(
            {"id": filter_string}, mock.sentinel.get_short_task_id
        )
        assert (
            format_non_running_mesos_task_row_patch.call_count == 10
        )  # maximum n of tasks we display
        assert (
            format_stdstreams_tail_for_task_patch.call_count
            == expected_format_tail_call_count
        )


@mark.asyncio
async def test_get_cpu_usage_good():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.cpu_limit = asynctest.CoroutineMock(return_value=0.35)
    fake_duration = 100
    fake_task.stats = asynctest.CoroutineMock(
        return_value={"cpus_system_time_secs": 2.5, "cpus_user_time_secs": 0.0}
    )
    current_time = datetime.datetime.now()
    fake_task.__getitem__.return_value = [
        {
            "state": "TASK_RUNNING",
            "timestamp": int(current_time.strftime("%s")) - fake_duration,
        }
    ]
    with asynctest.patch(
        "paasta_tools.mesos_tools.datetime.datetime", autospec=True
    ) as mock_datetime:
        mock_datetime.now.return_value = current_time
        actual = await mesos_tools.get_cpu_usage(fake_task)
    assert "10.0%" == actual


@mark.asyncio
async def test_get_cpu_usage_bad():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.cpu_limit = asynctest.CoroutineMock(return_value=1.1)
    fake_duration = 100
    fake_task.stats = asynctest.CoroutineMock(
        return_value={"cpus_system_time_secs": 50.0, "cpus_user_time_secs": 50.0}
    )
    current_time = datetime.datetime.now()
    fake_task.__getitem__.return_value = [
        {
            "state": "TASK_RUNNING",
            "timestamp": int(current_time.strftime("%s")) - fake_duration,
        }
    ]
    with asynctest.patch(
        "paasta_tools.mesos_tools.datetime.datetime", autospec=True
    ) as mock_datetime:
        mock_datetime.now.return_value = current_time
        actual = await mesos_tools.get_cpu_usage(fake_task)
    assert PaastaColors.red("100.0%") in actual


@mark.asyncio
async def test_get_cpu_usage_handles_missing_stats():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.cpu_limit = asynctest.CoroutineMock(return_value=1.1)
    fake_duration = 100
    fake_task.stats = asynctest.CoroutineMock(return_value={})
    fake_task.__getitem__.return_value = [
        {
            "state": "TASK_RUNNING",
            "timestamp": int(datetime.datetime.now().strftime("%s")) - fake_duration,
        }
    ]
    actual = await mesos_tools.get_cpu_usage(fake_task)
    assert "0.0%" in actual


@mark.asyncio
async def test_get_mem_usage_good():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.rss = asynctest.CoroutineMock(return_value=1024 * 1024 * 10)
    fake_task.mem_limit = asynctest.CoroutineMock(return_value=1024 * 1024 * 10 * 10)
    actual = await mesos_tools.get_mem_usage(fake_task)
    assert actual == "10/100MB"


@mark.asyncio
async def test_get_mem_usage_bad():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.rss = asynctest.CoroutineMock(return_value=1024 * 1024 * 100)
    fake_task.mem_limit = fake_task.rss
    actual = await mesos_tools.get_mem_usage(fake_task)
    assert actual == PaastaColors.red("100/100MB")


@mark.asyncio
async def test_get_mem_usage_divide_by_zero():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.rss = asynctest.CoroutineMock(return_value=1024 * 1024 * 10)
    fake_task.mem_limit = asynctest.CoroutineMock(return_value=0)
    actual = await mesos_tools.get_mem_usage(fake_task)
    assert actual == "Undef"


def test_get_zookeeper_config():
    zk_hosts = "1.1.1.1:1111,2.2.2.2:2222,3.3.3.3:3333"
    zk_path = "fake_path"
    fake_state = {"flags": {"zk": f"zk://{zk_hosts}/{zk_path}"}}
    expected = {"hosts": zk_hosts, "path": zk_path}
    assert mesos_tools.get_zookeeper_config(fake_state) == expected


def test_get_mesos_leader():
    fake_url = "http://93.184.216.34:5050"
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_master", autospec=True
    ) as mock_get_master, asynctest.patch(
        "paasta_tools.mesos_tools.socket.gethostbyaddr", autospec=True
    ) as mock_gethostbyaddr, asynctest.patch(
        "paasta_tools.mesos_tools.socket.getfqdn", autospec=True
    ) as mock_getfqdn:
        mock_master = mock.Mock()
        mock_master.host = fake_url
        mock_get_master.return_value = mock_master
        mock_gethostbyaddr.return_value = "example.org"
        mock_getfqdn.return_value = "example.org"
        assert mesos_tools.get_mesos_leader() == "example.org"


def test_get_mesos_leader_socket_error():
    fake_url = "http://93.184.216.34:5050"
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_master", autospec=True
    ) as mock_get_master, asynctest.patch(
        "paasta_tools.mesos_tools.socket.gethostbyaddr",
        side_effect=socket.error,
        autospec=True,
    ):
        mock_master = mock.Mock()
        mock_master.host = fake_url
        mock_get_master.return_value = mock_master
        with raises(socket.error):
            mesos_tools.get_mesos_leader()


def test_get_mesos_leader_no_hostname():
    fake_url = "localhost:5050"
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_master", autospec=True
    ) as mock_get_master:
        mock_master = mock.Mock()
        mock_master.host = fake_url
        mock_get_master.return_value = mock_master
        with raises(ValueError):
            mesos_tools.get_mesos_leader()


@mock.patch(
    "paasta_tools.mesos_tools.get_mesos_config",
    autospec=True,
    return_value={"scheme": "http", "master": "test"},
)
def test_get_mesos_leader_cli_mesosmasterconnectionerror(mock_get_mesos_config):
    with asynctest.patch(
        "paasta_tools.mesos.master.MesosMaster.resolve",
        side_effect=mesos.exceptions.MasterNotAvailableException,
        autospec=True,
    ):
        with raises(mesos.exceptions.MasterNotAvailableException):
            mesos_tools.get_mesos_leader()


@mock.patch("paasta_tools.mesos_tools.KazooClient", autospec=True)
def test_get_number_of_mesos_masters(
    mock_kazoo,
):
    host = "1.1.1.1"
    path = "fake_path"

    zk = mock_kazoo.return_value
    zk.get_children.return_value = ["log_11", "state", "json.info_1", "info_2"]
    assert mesos_tools.get_number_of_mesos_masters(host, path) == 2


@mock.patch("requests.get", autospec=True)
@mock.patch("socket.getfqdn", autospec=True)
def test_get_local_slave_state_connection_error(mock_getfqdn, mock_requests_get):
    fake_request = requests.Request("GET", url="doesnt_matter")
    mock_getfqdn.return_value = "fake_hostname"
    mock_requests_get.side_effect = requests.ConnectionError(
        "fake_message", request=fake_request
    )

    with raises(mesos_tools.MesosSlaveConnectionError):
        mesos_tools.get_local_slave_state()


def test_get_mesos_slaves_grouped_by_attribute():
    fake_value_1 = "fake_value_1"
    fake_value_2 = "fake_value_2"
    fake_slaves = [
        {"hostname": "fake_host_1", "attributes": {"fake_attribute": fake_value_1}},
        {"hostname": "fake_host_2", "attributes": {"fake_attribute": fake_value_2}},
        {"hostname": "fake_host_3", "attributes": {"fake_attribute": fake_value_1}},
        {
            "hostname": "fake_host_4",
            "attributes": {"fake_attribute": "fake_other_value"},
        },
    ]
    expected = {
        "fake_value_1": [
            {"hostname": "fake_host_1", "attributes": {"fake_attribute": fake_value_1}},
            {"hostname": "fake_host_3", "attributes": {"fake_attribute": fake_value_1}},
        ],
        "fake_value_2": [
            {"hostname": "fake_host_2", "attributes": {"fake_attribute": fake_value_2}}
        ],
        "fake_other_value": [
            {
                "hostname": "fake_host_4",
                "attributes": {"fake_attribute": "fake_other_value"},
            }
        ],
    }
    actual = mesos_tools.get_mesos_slaves_grouped_by_attribute(
        fake_slaves, "fake_attribute"
    )
    assert actual == expected


@mock.patch("paasta_tools.mesos_tools.host_passes_blacklist", autospec=True)
def test_filter_mesos_slaves_by_blacklist_when_unfiltered(mock_slave_passes_blacklist):
    mock_slave_passes_blacklist.return_value = True
    slaves = [
        {"hostname": "fake_host_1", "attributes": {"fake_attribute": "fake_value_1"}},
        {"hostname": "fake_host_2", "attributes": {"fake_attribute": "fake_value_1"}},
    ]
    blacklist = []
    whitelist = None
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(
        slaves=slaves, blacklist=blacklist, whitelist=whitelist
    )
    assert mock_slave_passes_blacklist.call_count == 2
    assert actual == slaves


@mock.patch("paasta_tools.mesos_tools.host_passes_blacklist", autospec=True)
def test_filter_mesos_slaves_by_blacklist_when_filtered(mock_slave_passes_blacklist):
    mock_slave_passes_blacklist.return_value = False
    slaves = [
        {"hostname": "fake_host_1", "attributes": {"fake_attribute": "fake_value_1"}},
        {"hostname": "fake_host_2", "attributes": {"fake_attribute": "fake_value_1"}},
    ]
    blacklist = []
    whitelist = None
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(
        slaves=slaves, blacklist=blacklist, whitelist=whitelist
    )
    assert mock_slave_passes_blacklist.call_count == 2
    assert actual == []


def test_get_paasta_execute_docker_healthcheck():
    mock_docker_client = mock.MagicMock(spec_set=docker.APIClient)
    fake_container_id = "fake_container_id"
    fake_mesos_id = "fake_mesos_id"
    fake_container_info = [
        {"Config": {"Env": None}},
        {
            "Config": {
                "Env": ["fake_key1=fake_value1", "MESOS_TASK_ID=fake_other_mesos_id"]
            },
            "Id": "11111",
        },
        {
            "Config": {
                "Env": ["fake_key2=fake_value2", "MESOS_TASK_ID=%s" % fake_mesos_id]
            },
            "Id": fake_container_id,
        },
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.APIClient,
        return_value=["fake_container_1", "fake_container_2", "fake_container_3"],
    )
    mock_docker_client.inspect_container = mock.MagicMock(
        spec_set=docker.APIClient, side_effect=fake_container_info
    )
    assert (
        mesos_tools.get_container_id_for_mesos_id(mock_docker_client, fake_mesos_id)
        == fake_container_id
    )


def test_get_paasta_execute_docker_healthcheck_when_not_found():
    mock_docker_client = mock.MagicMock(spec_set=docker.APIClient)
    fake_mesos_id = "fake_mesos_id"
    fake_container_info = [
        {
            "Config": {
                "Env": ["fake_key1=fake_value1", "MESOS_TASK_ID=fake_other_mesos_id"]
            },
            "Id": "11111",
        },
        {
            "Config": {
                "Env": ["fake_key2=fake_value2", "MESOS_TASK_ID=fake_other_mesos_id2"]
            },
            "Id": "2222",
        },
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.APIClient, return_value=["fake_container_1", "fake_container_2"]
    )
    mock_docker_client.inspect_container = mock.MagicMock(
        spec_set=docker.APIClient, side_effect=fake_container_info
    )
    assert (
        mesos_tools.get_container_id_for_mesos_id(mock_docker_client, fake_mesos_id)
        is None
    )


@mark.parametrize(
    "test_case",
    [
        [
            ["taska", "taskb"],  # test_case0 - OK
            [["outlna1", "outlna2", "errlna1"], ["outlnb1", "errlnb1", "errlnb2"]],
            [
                "taska",
                "outlna1",
                "outlna2",
                "errlna1",
                "taskb",
                "outlnb1",
                "errlnb1",
                "errlnb2",
            ],
            False,
        ],
        [["a"], [1, 2], None, True],  # test_case1 - can't zip different length lists
    ],
)
def test_zip_tasks_verbose_output(test_case):
    table, stdstreams, expected, should_raise = test_case
    result = None
    raised = False
    try:
        result = mesos_tools.zip_tasks_verbose_output(table, stdstreams)
    except ValueError:
        raised = True

    assert raised == should_raise
    assert result == expected


@mark.asyncio
@mark.parametrize(
    "test_case",
    [
        # task_id, file1, file2, nlines, raise_what
        [
            "a_task",  # test_case0 - OK
            ["stdout", [str(x) for x in range(20)]],
            ["stderr", [str(x) for x in range(30)]],
            10,
            None,
        ],
        [
            "a_task",  # test_case1 - OK, short stdout, swapped stdout/stderr
            ["stderr", [str(x) for x in range(30)]],
            ["stdout", ["1", "2"]],
            10,
            None,
        ],
        ["a_task", None, None, 10, mesos.exceptions.MasterNotAvailableException],
        ["a_task", None, None, 10, mesos.exceptions.SlaveDoesNotExist],
        ["a_task", None, None, 10, mesos.exceptions.TaskNotFoundException],
        ["a_task", None, None, 10, mesos.exceptions.FileNotFoundForTaskException],
        ["a_task", None, None, 10, utils.TimeoutError],
    ],
)
async def test_format_stdstreams_tail_for_task(
    test_case,
):
    def gen_mesos_cli_fobj(file_path, file_lines):
        """mesos.cli.cluster.files (0.1.5),
        returns a list of mesos.cli.mesos_file.File
        `File` is an iterator-like object.
        """

        async def _readlines_reverse():
            for line in reversed(file_lines):
                yield line

        fobj = mock.create_autospec(mesos.mesos_file.File)
        fobj.path = file_path
        fobj._readlines_reverse = _readlines_reverse
        return fobj

    def get_short_task_id(task_id):
        return task_id

    def gen_mock_cluster_files(file1, file2, raise_what):
        async def mock_cluster_files(*args, **kwargs):
            # If we're asked to raise a particular exception we do so.
            # .message is set to the exception class name.
            if raise_what:
                raise raise_what(raise_what)
            yield gen_mesos_cli_fobj(file1[0], file1[1])
            yield gen_mesos_cli_fobj(file2[0], file2[1])

        return mock_cluster_files

    def gen_output(task_id, file1, file2, nlines, raise_what):
        error_message = "    " + PaastaColors.red(
            "  couldn't read stdout/stderr for %s (%s)"
        )
        output = []
        if not raise_what:
            files = [file1, file2]
            # reverse sort because stdout is supposed to always come before stderr in the output
            files.sort(key=lambda f: f[0], reverse=True)
            for f in files:
                output.append(
                    "    " + PaastaColors.blue("{} tail for {}".format(f[0], task_id))
                )
                output.extend(f"      {line}" for line in f[1][-nlines:])
        else:
            output.append(error_message % (task_id, raise_what.__name__))
        return output

    task_id, file1, file2, nlines, raise_what = test_case

    mock_cluster_files = gen_mock_cluster_files(file1, file2, raise_what)
    fake_task = {"id": task_id}
    expected = gen_output(task_id, file1, file2, nlines, raise_what)
    with asynctest.patch("paasta_tools.mesos_tools.get_mesos_config", autospec=True):
        with asynctest.patch(
            "paasta_tools.mesos_tools.cluster.get_files_for_tasks",
            mock_cluster_files,
            autospec=None,
        ):
            result = await mesos_tools.format_stdstreams_tail_for_task(
                fake_task, get_short_task_id
            )
            assert result == expected


def test_slave_pid_to_ip():
    ret = mesos_tools.slave_pid_to_ip("slave(1)@10.40.31.172:5051")
    assert ret == "10.40.31.172"


@mark.asyncio
async def test_get_mesos_task_count_by_slave():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_all_running_tasks", autospec=True
    ) as mock_get_all_running_tasks:
        mock_tron = mock.Mock()
        mock_tron.name = "tron"
        mock_somethingelse = mock.Mock()
        mock_somethingelse.name = "somethingelse"
        mock_task1 = mock.Mock()
        mock_task1.slave = asynctest.CoroutineMock(return_value={"id": "slave1"})
        mock_task1.framework = asynctest.CoroutineMock(return_value=mock_tron)
        mock_task2 = mock.Mock()
        mock_task2.slave = asynctest.CoroutineMock(return_value={"id": "slave1"})
        mock_task2.framework = asynctest.CoroutineMock(return_value=mock_somethingelse)
        mock_task3 = mock.Mock()
        mock_task3.slave = asynctest.CoroutineMock(return_value={"id": "slave1"})
        mock_task3.framework = asynctest.CoroutineMock(return_value=mock_somethingelse)
        mock_task4 = mock.Mock()
        mock_task4.slave = asynctest.CoroutineMock(return_value={"id": "slave2"})
        mock_task4.framework = asynctest.CoroutineMock(return_value=mock_somethingelse)
        mock_tasks = [mock_task1, mock_task2, mock_task3, mock_task4]
        mock_get_all_running_tasks.return_value = mock_tasks
        mock_slave_1 = {
            "id": "slave1",
            "attributes": {"pool": "default"},
            "hostname": "host1",
        }
        mock_slave_2 = {
            "id": "slave2",
            "attributes": {"pool": "default"},
            "hostname": "host2",
        }
        mock_slave_3 = {
            "id": "slave3",
            "attributes": {"pool": "another"},
            "hostname": "host3",
        }
        mock_mesos_state = {"slaves": [mock_slave_1, mock_slave_2, mock_slave_3]}
        ret = await mesos_tools.get_mesos_task_count_by_slave(
            mock_mesos_state, pool="default"
        )
        assert mock_get_all_running_tasks.called
        expected = [
            {"task_counts": mesos_tools.SlaveTaskCount(count=3, slave=mock_slave_1)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=1, slave=mock_slave_2)},
        ]
        assert len(ret) == len(expected) and utils.sort_dicts(ret) == utils.sort_dicts(
            expected
        )
        ret = await mesos_tools.get_mesos_task_count_by_slave(
            mock_mesos_state, pool=None
        )
        assert mock_get_all_running_tasks.called
        expected = [
            {"task_counts": mesos_tools.SlaveTaskCount(count=3, slave=mock_slave_1)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=1, slave=mock_slave_2)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_3)},
        ]
        assert len(ret) == len(expected) and utils.sort_dicts(ret) == utils.sort_dicts(
            expected
        )

        # test slaves_list override
        mock_task2 = mock.Mock()
        mock_task2.slave = asynctest.CoroutineMock(return_value={"id": "slave2"})
        mock_task2.framework = asynctest.CoroutineMock(return_value=mock_somethingelse)
        mock_task3 = mock.Mock()
        mock_task3.slave = asynctest.CoroutineMock(return_value={"id": "slave2"})
        mock_task3.framework = asynctest.CoroutineMock(return_value=mock_somethingelse)
        mock_tasks = [mock_task1, mock_task2, mock_task3, mock_task4]
        mock_get_all_running_tasks.return_value = mock_tasks
        mock_slaves_list = [
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_1)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_2)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_3)},
        ]
        ret = await mesos_tools.get_mesos_task_count_by_slave(
            mock_mesos_state, slaves_list=mock_slaves_list
        )
        expected = [
            {"task_counts": mesos_tools.SlaveTaskCount(count=1, slave=mock_slave_1)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=3, slave=mock_slave_2)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_3)},
        ]
        assert len(ret) == len(expected) and utils.sort_dicts(ret) == utils.sort_dicts(
            expected
        )

        # test SlaveDoesNotExist exception handling
        mock_task2.__getitem__ = mock.Mock(side_effect="fakeid")
        mock_task2.slave = asynctest.CoroutineMock(
            return_value=mock.Mock(
                __getitem__=mock.Mock(side_effect=mesos.exceptions.SlaveDoesNotExist)
            )
        )
        # we expect to handle this SlaveDoesNotExist exception gracefully, and continue on to handle other tasks
        mock_tasks = [mock_task1, mock_task2, mock_task3, mock_task4]
        mock_get_all_running_tasks.return_value = mock_tasks
        mock_slaves_list = [
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_1)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_2)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_3)},
        ]
        ret = await mesos_tools.get_mesos_task_count_by_slave(
            mock_mesos_state, slaves_list=mock_slaves_list
        )
        # we expect mock_slave_2 to only count 2 tasks, as one of them returned a SlaveDoesNotExist exception
        expected = [
            {"task_counts": mesos_tools.SlaveTaskCount(count=1, slave=mock_slave_1)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=2, slave=mock_slave_2)},
            {"task_counts": mesos_tools.SlaveTaskCount(count=0, slave=mock_slave_3)},
        ]
        assert len(ret) == len(expected) and utils.sort_dicts(ret) == utils.sort_dicts(
            expected
        )


def test_get_count_running_tasks_on_slave():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_master", autospec=True
    ) as mock_get_master, asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_task_count_by_slave", autospec=True
    ) as mock_get_mesos_task_count_by_slave:
        mock_master = mock.Mock()
        mock_mesos_state = mock.Mock()
        mock_master.state_summary = asynctest.CoroutineMock(
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
            return_value=mock_mesos_state,
        )
        mock_get_master.return_value = mock_master

        mock_slave_counts = [
            {"task_counts": mock.Mock(count=3, slave={"hostname": "host1"})},
            {"task_counts": mock.Mock(count=0, slave={"hostname": "host2"})},
        ]
        mock_get_mesos_task_count_by_slave.return_value = mock_slave_counts

        assert mesos_tools.get_count_running_tasks_on_slave("host1") == 3
        assert mesos_tools.get_count_running_tasks_on_slave("host2") == 0
        assert mesos_tools.get_count_running_tasks_on_slave("host3") == 0
        assert mock_master.state_summary.called
        mock_get_mesos_task_count_by_slave.assert_called_with(mock_mesos_state)


def _ids(list_of_mocks):
    return {id(mck) for mck in list_of_mocks}


@mark.asyncio
async def test_get_tasks_from_app_id():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_running_tasks_from_frameworks", autospec=True
    ) as mock_get_running_tasks_from_frameworks:
        mock_task_1 = mock.Mock(
            slave=asynctest.CoroutineMock(return_value={"hostname": "host1"})
        )
        mock_task_2 = mock.Mock(
            slave=asynctest.CoroutineMock(return_value={"hostname": "host2"})
        )
        mock_task_3 = mock.Mock(
            slave=asynctest.CoroutineMock(return_value={"hostname": "host2.domain"})
        )
        mock_get_running_tasks_from_frameworks.return_value = [
            mock_task_1,
            mock_task_2,
            mock_task_3,
        ]

        ret = await mesos_tools.get_tasks_from_app_id("app_id")
        mock_get_running_tasks_from_frameworks.assert_called_with("app_id")
        expected = [mock_task_1, mock_task_2, mock_task_3]
        assert len(expected) == len(ret) and _ids(ret) == _ids(expected)

        ret = await mesos_tools.get_tasks_from_app_id("app_id", slave_hostname="host2")
        mock_get_running_tasks_from_frameworks.assert_called_with("app_id")
        expected = [mock_task_2, mock_task_3]
        assert len(expected) == len(ret) and _ids(ret) == _ids(expected)


@mark.asyncio
async def test_get_task():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_running_tasks_from_frameworks", autospec=True
    ) as mock_get_running_tasks_from_frameworks:
        mock_task_1 = {"id": "123"}
        mock_task_2 = {"id": "789"}
        mock_task_3 = {"id": "789"}
        mock_get_running_tasks_from_frameworks.return_value = [
            mock_task_1,
            mock_task_2,
            mock_task_3,
        ]
        ret = await mesos_tools.get_task("123", app_id="app_id")
        mock_get_running_tasks_from_frameworks.assert_called_with("app_id")
        assert ret == mock_task_1

        with raises(mesos_tools.TaskNotFound):
            await mesos_tools.get_task("111", app_id="app_id")

        with raises(mesos_tools.TooManyTasks):
            await mesos_tools.get_task("789", app_id="app_id")


@mark.asyncio
async def test_filter_task_by_hostname():
    mock_task = mock.Mock(
        slave=asynctest.CoroutineMock(return_value={"hostname": "host1"})
    )
    assert await mesos_tools.filter_task_by_hostname(mock_task, "host1")
    assert not await mesos_tools.filter_task_by_hostname(mock_task, "host2")


def test_filter_task_by_task_id():
    mock_task = {"id": "123"}
    assert mesos_tools.filter_task_by_task_id(mock_task, "123")
    assert not mesos_tools.filter_task_by_task_id(mock_task, "456")


def test_get_all_tasks_from_state():
    mock_task_1 = mock.Mock()
    mock_task_2 = mock.Mock()
    mock_task_3 = mock.Mock()
    mock_task_4 = mock.Mock()
    mock_state = {
        "frameworks": [{"tasks": [mock_task_1, mock_task_2]}, {"tasks": [mock_task_3]}],
        "orphan_tasks": [mock_task_4],
    }
    ret = mesos_tools.get_all_tasks_from_state(mock_state)
    expected = [mock_task_1, mock_task_2, mock_task_3]
    assert len(ret) == len(expected) and ret == expected

    ret = mesos_tools.get_all_tasks_from_state(mock_state, include_orphans=True)
    expected = [mock_task_1, mock_task_2, mock_task_3, mock_task_4]
    assert len(ret) == len(expected) and ret == expected


@mark.asyncio
async def test_get_running_tasks_from_frameworks():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_current_tasks", autospec=True
    ) as mock_get_current_tasks, asynctest.patch(
        "paasta_tools.mesos_tools.filter_running_tasks", autospec=True
    ) as mock_filter_running_tasks:
        ret = await mesos_tools.get_running_tasks_from_frameworks(job_id="")
        mock_get_current_tasks.assert_called_with("")
        mock_filter_running_tasks.assert_called_with(
            mock_get_current_tasks.return_value
        )
        assert ret == mock_filter_running_tasks.return_value


@mark.asyncio
async def test_get_all_running_tasks():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_current_tasks", autospec=True
    ) as mock_get_current_tasks, asynctest.patch(
        "paasta_tools.mesos_tools.filter_running_tasks", autospec=True
    ) as mock_filter_running_tasks, asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_master", autospec=True
    ) as mock_get_mesos_master:
        mock_task_1 = mock.Mock()
        mock_task_2 = mock.Mock()
        mock_task_3 = mock.Mock()

        mock_get_current_tasks.return_value = [mock_task_1, mock_task_2]
        mock_orphan_tasks = asynctest.CoroutineMock(return_value=[mock_task_3])
        mock_mesos_master = mock.Mock(orphan_tasks=mock_orphan_tasks)
        mock_get_mesos_master.return_value = mock_mesos_master

        ret = await mesos_tools.get_all_running_tasks()
        mock_get_current_tasks.assert_called_with("")
        mock_filter_running_tasks.assert_called_with(
            [mock_task_1, mock_task_2, mock_task_3]
        )
        assert ret == mock_filter_running_tasks.return_value


@mark.asyncio
async def test_get_non_running_tasks_from_frameworks():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_current_tasks", autospec=True
    ) as mock_get_current_tasks, asynctest.patch(
        "paasta_tools.mesos_tools.filter_not_running_tasks", autospec=True
    ) as mock_filter_not_running_tasks:
        ret = await mesos_tools.get_non_running_tasks_from_frameworks(job_id="")
        mock_get_current_tasks.assert_called_with("")
        mock_filter_not_running_tasks.assert_called_with(
            mock_get_current_tasks.return_value
        )
        assert ret == mock_filter_not_running_tasks.return_value


@mark.asyncio
async def test_get_current_tasks():
    with asynctest.patch(
        "paasta_tools.mesos_tools.get_mesos_master", autospec=True
    ) as mock_get_mesos_master:
        mock_task_1 = mock.Mock()
        mock_task_2 = mock.Mock()
        mock_tasks = asynctest.CoroutineMock(return_value=[mock_task_1, mock_task_2])
        mock_mesos_master = mock.Mock(tasks=mock_tasks)
        mock_get_mesos_master.return_value = mock_mesos_master

        expected = [mock_task_1, mock_task_2]
        ret = await mesos_tools.get_current_tasks("")
        assert ret == expected and len(ret) == len(expected)


def test_mesos_services_running_here():
    with mock.patch(
        "paasta_tools.mesos_tools.get_local_slave_state", autospec=True
    ) as mock_get_local_slave_state:
        mock_state = {
            "frameworks": [
                {
                    "name": "marathon2",
                    "executors": [
                        {
                            "id": "thing.main",
                            "resources": {"ports": "[31062-31062]"},
                            "tasks": [{"state": "TASK_RUNNING"}],
                        },
                        {"id": "thing.another", "tasks": [{"state": "TASK_LOST"}]},
                    ],
                },
                {
                    "name": "tron",
                    "executors": [
                        {
                            "id": "c.main",
                            "resources": {},
                            "tasks": [{"state": "TASK_RUNNING"}],
                        },
                        {
                            "id": "c.another",
                            "resources": {},
                            "tasks": [{"state": "TASK_RUNNING"}],
                        },
                    ],
                },
            ]
        }
        mock_get_local_slave_state.return_value = {}
        assert (
            mesos_tools.mesos_services_running_here(
                lambda _: True, lambda id_str: id_str.split(".")
            )
            == []
        )

        mock_get_local_slave_state.return_value = mock_state
        expected = [
            ("thing", "main", 31062),
            ("c", "main", None),
            ("c", "another", None),
        ]
        assert (
            mesos_tools.mesos_services_running_here(
                lambda _: True, lambda id_str: id_str.split(".")
            )
            == expected
        )

        mock_fw_filter = mock.Mock(side_effect=[True, False])
        expected = [("thing", "main", 31062)]
        assert (
            mesos_tools.mesos_services_running_here(
                mock_fw_filter, lambda id_str: id_str.split(".")
            )
            == expected
        )

        mock_parse_service_instance_from_executor_id = mock.Mock(
            side_effect=[("thing", "main"), ValueError, ("c", "another")]
        )
        expected = [("thing", "main", 31062), ("c", "another", None)]
        assert (
            mesos_tools.mesos_services_running_here(
                lambda _: True, mock_parse_service_instance_from_executor_id
            )
            == expected
        )
