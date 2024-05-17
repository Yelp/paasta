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

import asynctest
import mock
from pytest import mark

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


def _ids(list_of_mocks):
    return {id(mck) for mck in list_of_mocks}


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
