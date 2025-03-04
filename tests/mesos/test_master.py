from asynctest import CoroutineMock
from asynctest import patch
from mock import call
from mock import Mock
from pytest import mark

from paasta_tools.mesos import framework
from paasta_tools.mesos import master
from paasta_tools.mesos import task


@mark.asyncio
async def test_frameworks():
    with patch.object(
        master.MesosMaster, "_framework_list", autospec=True
    ) as mock_framework_list:
        fake_frameworks = [{"name": "test_framework1"}, {"name": "test_framework2"}]
        mock_framework_list.return_value = fake_frameworks
        expected_frameworks = [
            framework.Framework(config) for config in fake_frameworks
        ]
        mesos_master = master.MesosMaster({})
        assert expected_frameworks == await mesos_master.frameworks()


@mark.asyncio
async def test_framework_list_includes_completed_frameworks():
    with patch.object(
        master.MesosMaster, "_framework_list", autospec=True
    ) as mock_framework_list:
        fake_frameworks = [{"name": "test_framework1"}, {"name": "test_framework2"}]
        mock_framework_list.return_value = fake_frameworks
        expected_frameworks = [
            framework.Framework(config) for config in fake_frameworks
        ]
        mesos_master = master.MesosMaster({})
        assert expected_frameworks == await mesos_master.frameworks()


@mark.asyncio
async def test__frameworks():
    with patch.object(master.MesosMaster, "fetch", autospec=True) as mock_fetch:
        mesos_master = master.MesosMaster({})
        mock_frameworks = Mock()
        mock_fetch.return_value = CoroutineMock(
            json=CoroutineMock(return_value=mock_frameworks)
        )
        ret = await mesos_master._frameworks()
        mock_fetch.assert_called_with(mesos_master, "/master/frameworks", cached=True)
        assert ret == mock_frameworks


@mark.asyncio
async def test__framework_list():
    mock_frameworks = Mock()
    mock_completed = Mock()
    with patch.object(
        master.MesosMaster,
        "_frameworks",
        autospec=True,
        return_value={
            "frameworks": [mock_frameworks],
            "completed_frameworks": [mock_completed],
        },
    ):
        mesos_master = master.MesosMaster({})
        ret = await mesos_master._framework_list()
        expected = [mock_frameworks, mock_completed]
        assert list(ret) == expected

        ret = await mesos_master._framework_list(active_only=True)
        expected = [mock_frameworks]
        assert list(ret) == expected


@mark.asyncio
async def test__task_list():
    mock_task_1 = Mock()
    mock_task_2 = Mock()
    mock_framework = {"tasks": [mock_task_1], "completed_tasks": [mock_task_2]}
    with patch.object(
        master.MesosMaster,
        "_framework_list",
        autospec=True,
        return_value=[mock_framework],
    ) as mock__frameworks_list:
        mesos_master = master.MesosMaster({})
        ret = await mesos_master._task_list()
        mock__frameworks_list.assert_called_with(mesos_master, False)
        expected = [mock_task_1, mock_task_2]
        assert list(ret) == expected

        ret = await mesos_master._task_list(active_only=True)
        expected = [mock_task_1]
        assert list(ret) == expected

        ret = await mesos_master._task_list(active_only=False)
        expected = [mock_task_1, mock_task_2]
        assert list(ret) == expected


@mark.asyncio
async def test_tasks():
    with patch.object(
        master.MesosMaster, "_task_list", autospec=True
    ) as mock__task_list, patch.object(task, "Task", autospec=True) as mock_task:
        mock_task_1 = {"id": "aaa"}
        mock_task_2 = {"id": "bbb"}
        mock__task_list.return_value = [mock_task_1, mock_task_2]
        mock_task.return_value = Mock()
        mesos_master = master.MesosMaster({})
        ret = await mesos_master.tasks()
        mock_task.assert_has_calls(
            [call(mesos_master, mock_task_1), call(mesos_master, mock_task_2)]
        )
        mock__task_list.assert_called_with(mesos_master, False)
        expected = [mock_task.return_value, mock_task.return_value]
        assert list(ret) == expected


@mark.asyncio
async def test_orphan_tasks():
    mesos_master = master.MesosMaster({})
    mock_task_1 = Mock()
    mesos_master.state = CoroutineMock(return_value={"orphan_tasks": [mock_task_1]})
    assert await mesos_master.orphan_tasks() == [mock_task_1]
