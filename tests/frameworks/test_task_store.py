import json

import mock
import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError

from paasta_tools.frameworks.task_store import DictTaskStore
from paasta_tools.frameworks.task_store import MesosTaskParameters
from paasta_tools.frameworks.task_store import ZKTaskStore


def test_DictTaskStore():
    task_store = DictTaskStore(
        service_name="foo",
        instance_name="bar",
        framework_id="foo",
        system_paasta_config=None,
    )
    task_store.add_task_if_doesnt_exist("task_id", mesos_task_state="foo")

    task_store.update_task("task_id", is_draining=True)

    assert task_store.get_all_tasks() == {
        "task_id": MesosTaskParameters(mesos_task_state="foo", is_draining=True)
    }

    task_store.update_task("task_id", mesos_task_state="bar")

    assert task_store.get_all_tasks() == {
        "task_id": MesosTaskParameters(mesos_task_state="bar", is_draining=True)
    }


class TestMesosTaskParameters:
    def test_serdes(self):
        param_dict = {
            "health": "health",
            "mesos_task_state": "mesos_task_state",
            "is_draining": True,
            "is_healthy": True,
            "offer": "offer",
            "resources": "resources",
        }

        assert json.loads(MesosTaskParameters(**param_dict).serialize()) == param_dict
        assert MesosTaskParameters.deserialize(
            json.dumps(param_dict)
        ) == MesosTaskParameters(**param_dict)


class TestZKTaskStore:
    @pytest.yield_fixture
    def mock_zk_client(self):
        spec_zk_client = KazooClient()
        mock_zk_client = mock.Mock(spec=spec_zk_client)
        with mock.patch(
            "paasta_tools.frameworks.task_store.KazooClient",
            autospec=True,
            return_value=mock_zk_client,
        ):
            yield mock_zk_client

    def test_get_task(self, mock_zk_client):
        zk_task_store = ZKTaskStore(
            service_name="a",
            instance_name="b",
            framework_id="c",
            system_paasta_config=mock.Mock(),
        )

        fake_znodestat = mock.Mock()
        zk_task_store.zk_client.get.return_value = (
            '{"health": "healthy"}',
            fake_znodestat,
        )
        params, stat = zk_task_store._get_task("d")
        zk_task_store.zk_client.get.assert_called_once_with("/d")
        assert stat == fake_znodestat
        assert params.health == "healthy"

    def test_update_task(self, mock_zk_client):
        zk_task_store = ZKTaskStore(
            service_name="a",
            instance_name="b",
            framework_id="c",
            system_paasta_config=mock.Mock(),
        )

        # Happy case - task exists, no conflict on update.
        fake_znodestat = mock.Mock(version=1)
        zk_task_store.zk_client.get.return_value = (
            '{"health": "healthy"}',
            fake_znodestat,
        )
        new_params = zk_task_store.update_task("task_id", is_draining=True)
        assert new_params.is_draining is True
        assert new_params.health == "healthy"

        # Second happy case - no task exists.
        fake_znodestat = mock.Mock(version=1)
        zk_task_store.zk_client.get.side_effect = NoNodeError()
        new_params = zk_task_store.update_task("task_id", is_draining=True)
        assert new_params.is_draining is True
        assert new_params.health is None

        # Someone changed our data out from underneath us.
        zk_task_store.zk_client.get.reset_mock()
        zk_task_store.zk_client.set.reset_mock()
        zk_task_store.zk_client.get.side_effect = [
            ('{"health": "healthy"}', mock.Mock(version=1)),
            ('{"health": "healthy", "offer": "offer"}', mock.Mock(version=2)),
            (
                '{"health": "healthy", "offer": "offer", "resources": "resources"}',
                mock.Mock(version=3),
            ),
        ]
        zk_task_store.zk_client.set.side_effect = [
            BadVersionError,
            BadVersionError,
            None,
        ]
        new_params = zk_task_store.update_task("task_id", is_draining=True)
        assert zk_task_store.zk_client.get.call_count == 3
        zk_task_store.zk_client.get.assert_has_calls(
            [mock.call("/task_id"), mock.call("/task_id"), mock.call("/task_id")]
        )
        assert zk_task_store.zk_client.set.call_count == 3
        zk_task_store.zk_client.set.assert_has_calls(
            [
                mock.call("/task_id", mock.ANY, version=1),
                mock.call("/task_id", mock.ANY, version=2),
                mock.call("/task_id", mock.ANY, version=3),
            ]
        )
        assert new_params.is_draining is True
        assert new_params.health == "healthy"
        assert new_params.offer == "offer"
        assert new_params.resources == "resources"

        # Data wasn't there when we read it, but then was when we tried to create it
        zk_task_store.zk_client.get.reset_mock()
        zk_task_store.zk_client.set.reset_mock()
        zk_task_store.zk_client.create.reset_mock()
        zk_task_store.zk_client.get.side_effect = [
            NoNodeError,
            ('{"health": "healthy"}', mock.Mock(version=1)),
        ]
        zk_task_store.zk_client.create.side_effect = [NodeExistsError]
        zk_task_store.zk_client.set.side_effect = [None]
        new_params = zk_task_store.update_task("task_id", is_draining=True)
        assert zk_task_store.zk_client.get.call_count == 2
        zk_task_store.zk_client.get.assert_has_calls(
            [mock.call("/task_id"), mock.call("/task_id")]
        )
        assert zk_task_store.zk_client.create.call_count == 1
        zk_task_store.zk_client.create.assert_has_calls(
            [mock.call("/task_id", mock.ANY)]
        )
        assert zk_task_store.zk_client.set.call_count == 1
        zk_task_store.zk_client.set.assert_has_calls(
            [mock.call("/task_id", mock.ANY, version=1)]
        )
        assert new_params.is_draining is True
        assert new_params.health == "healthy"
        assert new_params.offer is None
