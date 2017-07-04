from __future__ import absolute_import
from __future__ import unicode_literals

from paasta_tools.frameworks.task_store import DictTaskStore
from paasta_tools.frameworks.task_store import MesosTaskParameters


def test_DictTaskStore():
    task_store = DictTaskStore(service_name="foo", instance_name="bar")
    task_store.add_task_if_doesnt_exist("task_id", mesos_task_state="foo")

    task_store.update_task("task_id", is_draining=True)

    assert task_store.get_all_tasks() == {
        "task_id": MesosTaskParameters(
            mesos_task_state="foo",
            is_draining=True,
        )
    }

    task_store.update_task("task_id", mesos_task_state="bar")

    assert task_store.get_all_tasks() == {
        "task_id": MesosTaskParameters(
            mesos_task_state="bar",
            is_draining=True,
        )
    }
