import copy
import json
from typing import Any
from typing import Dict
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import ZnodeStat

from paasta_tools.utils import _log


class MesosTaskParametersIsImmutableError(Exception):
    pass


_SelfT = TypeVar("_SelfT", bound="MesosTaskParameters")


class MesosTaskParameters:
    health: Any
    mesos_task_state: str
    is_draining: bool
    is_healthy: bool
    offer: Any
    resources: Any

    def __init__(
        self,
        health=None,
        mesos_task_state=None,
        is_draining=None,
        is_healthy=None,
        offer=None,
        resources=None,
    ):
        self.__dict__["health"] = health
        self.__dict__["mesos_task_state"] = mesos_task_state
        self.__dict__["is_draining"] = is_draining
        self.__dict__["is_healthy"] = is_healthy
        self.__dict__["offer"] = offer
        self.__dict__["resources"] = resources

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "{}(\n    {})".format(
            type(self).__name__,
            ",\n    ".join(["%s=%r" % kv for kv in self.__dict__.items()]),
        )

    def __setattr__(self, name, value):
        raise MesosTaskParametersIsImmutableError()

    def __delattr__(self, name):
        raise MesosTaskParametersIsImmutableError()

    def merge(self: _SelfT, **kwargs) -> "MesosTaskParameters":
        """Return a merged MesosTaskParameters object, where attributes in other take precedence over self."""

        new_dict = copy.deepcopy(self.__dict__)
        new_dict.update(kwargs)

        return MesosTaskParameters(**new_dict)

    @classmethod
    def deserialize(cls: Type[_SelfT], serialized_params: Union[str, bytes]) -> _SelfT:
        return cls(**json.loads(serialized_params))

    def serialize(self):
        return json.dumps(self.__dict__).encode("utf-8")


class TaskStore:
    def __init__(self, service_name, instance_name, framework_id, system_paasta_config):
        self.service_name = service_name
        self.instance_name = instance_name
        self.framework_id = framework_id
        self.system_paasta_config = system_paasta_config

    def get_task(self, task_id: str) -> MesosTaskParameters:
        """Get task data for task_id. If we don't know about task_id, return None"""
        raise NotImplementedError()

    def get_all_tasks(self) -> Dict[str, MesosTaskParameters]:
        """Returns a dictionary of task_id -> MesosTaskParameters for all known tasks."""
        raise NotImplementedError()

    def overwrite_task(self, task_id: str, params: MesosTaskParameters) -> None:
        raise NotImplementedError()

    def add_task_if_doesnt_exist(self, task_id: str, **kwargs) -> None:
        """Add a task if it does not already exist. If it already exists, do nothing."""
        if self.get_task(task_id) is not None:
            return
        else:
            self.overwrite_task(task_id, MesosTaskParameters(**kwargs))

    def update_task(self, task_id: str, **kwargs) -> MesosTaskParameters:
        existing_task = self.get_task(task_id)
        if existing_task:
            merged_params = existing_task.merge(**kwargs)
        else:
            merged_params = MesosTaskParameters(**kwargs)

        self.overwrite_task(task_id, merged_params)
        return merged_params

    def garbage_collect_old_tasks(self, max_dead_task_age: float) -> None:
        # TODO: call me.
        # TODO: implement in base class.
        raise NotImplementedError()

    def close(self):
        pass


class DictTaskStore(TaskStore):
    def __init__(self, service_name, instance_name, framework_id, system_paasta_config):
        self.tasks: Dict[str, MesosTaskParameters] = {}
        super().__init__(
            service_name, instance_name, framework_id, system_paasta_config
        )

    def get_task(self, task_id: str) -> MesosTaskParameters:
        return self.tasks.get(task_id)

    def get_all_tasks(self) -> Dict[str, MesosTaskParameters]:
        """Returns a dictionary of task_id -> MesosTaskParameters for all known tasks."""
        return dict(self.tasks)

    def overwrite_task(self, task_id: str, params: MesosTaskParameters) -> None:
        # serialize/deserialize to make sure the returned values are the same format as ZKTaskStore.
        self.tasks[task_id] = MesosTaskParameters.deserialize(params.serialize())


class ZKTaskStore(TaskStore):
    def __init__(self, service_name, instance_name, framework_id, system_paasta_config):
        super().__init__(
            service_name, instance_name, framework_id, system_paasta_config
        )
        self.zk_hosts = system_paasta_config.get_zk_hosts()

        # For some reason, I could not get the code suggested by this SO post to work to ensure_path on the chroot.
        # https://stackoverflow.com/a/32785625/25327
        # Plus, it just felt dirty to modify instance attributes of a running connection, especially given that
        # KazooClient.set_hosts() doesn't allow you to change the chroot. Must be for a good reason.

        chroot = f"task_store/{service_name}/{instance_name}/{framework_id}"

        temp_zk_client = KazooClient(hosts=self.zk_hosts)
        temp_zk_client.start()
        temp_zk_client.ensure_path(chroot)
        temp_zk_client.stop()
        temp_zk_client.close()

        self.zk_client = KazooClient(hosts=f"{self.zk_hosts}/{chroot}")
        self.zk_client.start()
        self.zk_client.ensure_path("/")

    def close(self):
        self.zk_client.stop()
        self.zk_client.close()

    def get_task(self, task_id: str) -> MesosTaskParameters:
        params, stat = self._get_task(task_id)
        return params

    def _get_task(self, task_id: str) -> Tuple[MesosTaskParameters, ZnodeStat]:
        """Like get_task, but also returns the ZnodeStat that self.zk_client.get() returns """
        try:
            data, stat = self.zk_client.get("/%s" % task_id)
            return MesosTaskParameters.deserialize(data), stat
        except NoNodeError:
            return None, None
        except json.decoder.JSONDecodeError:
            _log(
                service=self.service_name,
                instance=self.instance_name,
                level="debug",
                component="deploy",
                line=f"Warning: found non-json-decodable value in zookeeper for task {task_id}: {data}",
            )
            return None, None

    def get_all_tasks(self):
        all_tasks = {}

        for child_path in self.zk_client.get_children("/"):
            task_id = self._task_id_from_zk_path(child_path)
            params = self.get_task(task_id)
            # sometimes there are bogus child ZK nodes. Ignore them.
            if params is not None:
                all_tasks[task_id] = params

        return all_tasks

    def update_task(self, task_id: str, **kwargs):
        retry = True
        while retry:
            retry = False
            existing_task, stat = self._get_task(task_id)

            zk_path = self._zk_path_from_task_id(task_id)
            if existing_task:
                merged_params = existing_task.merge(**kwargs)
                try:
                    self.zk_client.set(
                        zk_path, merged_params.serialize(), version=stat.version
                    )
                except BadVersionError:
                    retry = True
            else:
                merged_params = MesosTaskParameters(**kwargs)
                try:
                    self.zk_client.create(zk_path, merged_params.serialize())
                except NodeExistsError:
                    retry = True

        return merged_params

    def overwrite_task(
        self, task_id: str, params: MesosTaskParameters, version=-1
    ) -> None:
        try:
            self.zk_client.set(
                self._zk_path_from_task_id(task_id), params.serialize(), version=version
            )
        except NoNodeError:
            self.zk_client.create(
                self._zk_path_from_task_id(task_id), params.serialize()
            )

    def _zk_path_from_task_id(self, task_id: str) -> str:
        return "/%s" % task_id

    def _task_id_from_zk_path(self, zk_path: str) -> str:
        return zk_path.lstrip("/")
