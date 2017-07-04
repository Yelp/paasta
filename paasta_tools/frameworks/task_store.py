from __future__ import absolute_import
from __future__ import unicode_literals


class MesosTaskParametersIsImmutableError(Exception):
    pass


class MesosTaskParameters(object):
    def __init__(
        self,
        health=None,
        mesos_task_state=None,
        is_draining=None,
        is_healthy=None,
        offer=None
    ):
        self.__dict__['health'] = health
        self.__dict__['mesos_task_state'] = mesos_task_state
        self.__dict__['is_draining'] = is_draining
        self.__dict__['is_healthy'] = is_healthy
        self.__dict__['offer'] = offer

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "%s(\n    %s)" % (type(self).__name__, ',\n    '.join(["%s=%r" % kv for kv in self.__dict__.items()]))

    def __setattr__(self, name, value):
        raise MesosTaskParametersIsImmutableError()

    def __delattr__(self, name):
        raise MesosTaskParametersIsImmutableError()

    def merge(self, **kwargs):
        """Return a merged MesosTaskParameters object, where attributes in other take precedence over self."""
        return MesosTaskParameters(
            health=kwargs.get('health', self.health),
            mesos_task_state=kwargs.get('mesos_task_state', self.mesos_task_state),
            is_draining=kwargs.get('is_draining', self.is_draining),
            is_healthy=kwargs.get('is_healthy', self.is_healthy),
            offer=kwargs.get('offer', self.offer),
        )


class TaskStore(object):
    def __init__(self, service_name, instance_name):
        self.service_name = service_name
        self.instance_name = instance_name

    def get_task(self, task_id):
        """Get task data for task_id. If we don't know about task_id, return None"""
        raise NotImplementedError()

    def get_all_tasks(self):
        """Returns a dictionary of task_id -> MesosTaskParameters for all known tasks."""
        raise NotImplementedError()

    def overwrite_task(self, task_id, params):
        raise NotImplementedError()

    def add_task_if_doesnt_exist(self, task_id, **kwargs):
        """Add a task if it does not already exist. If it already exists, do nothing."""
        if self.get_task(task_id) is not None:
            return
        else:
            self.overwrite_task(task_id, MesosTaskParameters(**kwargs))

    def update_task(self, task_id, **kwargs):
        if task_id in self.tasks:
            merged_params = self.tasks[task_id].merge(**kwargs)
        else:
            merged_params = MesosTaskParameters(**kwargs)

        self.overwrite_task(task_id, merged_params)
        return merged_params

    def garbage_collect_old_tasks(self, max_dead_task_age):
        # TODO: call me.
        # TODO: implement in base class.
        raise NotImplementedError()


class DictTaskStore(TaskStore):
    def __init__(self, service_name, instance_name):
        self.tasks = {}
        super(DictTaskStore, self).__init__(service_name, instance_name)

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def get_all_tasks(self):
        """Returns a dictionary of task_id -> MesosTaskParameters for all known tasks."""
        return dict(self.tasks)

    def overwrite_task(self, task_id, params):
        self.tasks[task_id] = params
