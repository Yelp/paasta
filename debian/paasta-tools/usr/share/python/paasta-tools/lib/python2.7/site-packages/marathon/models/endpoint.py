try:
    import json
except ImportError:
    import simplejson as json

from .base import MarathonResource
from .task import MarathonTask


class MarathonEndpoint(MarathonResource):
    """(DEPRECATED) Marathon Endpoint resource.

    This was removed in Marathon 0.7.0.

    :param str app_id: application id
    :param str host: mesos slave running the task
    :param str id: task id
    :param int app_port: port seen by the application
    :param int task_port: port allocated on the slave
    """

    def __repr__(self):
        return "{clazz}::{app_id}::{task_id}".format(clazz=self.__class__.__name__, app_id=self.app_id, task_id=self.task_id)

    def __init__(self, app_id=None, app_port=None, host=None, task_id=None, task_port=None):
        self.app_id = app_id
        self.app_port = app_port
        self.host = host
        self.task_id = task_id
        self.task_port = task_port

    @classmethod
    def from_json(cls, obj):
        """Construct a list of MarathonEndpoints from a parsed endpoints response.

        :param dict obj: object obj from parsed response

        :rtype: list[:class:`MarathonEndpoint`]
        """
        app_id = obj.get('id')
        app_ports = obj.get('ports')
        f = MarathonTask()
        endpoints = []
        tasks = [MarathonTask.from_json(i) for i in obj['instances']]

        for task in tasks:
            # If this fails, fundamental assumptions around port mappings are incorrect and we need to bail
            assert len(task.ports) == len(app_ports), "Mismatch between app and task ports. Something's wrong."

            for task_port in task.ports:
                index = task.ports.index(task_port)
                app_port = app_ports[index]
                endpoints.append(cls(app_id=app_id, app_port=app_port, host=task.host,
                                     task_id=task.id, task_port=task_port))

        return endpoints