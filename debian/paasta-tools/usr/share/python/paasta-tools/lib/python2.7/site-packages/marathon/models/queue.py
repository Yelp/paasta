from .base import MarathonResource
from .app import MarathonApp


class MarathonQueueItem(MarathonResource):
    """Marathon queue item.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#queue

    :param app:
    :type app: :class:`marathon.models.app.MarathonApp` or dict
    :param bool overdue:
    """

    def __init__(self, app=None, overdue=None):
        self.app = app if isinstance(app, MarathonApp) else MarathonApp().from_json(app)
        self.overdue = overdue
