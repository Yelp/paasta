from .base import MarathonObject, MarathonResource


class MarathonDeployment(MarathonResource):
    """Marathon Application resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#deployments

    :param list[str] affected_apps: list of affected app ids
    :param current_actions: current actions
    :type current_actions: list[:class:`marathon.models.deployment.MarathonDeploymentAction`] or list[dict]
    :param int current_step: current step
    :param str id: deployment id
    :param steps: deployment steps
    :type steps: list[:class:`marathon.models.deployment.MarathonDeploymentAction`] or list[dict]
    :param int total_steps: total number of steps
    :param str version: version id
    """

    def __init__(self, affected_apps=None, current_actions=None, current_step=None, id=None, steps=None,
                 total_steps=None, version=None):
        self.affected_apps = affected_apps
        self.current_actions = [
            a if isinstance(a, MarathonDeploymentAction) else MarathonDeploymentAction().from_json(a)
            for a in (current_actions or [])
        ]
        self.current_step = current_step
        self.id = id
        self.steps = [
            s if isinstance(a, MarathonDeploymentAction) else MarathonDeploymentAction().from_json(a)
            for s in (steps or [])
        ]
        self.total_steps = total_steps
        self.version = version


class MarathonDeploymentAction(MarathonObject):
    """Marathon Application resource.

    See: https://mesosphere.github.io/marathon/docs/rest-api.html#deployments

    :param str action: action
    :param str app: app id
    :param str apps: app id (see https://github.com/mesosphere/marathon/pull/802)
    """

    def __init__(self, action=None, app=None, apps=None):
        self.action = action
        self.app = app
        self.apps = apps
