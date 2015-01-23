import itertools
import time
import urlparse

try:
    import json
except ImportError:
    import simplejson as json
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError

import requests
import requests.exceptions

import marathon
from .models import MarathonApp, MarathonDeployment, MarathonGroup, MarathonInfo, MarathonTask, MarathonEndpoint
from .exceptions import InternalServerError, NotFoundError, MarathonHttpError, MarathonError


class MarathonClient(object):
    """Client interface for the Marathon REST API."""

    def __init__(self, servers, username=None, password=None, timeout=10):
        """Create a MarathonClient instance.

        If multiple servers are specified, each will be tried in succession until a non-"Connection Error"-type
        response is received. Servers are expected to have the same username and password.

        :param servers: One or a priority-ordered list of Marathon URLs (e.g., 'http://host:8080' or
        ['http://host1:8080','http://host2:8080'])
        :type servers: str or list[str]
        :param str username: Basic auth username
        :param str password: Basic auth password
        :param int timeout: Timeout (in seconds) for requests to Marathon
        """
        self.servers = servers if isinstance(servers, list) else [servers]
        self.auth = (username, password) if username and password else None
        self.timeout = timeout

    def __repr__(self):
        return 'Connection:%s' % self.servers

    @staticmethod
    def _parse_response(response, clazz, is_list=False, resource_name=None):
        """Parse a Marathon response into an object or list of objects."""
        target = response.json()[resource_name] if resource_name else response.json()
        if is_list:
            return [clazz.from_json(resource) for resource in target]
        else:
            return clazz.from_json(target)

    def _do_request(self, method, path, params=None, data=None):
        """Query Marathon server."""
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        response = None
        servers = list(self.servers)
        while servers and response is None:
            server = servers.pop(0)
            url = ''.join([server.rstrip('/'), path])
            try:
                response = requests.request(method, url, params=params, data=data, headers=headers,
                                            auth=self.auth, timeout=self.timeout)
                marathon.log.info('Got response from %s', server)
            except requests.exceptions.RequestException, e:
                marathon.log.error('Error while calling %s: %s', url, e.message)

        if response is None:
            raise MarathonError('No remaining Marathon servers to try')

        if response.status_code >= 500:
            marathon.log.error('Got HTTP {code}: {body}'.format(code=response.status_code, body=response.text))
            raise InternalServerError(response)
        elif response.status_code >= 400:
            marathon.log.error('Got HTTP {code}: {body}'.format(code=response.status_code, body=response.text))
            if response.status_code == 404:
                raise NotFoundError(response)
            else:
                raise MarathonHttpError(response)
        elif response.status_code >= 300:
            marathon.log.warn('Got HTTP {code}: {body}'.format(code=response.status_code, body=response.text))
        else:
            marathon.log.debug('Got HTTP {code}: {body}'.format(code=response.status_code, body=response.text))

        return response

    def list_endpoints(self):
        """List the current endpoints for all applications

        :returns: list of endpoints
        :rtype: list[`MarathonEndpoint`]
        """
        response = self._do_request('GET', '/v1/endpoints')
        endpoints = [MarathonEndpoint.from_json(app) for app in response.json()]
        # Flatten result
        return [item for sublist in endpoints for item in sublist]

    def create_app(self, app_id, app):
        """Create and start an app.

        :param str app_id: application ID
        :param :class:`marathon.models.app.MarathonApp` app: the application to create

        :returns: the created app (on success)
        :rtype: :class:`marathon.models.app.MarathonApp` or False
        """
        app.id = app_id
        data = app.to_json()
        response = self._do_request('POST', '/v2/apps', data=data)
        if response.status_code == 201:
            return self._parse_response(response, MarathonApp)
        else:
            return False

    def list_apps(self, cmd=None, embed_tasks=False, embed_failures=False, **kwargs):
        """List all apps.

        :param str app_id: application ID
        :param str cmd: if passed, only show apps with a matching `cmd`
        :param bool embed_tasks: embed tasks in result
        :param bool embed_failures: embed tasks and last task failure in result
        :param kwargs: arbitrary search filters

        :returns: list of applications
        :rtype: list[:class:`marathon.models.app.MarathonApp`]
        """
        params = {}
        if cmd:
            params['cmd'] = cmd

        if embed_failures:
            params['embed'] = 'apps.failures'
        elif embed_tasks:
            params['embed'] = 'apps.tasks'

        response = self._do_request('GET', '/v2/apps', params=params)
        apps = self._parse_response(response, MarathonApp, is_list=True, resource_name='apps')
        for k, v in kwargs.iteritems():
            apps = filter(lambda o: getattr(o, k) == v, apps)
        return apps

    def get_app(self, app_id, embed_tasks=False):
        """Get a single app.

        :param str app_id: application ID
        :param bool embed_tasks: embed tasks in result

        :returns: application
        :rtype: :class:`marathon.models.app.MarathonApp`
        """
        params = {'embed': 'apps.tasks'} if embed_tasks else {}
        response = self._do_request('GET', '/v2/apps/{app_id}'.format(app_id=app_id), params=params)
        return self._parse_response(response, MarathonApp, resource_name='app')

    def update_app(self, app_id, app, force=False):
        """Update an app.

        Applies writable settings in `app` to `app_id`
        Note: this method can not be used to rename apps.

        :param str app_id: target application ID
        :param app: application settings
        :type app: :class:`marathon.models.app.MarathonApp`
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        # Changes won't take if version is set - blank it for convenience
        app.version = None

        params = {'force': force}
        data = app.to_json(minimal=True)

        response = self._do_request('PUT', '/v2/apps/{app_id}'.format(app_id=app_id), params=params, data=data)
        return response.json()

    def rollback_app(self, app_id, version, force=False):
        """Roll an app back to a previous version.

        :param str app_id: application ID
        :param str version: application version
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        params = {'force': force}
        data = json.dumps({'version': version})
        response = self._do_request('PUT', '/v2/apps/{app_id}'.format(app_id=app_id), params=params, data=data)
        return response.json()

    def delete_app(self, app_id, force=False):
        """Stop and destroy an app.

        :param str app_id: application ID
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        params = {'force': force}
        response = self._do_request('DELETE', '/v2/apps/{app_id}'.format(app_id=app_id), params=params)
        return response.json()

    def scale_app(self, app_id, instances=None, delta=None, force=False):
        """Scale an app.

        Scale an app to a target number of instances (with `instances`), or scale the number of
        instances up or down by some delta (`delta`). If the resulting number of instances would be negative,
        desired instances will be set to zero.

        If both `instances` and `delta` are passed, use `instances`.

        :param str app_id: application ID
        :param int instances: [optional] the number of instances to scale to
        :param int delta: [optional] the number of instances to scale up or down by
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        if instances is None and delta is None:
            marathon.log.error('instances or delta must be passed')
            return

        try:
            app = self.get_app(app_id)
        except NotFoundError:
            marathon.log.error('App "{app}" not found'.format(app=app_id))
            return

        desired = instances if instances is not None else (app.instances + delta)
        return self.update_app(app.id, MarathonApp(instances=desired), force=force)

    def create_group(self, group):
        """Create and start a group.

        :param :class:`marathon.models.group.MarathonGroup` group: the group to create

        :returns: success
        :rtype: dict containing the version ID
        """
        data = group.to_json()
        response = self._do_request('POST', '/v2/groups', data=data)
        return response.json()

    def list_groups(self, **kwargs):
        """List all groups.

        :param kwargs: arbitrary search filters

        :returns: list of groups
        :rtype: list[:class:`marathon.models.group.MarathonGroup`]
        """
        response = self._do_request('GET', '/v2/groups')
        groups = self._parse_response(response, MarathonGroup, is_list=True, resource_name='groups')
        for k, v in kwargs.iteritems():
            groups = filter(lambda o: getattr(o, k) == v, groups)
        return groups

    def get_group(self, group_id):
        """Get a single group.

        :param str group_id: group ID

        :returns: group
        :rtype: :class:`marathon.models.group.MarathonGroup`
        """
        response = self._do_request('GET', '/v2/groups/{group_id}'.format(group_id=group_id))
        return self._parse_response(response, MarathonGroup, resource_name='group')

    def update_group(self, group_id, group, force=False):
        """Update a group.

        Applies writable settings in `group` to `group_id`
        Note: this method can not be used to rename groups.

        :param str group_id: target group ID
        :param group: group settings
        :type group: :class:`marathon.models.group.MarathonGroup`
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        # Changes won't take if version is set - blank it for convenience
        group.version = None

        params = {'force': force}
        data = group.to_json(minimal=True)

        response = self._do_request('PUT', '/v2/groups/{group_id}'.format(group_id=group_id), data=data, params=params)
        return response.json()

    def rollback_group(self, group_id, version, force=False):
        """Roll a group back to a previous version.

        :param str group_id: group ID
        :param str version: group version
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        params = {'force': force}
        response = self._do_request('PUT', '/v2/groups/{group_id}/versions/{version}'.format(group_id=group_id,
                                                                                             version=version),
                                    params=params)
        return response.json()

    def delete_group(self, group_id, force=False):
        """Stop and destroy a group.

        :param str group_id: group ID
        :param bool force: apply even if a deployment is in progress

        :returns: a dict containing the deleted version
        :rtype: dict
        """
        params = {'force': force}
        response = self._do_request('DELETE', '/v2/groups/{group_id}'.format(group_id=group_id), params=params)
        return response.json()

    def scale_group(self, group_id, scale_by):
        """Scale a group by a factor.

        :param str group_id: group ID
        :param int scale_by: factor to scale by

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        params = {'scaleBy': scale_by}
        response = self._do_request('PUT', '/v2/groups/{group_id}'.format(group_id=group_id), params=params)
        return response.json()

    def list_tasks(self, app_id=None, **kwargs):
        """List running tasks, optionally filtered by app_id.

        :param str app_id: if passed, only show tasks for this application
        :param kwargs: arbitrary search filters

        :returns: list of tasks
        :rtype: list[:class:`marathon.models.task.MarathonTask`]
        """
        if app_id:
            response = self._do_request('GET', '/v2/apps/{app_id}/tasks'.format(app_id=app_id))
        else:
            response = self._do_request('GET', '/v2/tasks')

        tasks = self._parse_response(response, MarathonTask, is_list=True, resource_name='tasks')
        [setattr(t, 'app_id', app_id) for t in tasks if app_id and t.app_id is None]
        for k, v in kwargs.iteritems():
            tasks = filter(lambda o: getattr(o, k) == v, tasks)

        return tasks

    def kill_tasks(self, app_id, scale=False, host=None, batch_size=0, batch_delay=0):
        """Kill all tasks belonging to app.

        :param str app_id: application ID
        :param bool scale: if true, scale down the app by the number of tasks killed
        :param str host: if provided, only terminate tasks on this Mesos slave
        :param int batch_size: if non-zero, terminate tasks in groups of this size
        :param int batch_delay: time (in seconds) to wait in between batched kills. If zero, automatically determine

        :returns: list of killed tasks
        :rtype: list[:class:`marathon.models.task.MarathonTask`]
        """
        def batch(iterable, size):
            sourceiter = iter(iterable)
            while True:
                batchiter = itertools.islice(sourceiter, size)
                yield itertools.chain([batchiter.next()], batchiter)

        if batch_size == 0:
            # Terminate all at once
            params = {'scale': scale}
            if host: params['host'] = host
            response = self._do_request('DELETE', '/v2/apps/{app_id}/tasks'.format(app_id=app_id), params)
            return self._parse_response(response, MarathonTask, is_list=True, resource_name='tasks')
        else:
            # Terminate in batches
            tasks = self.list_tasks(app_id, host=host) if host else self.list_tasks(app_id)
            for tbatch in batch(tasks, batch_size):
                killed_tasks = [self.kill_task(app_id, t.id, scale=scale) for t in tbatch]

                # Pause until the tasks have been killed to avoid race conditions
                killed_task_ids = set(t.id for t in killed_tasks)
                running_task_ids = killed_task_ids
                while killed_task_ids.intersection(running_task_ids):
                    time.sleep(1)
                    running_task_ids = set(t.id for t in self.get_app(app_id).tasks)

                if batch_delay == 0:
                    # Pause until the replacement tasks are healthy
                    desired_instances = self.get_app(app_id).instances
                    running_instances = 0
                    while running_instances < desired_instances:
                        time.sleep(1)
                        running_instances = sum(t.started_at is None for t in self.get_app(app_id).tasks)
                else:
                    time.sleep(batch_delay)

            return tasks

    def kill_task(self, app_id, task_id, scale=False):
        """Kill a task.

        :param str app_id: application ID
        :param str task_id: the task to kill
        :param bool scale: if true, scale down the app by one if the task exists

        :returns: the killed task
        :rtype: :class:`marathon.models.task.MarathonTask`
        """
        params = {'scale': scale}
        response = self._do_request('DELETE', '/v2/apps/{app_id}/tasks/{task_id}'
                                    .format(app_id=app_id, task_id=task_id), params)
        return self._parse_response(response, MarathonTask, resource_name='task')

    def list_versions(self, app_id):
        """List the versions of an app.

        :param str app_id: application ID

        :returns: list of versions
        :rtype: list[str]
        """
        response = self._do_request('GET', '/v2/apps/{app_id}/versions'.format(app_id=app_id))
        return [version for version in response.json()['versions']]

    def get_version(self, app_id, version):
        """Get the configuration of an app at a specific version.

        :param str app_id: application ID
        :param str version: application version

        :return: application configuration
        :rtype: :class:`marathon.models.app.MarathonApp`
        """
        response = self._do_request('GET', '/v2/apps/{app_id}/versions/{version}'
                                    .format(app_id=app_id, version=version))
        return MarathonApp(response.json())

    def list_event_subscriptions(self):
        """List the event subscriber callback URLs.

        :returns: list of callback URLs
        :rtype: list[str]
        """
        response = self._do_request('GET', '/v2/eventSubscriptions')
        return [url for url in response.json()['callbackUrls']]

    def create_event_subscription(self, url):
        """Register a callback URL as an event subscriber.

        :param str url: callback URL

        :returns: the created event subscription
        :rtype: dict
        """
        params = {'callbackUrl': url}
        response = self._do_request('POST', '/v2/eventSubscriptions', params)
        return response.json()

    def delete_event_subscription(self, url):
        """Deregister a callback URL as an event subscriber.

        :param str url: callback URL

        :returns: the deleted event subscription
        :rtype: dict
        """
        params = {'callbackUrl': url}
        response = self._do_request('DELETE', '/v2/eventSubscriptions', params)
        return response.json()

    def list_deployments(self):
        """List all running deployments.

        :returns: list of deployments
        :rtype: list[:class:`marathon.models.deployment.MarathonDeployment`]
        """
        response = self._do_request('GET', '/v2/deployments')
        return self._parse_response(response, MarathonDeployment, is_list=True)

    def delete_deployment(self, deployment_id):
        """Cancel a deployment.

        :param str deployment_id: deployment id

        :returns: a dict containing the deployment id and version
        :rtype: dict
        """
        response = self._do_request('DELETE', '/v2/deployments/{deployment}'.format(deployment=deployment_id))
        return response.json()

    def get_info(self):
        """Get server configuration information.

        :returns: server config info
        :rtype: :class:`marathon.models.info.MarathonInfo`
        """
        response = self._do_request('GET', '/v2/info')
        return self._parse_response(response, MarathonInfo)

    def ping(self):
        """Ping the Marathon server.

        :returns: the text response
        :rtype: str
        """
        response = self._do_request('GET', '/ping')
        return response.text

    def get_metrics(self):
        """Get server metrics

        :returns: metrics dict
        :rtype: dict
        """
        response = self._do_request('GET', '/metrics')
        return response.json()