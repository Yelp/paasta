import logging
from urllib.parse import urljoin

import requests

from paasta_tools.utils import get_user_agent


log = logging.getLogger(__name__)


class TronClient:

    def __init__(self, url):
        self.master_url = url

    def _request(self, method, url, data):
        headers = {'User-Agent': get_user_agent()}
        kwargs = {
            'url': urljoin(self.master_url, url),
            'headers': headers,
        }
        if method == 'GET':
            kwargs['params'] = data
            response = requests.get(**kwargs)
        elif method == 'POST':
            kwargs['data'] = data
            response = requests.post(**kwargs)
        else:
            raise ValueError('Unrecognized method: {}'.format(method))

        # Raise an exception if unsuccessful.
        response.raise_for_status()
        return response.json()

    def _get(self, url, data=None):
        return self._request('GET', url, data)

    def _post(self, url, data=None):
        return self._request('POST', url, data)

    def update_namespace(self, namespace, new_config, skip_if_unchanged=True):
        current_config = self._get('/api/config', {'name': namespace})
        if skip_if_unchanged and new_config == current_config['config']:
            log.info('No change in config, skipping update.')
            return

        self._post(
            '/api/config',
            data={
                'name': namespace,
                'config': new_config,
                'hash': current_config['hash'],
                'check': 0,
            },
        )

    def list_namespaces(self):
        response = self._get('/api')
        return response.get('namespaces', [])
