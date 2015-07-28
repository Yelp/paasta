import chronos
import json
import logging
import os
import urlparse

from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR


# In Marathon spaces are not allowed, in Chronos periods are not allowed.
# In the Chronos docs a space is suggested as the natural separator
SPACER = " "
PATH_TO_CHRONOS_CONFIG = os.path.join(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, 'chronos.json')
log = logging.getLogger('__main__')


class ChronosNotConfigured(Exception):
    pass


class ChronosConfig(dict):

    def __init__(self, config, path):
        self.path = path
        super(ChronosConfig, self).__init__(config)

    def get_url(self):
        """:returns: The Chronos API endpoint"""
        try:
            return self['url']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos url in system chronos config: %s' % self.path)

    def get_username(self):
        """:returns: The Chronos API username"""
        try:
            return self['user']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos user in system chronos config: %s' % self.path)

    def get_password(self):
        """:returns: The Chronos API password"""
        try:
            return self['password']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos password in system chronos config: %s' % self.path)


def load_chronos_config(path=PATH_TO_CHRONOS_CONFIG):
    try:
        with open(path) as f:
            return ChronosConfig(json.load(f), path)
    except IOError as e:
        raise ChronosNotConfigured("Could not load chronos config file %s: %s" % (e.filename, e.strerror))


def get_chronos_client(config):
    """Returns a chronos client object for interacting with the API"""
    chronos_url = config.get_url()[0]
    chronos_hostname = urlparse.urlsplit(chronos_url).netloc
    log.info("Connecting to Chronos server at: %s", chronos_url)
    return chronos.connect(hostname=chronos_hostname,
                           username=config.get_username(),
                           password=config.get_password())


def get_job_id(service, instance):
    return "%s%s%s" % (service, SPACER, instance)
