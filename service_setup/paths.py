import os.path

from service_setup import config

SERVICE_FILES = os.path.join('modules', 'yelp_soa', 'files', 'services')
HEALTHCHECKS = os.path.join('files', 'healthcheck', 'nail', 'sys',
                            'healthcheck', '_healthcheck_services')
SERVICEGROUPS = os.path.join('etc', 'shared', 'prod-and-stage', 'servicegroups')
HOSTGROUPS = os.path.join('etc', 'datacenters')
CHECKS = os.path.join('etc', 'shared', 'prod-and-stage', 'services')

ALL_FILES = set([
    'runas', 'runas_group', 'port', 'status_port', 'lb.yaml', 'vip',
    'post-download', 'post-activate'])


class SrvPathBuilder(object):
    """Builds paths to files in the puppet heirarchy for a given service."""

    def __init__(self, srvname):
        self.srvname = srvname

    @property
    def root_dir(self):
        return os.path.join(config.PUPPET_ROOT, SERVICE_FILES, self.srvname)

    def to_file(self, filename):
        """Return a filename under this service's directory"""
        return os.path.join(self.root_dir, filename)

    @property
    def healthcheck(self):
        """Path to the healthcheck for this service"""
        return os.path.join(
            config.PUPPET_ROOT, HEALTHCHECKS, self.srvname + '.py')

    @property
    def servicegroup(self):
        """Path to the servicegroup for this service"""
        return os.path.join(
            config.NAGIOS_ROOT, SERVICEGROUPS, 'soa.cfg')

    @property
    def hostgroup(self):
        """Path to the servicegroup for this service"""
        return os.path.join(
            config.NAGIOS_ROOT, HOSTGROUPS)

    @property
    def check(self):
        """Path to the check for this service"""
        return os.path.join(
            config.NAGIOS_ROOT, CHECKS, self.srvname + '.cfg')
