import os.path


class SrvPathBuilder(object):
    """Builds paths to files in different repos needed for service
    configuration.
    """

    def __init__(self, srvname, yelpsoa_config_root):
        self.srvname = srvname
        self.yelpsoa_config_root = yelpsoa_config_root

    @property
    def root_dir(self):
        return os.path.join(self.yelpsoa_config_root, self.srvname)

    def to_file(self, filename):
        """Return a filename under this service's directory"""
        return os.path.join(self.root_dir, filename)

    @property
    def service_yaml(self):
        """Path to the service.yaml for this service"""
        return os.path.join(
            self.root_dir, self.srvname + '.yaml')
