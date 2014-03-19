import os

from service_wizard import paths


class Service(object):

    @classmethod
    def from_files(cls, srvname):
        srv = cls(srvname)
        for f in paths.ALL_FILES:
            setattr(srv, f.replace('-','_').replace('.','_'),
                    srv.io.read_file(f))
        return srv

    def __init__(self, name):
        self.name = name
        self.paths = paths.SrvPathBuilder(name)
        self.io = SrvReaderWriter(self.paths)

class SrvReaderWriter(object):

    def __init__(self, path_builder):
        self.paths = path_builder

    def read_file(self, filename):
        return self._read(self.paths.to_file(filename))

    def write_file(self, filename, contents, executable=False):
        if not os.path.exists(self.paths.root_dir):
            os.makedirs(self.paths.root_dir)
        self._write(self.paths.to_file(filename),
                    contents,
                    executable=executable)

    def read_healthcheck(self):
        return self._read(self.paths.to_file)

    def write_healthcheck(self, contents):
        self._write(self.paths.healthcheck, contents, executable=True)

    def append_servicegroup(self, contents):
        self._append(self.paths.servicegroup, contents)

    def append_hostgroups(self, default_contents, habitat_overrides=None, vip=False):
        """Append a provided Nagios stanza to relevant hostgroups files.

        If 'vip' is provided and truthy, append to vips.cfg instead of soa.cfg.

        If 'habitat' is not provided, 'default_contents' will be appended to
        the appropriate hostgroups file. If 'habitat' is provided, it is a
        dictionary where the key is a habitat and the value is the contents
        to be written into the hostgroups file for that habitat.

        Example: append_hostgroups("foo") -> append "foo" to all hostgroups files
        Example: append_hostgroups("foo", {"stagea": bar}) ->
                 append "bar" to the stagea hostgroups file and append "foo" to
                 all other hostgroups files
        """
        if habitat_overrides is None:
            habitat_overrides = {}

        filename = 'soa.cfg'
        if vip:
            filename = 'vips.cfg'

        for habitat in os.listdir(self.paths.hostgroup):
            for root, dirs, files in os.walk(os.path.join(self.paths.hostgroup, habitat)):
                if root.endswith('hostgroups') and filename in files:
                    # We found a file we want to modify. Let's figure out which
                    # contents to put in it.
                    if habitat in habitat_overrides.keys():
                        contents = habitat_overrides[habitat]
                    else:
                        contents = default_contents
                    self._append(os.path.join(root, filename), contents)

    def write_check(self, contents):
        self._write(self.paths.check, contents)

    def append_check(self, contents):
        self._append(self.paths.check, contents)

    def write_service_yaml(self, contents):
        self._write(self.paths.service_yaml, contents)

    def _read(self, path):
        if not os.path.exists(path):
            return ''
        with open(path, 'r') as f:
            return f.read()

    def _write(self, path, contents, executable=False):
        with open(path, 'w') as f:
            if executable and not contents:
                f.write('# Do nothing\n')
            else:
                contents = str(contents)
                f.write(contents)
                # Add trailing newline
                if not contents.endswith('\n'):
                    f.write('\n')
        if executable:
            os.chmod(path, 0755)

    def _append(self, path, contents):
        with open(path, 'a') as f:
            contents = str(contents)
            f.write(contents)
            # Add trailing newline
            if not contents.endswith('\n'):
                f.write('\n')


