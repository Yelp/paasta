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

    def append_hostgroups(self, contents, ecosystem=None, vip=False):
        filename = 'soa.cfg'
        if vip:
            filename = 'vips.cfg'
        for root, dirs, files in os.walk(self.paths.hostgroup):
            if root.endswith('hostgroups') and filename in files:
                # If 'ecosystem' was not specified, append to all files. If
                # 'ecosystem' was specified, only append to files in that
                # ecosystem.
                print ecosystem, root
                if ecosystem is None or ("/%s/" % ecosystem) in root:
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


