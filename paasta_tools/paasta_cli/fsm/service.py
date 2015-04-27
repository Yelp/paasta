import os

from service_wizard import paths


class Service(object):

    def __init__(self, name, yelpsoa_config_root):
        self.name = name
        self.paths = paths.SrvPathBuilder(name, yelpsoa_config_root)
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

    def symlink_file_relative(self, source_filename, link_filename):
        """Create a symlink in our service root from source_filename to
        link_filename. This is a relative symlink so that
        /nail/home/troscoe/yelpsoa-configs/ isn't baked in.
        """
        if not os.path.exists(self.paths.root_dir):
            os.makedirs(self.paths.root_dir)
        os.symlink(
            source_filename,
            self.paths.to_file(link_filename),
        )

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
