#!/usr/bin/python

import optparse
import os
import os.path
import sys

from service_setup import config
from service_setup import paths
from service_setup import prompt
from service_setup.autosuggest import suggest_port, suggest_vip
from service_setup.template import Template


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

    def append_hostgroups(self, contents):
        for root, dirs, files in os.walk(self.paths.hostgroup):
            if root.endswith('hostgroups') and 'soa.cfg' in files:
                self._append(os.path.join(root, 'soa.cfg'), contents)

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
        """Note that this will raise IOError if 'path' does not exist."""
        with open(path, 'a') as f:
            contents = str(contents)
            f.write(contents)
            # Add trailing newline
            if not contents.endswith('\n'):
                f.write('\n')

def ask_srvname():
    srvname = None
    while not srvname:
        srvname = raw_input('Service name? ')
    return srvname

def ask_file_survey(srvname):
    """Surveys the user about the various entries in files/services/$srvname"""
    post_download = None
    post_activate = None
    runas = prompt.ask('Run as user?', 'batch')
    runasgroup = prompt.ask('Run as group?', runas)
    if prompt.yes_no('Load Balanced?'):
        vip = prompt.ask('VIP?', suggest_vip())
    else:
        vip = None
    port = prompt.ask('Port?', str(suggest_port()))
    status_port = prompt.ask('Status port?', str(int(port) + 1))

    if prompt.yes_no('Any post-download actions?'):
        post_download = prompt.ask(
            'Input post-download script',
            Template('post_download').substitute({'srvname': srvname}))

    if prompt.yes_no('Any post-activate actions?'):
        post_activate = prompt.ask(
            'Input post-activate script',
            Template('post_activate').substitute({'srvname': srvname}))
    return runas, runasgroup, port, status_port, vip, post_download, post_activate

def parse_args():
    parser = optparse.OptionParser()
    parser.add_option("-n", "--nagios-root", dest="nagios_root", default=None, help="Path to root of Nagios checkout")
    parser.add_option("-N", "--disable-nagios", dest="enable_nagios", default=True, action="store_false", help="Don't run steps related to Nagios")
    parser.add_option("-p", "--puppet-root", dest="puppet_root", default=None, help="Path to root of Puppet checkout")
    parser.add_option("-P", "--disable-puppet", dest="enable_puppet", default=True, action="store_false", help="Don't run steps related to Puppet")
    parser.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured")
    opts, args = parser.parse_args()

    validate_options(parser, opts)
    return opts, args

def validate_options(parser, opts):
    if opts.enable_puppet and not opts.puppet_root:
        print "ERROR: Puppet is enabled but puppet_root is not set!"
        parser.print_usage()
        sys.exit(1)

    if opts.enable_nagios and not opts.nagios_root:
        print "ERROR: Nagios is enabled but nagios_root is not set!"
        parser.print_usage()
        sys.exit(1)

def setup_config_paths(puppet_root, nagios_root):
    config.TEMPLATE_DIR = os.path.join(os.path.dirname(sys.argv[0]), 'templates')
    config.PUPPET_ROOT = puppet_root
    config.NAGIOS_ROOT = nagios_root


def do_puppet_steps(srv):
    runas, runasgroup, port, status_port, vip, post_download, post_activate = ask_file_survey(srv.name)
    srv.io.write_file('runas', runas)
    srv.io.write_file('runas_group', runasgroup)
    srv.io.write_file('port', port)
    srv.io.write_file('status_port', status_port)
    srv.io.write_file('post-download', post_download, executable=True)
    srv.io.write_file('post-activate', post_activate, executable=True)
    if vip is not None:
        srv.io.write_file('vip', vip)
        srv.io.write_file('lb.yaml', '')
        srv.io.write_healthcheck(
            Template('healthcheck').substitute(
                {'srvname': srv.name, 'port': port}))

def do_nagios_steps(srv):
    servicegroup_contents = Template('servicegroup').substitute(
        {'srvname': srv.name })
    srv.io.append_servicegroup(servicegroup_contents)

    hostgroup_contents = Template('hostgroup').substitute(
        {'srvname': srv.name })
    srv.io.append_hostgroups(hostgroup_contents)

def main(opts, args):
    setup_config_paths(opts.puppet_root, opts.nagios_root)

    srvname = opts.srvname
    if not srvname:
        srvname = ask_srvname()
    srv = Service(srvname)

    if opts.enable_puppet:
        do_puppet_steps(srv)

    if opts.enable_nagios:
        do_nagios_steps(srv)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)
