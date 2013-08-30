#!/usr/bin/python

import optparse
import os
from os import path
import sys

from service_setup import config
from service_setup import prompt
from service_setup.autosuggest import suggest_port, suggest_vip
from service_setup.template import Template

class ServiceBuilder(object):

    def __init__(self, srvname):
        self.srvname = srvname

    @property
    def srvroot(self):
        return path.join(config.PUPPET_SRV_ROOT, self.srvname)

    def write_file(self, filename, contents, executable=False):
        if not path.exists(self.srvroot):
            os.makedirs(self.srvroot)
        filename = path.join(self.srvroot, filename)
        with open(filename, 'w') as f:
            if executable and not contents:
                f.write('# Do nothing\n')
            else:
                contents = str(contents)
                f.write(contents)
                # Add trailing newline
                if not contents.endswith('\n'):
                    f.write('\n')
        if executable:
            os.chmod(filename, 0755)

def ask_file_survey():
    """Surveys the user about the various entries in files/services/$srvname"""
    srvname = None
    post_download = None
    post_activate = None
    while not srvname:
        srvname = raw_input('Service name? ')
    runas = prompt.ask('Run as user?', 'batch')
    runasgroup = prompt.ask('Run as group?', runas)
    if prompt.yes_no('Load Balanced?'):
        vip = prompt.ask('VIP?', suggest_vip())
    else:
        vip = None
    port = prompt.ask('Port?', str(suggest_port()))
    status_port = prompt.ask('Status port?', str(int(port) + 1))

    if prompt.yes_no('Any post-download actions?'):
        post_download = prompt.ask('Input post-download script',
                                Template('post_download').substitute({'srvname': srvname}))

    if prompt.yes_no('Any post-activate actions?'):
        post_activate = prompt.ask('Input post-activate script',
                                Template('post_activate').substitute({'srvname': srvname}))
    return srvname, runas, runasgroup, port, status_port, vip, post_download, post_activate

def main(opts, args):
    setup_config_paths(args[0])

    srvname, runas, runasgroup, port, status_port, vip, post_download, post_activate = ask_file_survey()
    bldr = ServiceBuilder(srvname)
    bldr.write_file('runas', runas)
    bldr.write_file('runas_group', runasgroup)
    bldr.write_file('port', port)
    bldr.write_file('status_port', status_port)
    bldr.write_file('post-download', post_download, executable=True)
    bldr.write_file('post-activate', post_activate, executable=True)
    if vip is not None:
        bldr.write_file('vip', vip)
        bldr.write_file('lb.yaml', '')

def parse_args():
    parser = optparse.OptionParser(
        usage="%prog PUPPET_PATH"
    )
    opts, args = parser.parse_args()
    if len(args) != 1:
        parser.print_usage()
        sys.exit(1)
    return opts, args

def setup_config_paths(puppet_root):
    config.PUPPET_ROOT = puppet_root
    config.PUPPET_SRV_ROOT = path.join(config.PUPPET_ROOT, 'files', 'services')
    config.TEMPLATE_DIR = path.join(path.dirname(sys.argv[0]), 'templates')
    config.HEALTHCHECK_DIR = path.join(
        config.PUPPET_ROOT, 'files', 'healthcheck', 'nail',
        'sys', 'healthcheck', '_healthcheck_services')

if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)
