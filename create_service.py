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

    def write_check(self, contents):
        self._write(self.paths.check, contents)

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

def ask_srvname():
    srvname = None
    while not srvname:
        srvname = raw_input('Service name? ')
    return srvname

def ask_port():
    port = prompt.ask('Port?', str(suggest_port()))
    return port

def ask_puppet_questions(srvname, port):
    """Surveys the user about the various entries in files/services/$srvname"""
    post_download = None
    post_activate = None
    runas = prompt.ask('Run as user?', 'batch')
    runasgroup = prompt.ask('Run as group?', runas)
    if prompt.yes_no('Load Balanced?'):
        vip = prompt.ask('VIP?', suggest_vip())
    else:
        vip = None
    status_port = prompt.ask('Status port?', str(int(port) + 1))

    if prompt.yes_no('Any post-download actions?'):
        post_download = prompt.ask(
            'Input post-download script',
            Template('post_download').substitute({'srvname': srvname}))

    if prompt.yes_no('Any post-activate actions?'):
        post_activate = prompt.ask(
            'Input post-activate script',
            Template('post_activate').substitute({'srvname': srvname}))
    return runas, runasgroup, status_port, vip, post_download, post_activate

def ask_nagios_quetsions(contact_groups=None, contacts=None, include_ops=None):
    if not contact_groups and not contacts:
        contact_groups = prompt.ask('Nagios contact_groups (comma-separated list)?')
        contacts = prompt.ask('Nagios contacts (individuals, comma-separated list)?')
    if include_ops is None:
        include_ops = prompt.yes_no('Nagios alerts ops?')
    return contact_groups, contacts, include_ops

def parse_args():
    parser = optparse.OptionParser()
    group = optparse.OptionGroup(parser, "Configuring this script")
    group.add_option("-n", "--nagios-root", dest="nagios_root", default=None, help="Path to root of Nagios checkout")
    group.add_option("-N", "--disable-nagios", dest="enable_nagios", default=True, action="store_false", help="Don't run steps related to Nagios")
    group.add_option("-p", "--puppet-root", dest="puppet_root", default=None, help="Path to root of Puppet checkout")
    group.add_option("-P", "--disable-puppet", dest="enable_puppet", default=True, action="store_false", help="Don't run steps related to Puppet")
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, "Configuring the service being added. User will be prompted for ")
    group.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured")
    group.add_option("-o", "--port", dest="port", default=None, help="Port used by service")
    ###group.add_option("-t", "--status-port", dest="status_port", default=None, help="Status port used by service")
    group.add_option("-g", "--contact-groups", dest="contact_groups", default=None, help="Comma-separated list of Nagios groups to alert")
    group.add_option("-c", "--contacts", dest="contacts", default=None, help="Comma-separated list of individuals to alert. If --contacts or --contact-groups specified, user will not be prompted for either option.")
    group.add_option("-i", "--include-ops", dest="include_ops", default=None, action="store_true", help="Operations on-call shall be alerted")
    group.add_option("-x", "--exclude-ops", dest="exclude_ops", default=None, action="store_true", help="Operations on-call shall NOT be alerted. If neither --include-ops nor --exclude-ops specified, user will be prompted.")
    parser.add_option_group(group)

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args

def validate_options(parser, opts):
    if opts.enable_puppet and not opts.puppet_root:
        print "ERROR: Puppet is enabled but --puppet-root is not set!"
        parser.print_usage()
        sys.exit(1)

    if opts.enable_nagios and not opts.nagios_root:
        print "ERROR: Nagios is enabled but --nagios-root is not set!"
        parser.print_usage()
        sys.exit(1)

    if not opts.puppet_root and not opts.port:
        print "ERROR: Must provide either --puppet-root or --port!"
        parser.print_usage()
        sys.exit(1)

    if opts.include_ops and opts.exclude_ops:
        print "ERROR: Provide only one of --include-ops and --exclude-ops"
        parser.print_usage()
        sys.exit(1)
    else:
        if opts.exclude_ops:
            opts.include_ops = False

def setup_config_paths(puppet_root, nagios_root):
    config.TEMPLATE_DIR = os.path.join(os.path.dirname(sys.argv[0]), 'templates')
    config.PUPPET_ROOT = puppet_root
    config.NAGIOS_ROOT = nagios_root

def do_puppet_steps(srv, port):
    runas, runasgroup, status_port, vip, post_download, post_activate = ask_puppet_questions(srv.name, port)
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

def do_nagios_steps(srv, port, contact_groups=None, contacts=None, include_ops=None):
    contact_groups, contacts, include_ops = ask_nagios_quetsions(contact_groups, contacts, include_ops)
    if not contact_groups and not contacts and not include_ops:
        print "ERROR: No contact_groups or contacts provided and Operations on-call is not alerted."
        print "Must provide someone to be alerted!"
        sys.exit(2)

    servicegroup_contents = Template('servicegroup').substitute(
        {'srvname': srv.name })
    srv.io.append_servicegroup(servicegroup_contents)

    hostgroup_contents = Template('hostgroup').substitute(
        {'srvname': srv.name })
    srv.io.append_hostgroups(hostgroup_contents)

    ### vip hostgroup

    ### contact_groups and/or contacts
    ### replace ops or not (+)
    check_contents = Template('check').substitute({
        'srvname': srv.name,
        'port': port,
    })
    srv.io.write_check(check_contents)


def main(opts, args):
    setup_config_paths(opts.puppet_root, opts.nagios_root)

    srvname = opts.srvname
    if not srvname:
        srvname = ask_srvname()
    srv = Service(srvname)

    port = opts.port
    if not port:
        port = ask_port()

    if opts.enable_puppet:
        do_puppet_steps(srv, port)

    if opts.enable_nagios:
        do_nagios_steps(srv, port, opts.contact_groups, opts.contacts, opts.include_ops)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)
