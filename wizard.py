#!/usr/bin/python

import optparse
import os
import socket
import sys

import yaml

from service_wizard import config
from service_wizard import prompt
from service_wizard.autosuggest import suggest_port
from service_wizard.autosuggest import suggest_runs_on
from service_wizard.autosuggest import suggest_vip
from service_wizard.service import Service
from service_wizard.service_configuration import collate_hosts_by_habitat
from service_wizard.template import Template


def ask_srvname(srvname=None):
    if srvname is None:
        while not srvname:
            srvname = raw_input('Service name? ')
    return srvname

def ask_port(port=None):
    # Don't bother calculating (doing so is non-trivial) if we don't have to.
    if port and port != "AUTO":
        return port

    default = str(suggest_port())
    if port == "AUTO":
        port = default
    elif port is None:
        while not port:
            port = prompt.ask('Port?', default)
    return port

def ask_status_port(port, status_port=None):
    default = str(int(port) + 1)
    if status_port == "AUTO":
        status_port = default
    elif status_port is None:
        while not status_port:
            status_port = prompt.ask('Status port?', default)
    return status_port

def ask_runs_on(runs_on=None):
    # Don't bother calculating (doing so is non-trivial) if we don't have to.
    if runs_on and runs_on != "AUTO":
        return runs_on

    default_runs_on = suggest_runs_on()
    if runs_on == "AUTO":
        runs_on = default_runs_on
        ###### suggest_dev_hosts()
        ### suggest_stage_hosts()
        ### suggest_prod_hosts()
    elif runs_on is None:
        runs_on = prompt.ask(
            'Machines to run on (comma-separated short hostnames)?',
            default_runs_on,
        )

    parsed_runs_on = parse_hostnames_string(runs_on)
    return parsed_runs_on

def ask_vip(vip=None):
    default = suggest_vip()
    if vip == "AUTO":
        vip = default
    elif vip is None:
        if prompt.yes_no('Load Balanced?'):
            while not vip:
                vip = prompt.ask('VIP?', default)
        else:
            vip = None
    return vip

def get_fqdn(hostname):
    # socket.getfqdn on an empty string returns localhost, which is not what we
    # want. Just give it back and let the caller worry about it.
    if not hostname:
        return hostname
    fqdn = socket.getfqdn(hostname)
    if fqdn == hostname:
        print "WARNING: getfqdn returned %s for itself, which implies a DNS miss *unless* it's already an fqdn. Typo?" % hostname
    return fqdn

def parse_hostnames_string(hostnames_string):
    """Given a comma-separated list of hostnames (either passed in or received
    by prompting the user), return a list containing the FQDN for each
    hostname.
    """
    orig_hostnames = [h.strip() for h in hostnames_string.split(",")]
    fqdn_hostnames = [get_fqdn(h) for h in orig_hostnames]
    return fqdn_hostnames

def get_service_yaml_contents(runs_on, deploys_on):
    """Given lists 'runs_on' and 'deploys_on', return yaml appropriate for
    writing into service.yaml.
    """
    contents = {
        "runs_on": runs_on,
        "deployed_to": deploys_on,
    }
    return yaml.dump(contents, explicit_start=True, default_flow_style=False)

def get_habitat_overrides(host_by_habitat, srvname):
    habitat_overrides = {}
    for (habitat, members_list) in host_by_habitat.items():
        members_string = ",".join(sorted(members_list))
        contents = Template('hostgroup_with_members').substitute(
                {'srvname': srvname, 'members': members_string})
        habitat_overrides[habitat] = contents
    return habitat_overrides

def ask_yelpsoa_config_questions(srvname, port, status_port, runas, runas_group, post_download, post_activate, deploys_on):
    """Surveys the user about the various entries in files/services/$srvname"""
    status_port = ask_status_port(port, status_port)

    default_runas = "batch"
    if runas == "AUTO":
        runas = default_runas
    if runas is None:
        runas = prompt.ask('Run as user?', default_runas)

    default_runas_group = runas
    if runas_group == "AUTO":
        runas_group = default_runas_group
    if runas_group is None:
        runas_group = prompt.ask('Run as group?', default_runas_group)

    default_post_download = Template('post_download').substitute({'srvname': srvname})
    if post_download == "NONE":
        post_download = ""
    elif post_download == "AUTO":
        post_download = default_post_download
    elif post_download is None:
        if prompt.yes_no('Any post-download actions?'):
            post_download = prompt.ask(
                'Input post-download script',
                default_post_download,
            )

    default_post_activate = Template('post_activate').substitute({'srvname': srvname})
    if post_activate == "NONE":
        post_activate = ""
    elif post_activate == "AUTO":
        post_activate = default_post_activate
    if post_activate is None:
        if prompt.yes_no('Any post-activate actions?'):
            post_activate = prompt.ask(
                'Input post-activate script',
                default_post_activate,
            )

    default_deploys_on = ''
    if deploys_on == "AUTO":
        deploys_on = default_deploys_on
    elif deploys_on is None:
        deploys_on = prompt.ask(
            'Machines to deploy on (comma-separated short hostnames)?',
            default_deploys_on,
        )
    parsed_deploys_on = parse_hostnames_string(deploys_on)

    return status_port, runas, runas_group, post_download, post_activate, parsed_deploys_on

def ask_nagios_questions(contact_groups=None, contacts=None, include_ops=None):
    if not contact_groups and not contacts:
        contact_groups = prompt.ask('Nagios contact_groups (comma-separated list)?')
        contacts = prompt.ask('Nagios contacts (individuals, comma-separated list)?')
    if include_ops is None:
        include_ops = prompt.yes_no('Nagios alerts ops?')

    if not contact_groups and not contacts and not include_ops:
        print "ERROR: No contact_groups or contacts provided and Operations on-call is not alerted."
        print "Must provide someone to be alerted!"
        sys.exit(2)

    return contact_groups, contacts, include_ops

def parse_args():
    parser = optparse.OptionParser()
    group = optparse.OptionGroup(parser, "Configuring this script")
    group.add_option("-n", "--nagios-root", dest="nagios_root", default=None, help="Path to root of Nagios checkout")
    group.add_option("-N", "--disable-nagios", dest="enable_nagios", default=True, action="store_false", help="Don't run steps related to Nagios")
    group.add_option("-p", "--puppet-root", dest="puppet_root", default=None, help="Path to root of Puppet checkout")
    group.add_option("-P", "--disable-puppet", dest="enable_puppet", default=True, action="store_false", help="Don't run steps related to Puppet")
    group.add_option("-y", "--yelpsoa-config-root", dest="yelpsoa_config_root", default=None, help="Path to root of yelpsoa-configs checkout")
    group.add_option("-Y", "--disable-yelpsoa-config", dest="enable_yelpsoa_config", default=True, action="store_false", help="Don't run steps related to yelpsoa-configs")
    group.add_option("-A", "--auto", dest="auto", default=False, action="store_true", help="Use defaults instead of prompting when default value is available")
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, "General configuration for the service being added. User will be prompted for anything left unspecified")
    group.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured")
    group.add_option("-o", "--port", dest="port", default=None, help="Port used by service. If AUTO, use default")
    group.add_option("-t", "--status-port", dest="status_port", default=None, help="Status port used by service. If AUTO, use default")
    group.add_option("-v", "--vip", dest="vip", default=None, help="VIP used by service (e.g. 'vip1'). If AUTO, use default")
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, "Nagios configuration for the service being added. User will be prompted for anything left unspecified")
    group.add_option("-C", "--contact-groups", dest="contact_groups", default=None, help="Comma-separated list of Nagios groups to alert")
    group.add_option("-c", "--contacts", dest="contacts", default=None, help="Comma-separated list of individuals to alert. If either --contacts or --contact-groups specified, user will not be prompted for either option.")
    group.add_option("-i", "--include-ops", dest="include_ops", default=None, action="store_true", help="Operations on-call shall be alerted")
    group.add_option("-x", "--exclude-ops", dest="exclude_ops", default=None, action="store_true", help="Operations on-call shall NOT be alerted. If neither --include-ops nor --exclude-ops specified, user will be prompted.")
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, "Puppet configuration for the service being added. User will be prompted for anything left unspecified")
    group.add_option("-r", "--runas", dest="runas", default=None, help="UNIX user which will run service. If AUTO, use default")
    group.add_option("-R", "--runas-group", dest="runas_group", default=None, help="UNIX group which will run service. If AUTO, use default")
    group.add_option("-d", "--post-download", dest="post_download", default=None, help="Script executed after service is downloaded by target machine. (Probably easier to do this by hand if the script is complex.) Can be NONE for an empty template or AUTO for the default (python) template.")
    group.add_option("-a", "--post-activate", dest="post_activate", default=None, help="Script executed after service is activated by target machine. (Probably easier to do this by hand if the script is complex.) Can be NONE for an empty template or AUTO for the default (python) template.")
    group.add_option("-S", "--runs-on", dest="runs_on", default=None, help="Comma-separated list of machines where the service runs. You can use shortnames appropriate and I will translated to FQDN as needed. Can be empty string ('') for no machines; or AUTO for the default set. AUTO requires --puppet-root.")
    group.add_option("-D", "--deploys-on", dest="deploys_on", default=None, help="Comma-separated list of machines where the service runs. You can use shortnames appropriate and I will translated to FQDN as needed. Can be empty string ('') for no machines; or AUTO for the default set.")
    parser.add_option_group(group)

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args

def validate_options(parser, opts):
    """Does sys.exit() if an invalid combination of options is specified.
    Otherwise returns None (implicitly)."""

    if opts.enable_yelpsoa_config:
        if not opts.yelpsoa_config_root:
            print "ERROR: yelpsoa-configs is enabled but --yelpsoa-config-root is not set!"
            parser.print_usage()
            sys.exit(1)
        if not os.path.exists(opts.yelpsoa_config_root):
            print "ERROR: --yelpsoa-config-root %s does not exist!" % opts.yelpsoa_config_root
            parser.print_usage()
            sys.exit(1)

    if opts.enable_puppet:
        if not opts.puppet_root:
            print "ERROR: Puppet is enabled but --puppet-root is not set!"
            parser.print_usage()
            sys.exit(1)
        if not os.path.exists(opts.puppet_root):
            print "ERROR: --puppet-root %s does not exist!" % opts.puppet_root
            parser.print_usage()
            sys.exit(1)

    if opts.enable_nagios:
        if not opts.nagios_root:
            print "ERROR: Nagios is enabled but --nagios-root is not set!"
            parser.print_usage()
            sys.exit(1)
        if not os.path.exists(opts.nagios_root):
            print "ERROR: --nagios-root %s does not exist!" % opts.nagios_root
            parser.print_usage()
            sys.exit(1)

    if not opts.yelpsoa_config_root and not opts.port:
        print "ERROR: Must provide either --yelpsoa-config-root or --port!"
        parser.print_usage()
        sys.exit(1)

    if not opts.yelpsoa_config_root and not opts.vip:
        print "ERROR: Must provide either --yelpsoa-config-root or --vip!"
        parser.print_usage()
        sys.exit(1)

    if opts.vip and not (opts.vip.startswith("vip") or opts.vip == "AUTO"):
        print "ERROR: --vip must start with 'vip'!"
        parser.print_usage()
        sys.exit(1)

    if opts.include_ops and opts.exclude_ops:
        print "ERROR: Provide only one of --include-ops and --exclude-ops"
        parser.print_usage()
        sys.exit(1)
    else:
        if opts.exclude_ops:
            opts.include_ops = False

def setup_config_paths(yelpsoa_config_root, puppet_root, nagios_root):
    config.TEMPLATE_DIR = os.path.join(os.path.dirname(sys.argv[0]), 'templates')
    assert os.path.exists(config.TEMPLATE_DIR)
    config.YELPSOA_CONFIG_ROOT = yelpsoa_config_root
    config.PUPPET_ROOT = puppet_root
    config.NAGIOS_ROOT = nagios_root

def do_yelpsoa_config_steps(srv, port, status_port, vip, runas, runas_group, post_download, post_activate, runs_on, deploys_on):
    srv.io.write_file('runas', runas)
    srv.io.write_file('runas_group', runas_group)
    srv.io.write_file('port', port)
    srv.io.write_file('status_port', status_port)
    srv.io.write_file('post-download', post_download, executable=True)
    srv.io.write_file('post-activate', post_activate, executable=True)
    service_yaml_contents = get_service_yaml_contents(runs_on, deploys_on)
    srv.io.write_file('service.yaml', service_yaml_contents)
    if vip is not None:
        srv.io.write_file('vip', vip)
        srv.io.write_file('lb.yaml', '')

def do_puppet_steps(srv, port, vip):
    if vip is not None:
        srv.io.write_healthcheck(
            Template('healthcheck').substitute(
                {'srvname': srv.name, 'port': port}))

def do_nagios_steps(srv, port, vip, contact_groups, contacts, include_ops, runs_on):
    servicegroup_contents = Template('servicegroup').substitute(
        {'srvname': srv.name })
    srv.io.append_servicegroup(servicegroup_contents)

    default_contents = Template('hostgroup_empty_members').substitute(
                {'srvname': srv.name})
    host_by_habitat = collate_hosts_by_habitat(runs_on)
    habitat_overrides = get_habitat_overrides(host_by_habitat, srv.name)
    srv.io.append_hostgroups(default_contents, habitat_overrides=habitat_overrides)

    check_contents = Template('check').substitute({
        'srvname': srv.name,
        'port': port,
        'contact_groups': contact_groups or '',
        'contacts': contacts or '',
        'plus_if_include_ops': '+' if include_ops else '',
    })
    srv.io.write_check(check_contents)

    if vip:
        vip_hostgroup_contents = Template('vip_hostgroup').substitute(
            {'srvname': srv.name })
        srv.io.append_hostgroups(vip_hostgroup_contents, vip=True)

        check_contents = Template('vip_check').substitute({
            'srvname': srv.name,
            'port': port,
            'contact_groups': contact_groups or '',
            'contacts': contacts or '',
            'plus_if_include_ops': '+' if include_ops else '',
        })
        srv.io.append_check(check_contents)


def main(opts, args):
    setup_config_paths(opts.yelpsoa_config_root, opts.puppet_root, opts.nagios_root)

    if opts.auto:
        opts.port = opts.port or "AUTO"
        opts.status_port = opts.status_port or "AUTO"
        opts.vip = opts.vip or "AUTO"
        opts.runas = opts.runas or "AUTO"
        opts.runas_group = opts.runas_group or "AUTO"
        opts.post_download = opts.post_download or "AUTO"
        opts.post_activate = opts.post_activate or "AUTO"
        opts.runs_on = opts.runs_on or "AUTO"
        opts.deploys_on = opts.deploys_on or "AUTO"

    srvname = ask_srvname(opts.srvname)
    srv = Service(srvname)

    port = ask_port(opts.port)
    vip = ask_vip(opts.vip)
    runs_on = ask_runs_on(opts.runs_on)

    # Ask all the questions (and do all the validation) first so we don't have to bail out and undo later.
    if opts.enable_yelpsoa_config:
        status_port, runas, runas_group, post_download, post_activate, deploys_on = ask_yelpsoa_config_questions(srv.name, port, opts.status_port, opts.runas, opts.runas_group, opts.post_download, opts.post_activate, opts.deploys_on)
    if opts.enable_nagios:
        contact_groups, contacts, include_ops = ask_nagios_questions(opts.contact_groups, opts.contacts, opts.include_ops)

    if opts.enable_yelpsoa_config:
        do_yelpsoa_config_steps(srv, port, status_port, vip, runas, runas_group, post_download, post_activate, runs_on, deploys_on)
    if opts.enable_puppet:
        do_puppet_steps(srv, port, vip)
    if opts.enable_nagios:
        do_nagios_steps(srv, port, vip, contact_groups, contacts, include_ops, runs_on)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)

# vim: set expandtab tabstop=4 sts=4 shiftwidth=4:
