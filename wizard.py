#!/usr/bin/python

import optparse
import os
import socket
import sys

from service_wizard import config
from service_wizard import prompt
from service_wizard.autosuggest import suggest_port
from service_wizard.autosuggest import suggest_runs_on
from service_wizard.autosuggest import suggest_vip
from service_wizard.questions import _yamlize
from service_wizard.questions import get_srvname
from service_wizard.questions import get_smartstack_stanza
from service_wizard.service import Service
from service_wizard.template import Template


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
    suggested_runs_on = suggest_runs_on(runs_on)
    if runs_on is None:
        runs_on = prompt.ask(
            'Machines to run on (comma-separated short hostnames)?',
            suggested_runs_on,
        )
    else:
        runs_on = suggested_runs_on

    parsed_runs_on = parse_hostnames_string(runs_on)
    return parsed_runs_on

def ask_vip(vip=None):
    default = suggest_vip()
    if vip == "AUTO":
        vip = default
    elif vip is None:
        if prompt.yes_no('Load Balanced via VIP?'):
            while not vip:
                vip = prompt.ask('VIP?', default)
        else:
            vip = None
    return vip

def ask_smartstack():
    return prompt.yes_no('Load Balanced via SmartStack?')

def ask_lbs(yelpsoa_config_root, vip, smartstack_only):
    if smartstack_only == "AUTO":
        smartstack_only = False

    if smartstack_only:
        vip = None
        use_smartstack = True
    else:
        vip = ask_vip(vip)

        if vip:
            # If vip is configured, we also use SmartStack by default.
            use_smartstack = True
        else:
            use_smartstack = ask_smartstack()

    if use_smartstack:
        smartstack = get_smartstack_stanza(yelpsoa_config_root, True, None, legacy_style=True)
    else:
        smartstack = None

    return vip, smartstack

def get_fqdn(hostname):
    # socket.getfqdn on an empty string returns localhost, which is not what we
    # want. Just give it back and let the caller worry about it.
    if not hostname:
        return hostname
    fqdn = socket.getfqdn(hostname)
    if fqdn == hostname and not hostname.endswith(".com"): # It's dot com.
        print "WARNING: getfqdn returned %s for itself, which implies a DNS miss *unless* it's already an fqdn. Typo?" % hostname
    return fqdn

def parse_hostnames_string(hostnames_string):
    """Given a string of comma-separated hostnames (either passed in or
    received by prompting the user), return a list containing the FQDN for each
    hostname.
    """
    orig_hostnames = [h.strip() for h in hostnames_string.split(",")]
    non_blank_hostnames = [h for h in orig_hostnames if len(h) > 0]
    fqdn_hostnames = [get_fqdn(h) for h in non_blank_hostnames]
    return fqdn_hostnames

def get_service_yaml_contents(runs_on, deploys_on, smartstack):
    """Given 'runs_on' and 'deploys_on' lists, and a 'smartstack' dictionary,
    return yaml appropriate for writing into service.yaml.
    """
    contents = {
        "runs_on": runs_on,
        "deployed_to": deploys_on,
    }
    if smartstack is not None:
        contents.update(smartstack)
    return _yamlize(contents)

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
        print "Machines to deploy on - machines that download the service code but don't run an instance of it."
        print "This is useful e.g. for services which want to run batch jobs on a batch machine."
        deploys_on = prompt.ask(
            'Machines to deploy on (comma-separated short hostnames)?',
            default_deploys_on,
        )
    parsed_deploys_on = parse_hostnames_string(deploys_on)

    return status_port, runas, runas_group, post_download, post_activate, parsed_deploys_on

def parse_args():
    parser = optparse.OptionParser()
    group = optparse.OptionGroup(parser, "Configuring this script")
    group.add_option("-p", "--puppet-root", dest="puppet_root", default=None, help="Path to root of Puppet checkout")
    group.add_option("-P", "--disable-puppet", dest="enable_puppet", default=True, action="store_false", help="Don't run steps related to Puppet")
    group.add_option("-y", "--yelpsoa-config-root", dest="yelpsoa_config_root", default=None, help="Path to root of yelpsoa-configs checkout")
    group.add_option("-Y", "--disable-yelpsoa-config", dest="enable_yelpsoa_config", default=True, action="store_false", help="Don't run steps related to yelpsoa-configs")
    group.add_option("-A", "--auto", dest="auto", default=False, action="store_true", help="Use defaults instead of prompting when default value is available")
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, "General configuration for the service being added. User will be prompted for anything left unspecified")
    group.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured")
    group.add_option("-o", "--port", dest="port", default=None, help="Port used by service. If AUTO, use default (calculated from yelpsoa-configs)")
    group.add_option("-t", "--status-port", dest="status_port", default=None, help="Status port used by service. If AUTO, use default (calculated from yelpsoa-configs)")
    group.add_option("-v", "--vip", dest="vip", default=None, help="VIP used by service (e.g. 'vip1'). If AUTO, use default (caluclated from yelpsoa-configs)")
    group.add_option(
        "-m",
        "--smartstack-only",
        dest="smartstack_only",
        default=False,
        action="store_true",
        help="Service will be load-balanced by SmartStack alone. Cannot be used when vip is specified. If AUTO, use default (%default)."
    )
    group.add_option("-r", "--runas", dest="runas", default=None, help="UNIX user which will run service. If AUTO, use default (consult the code)")
    group.add_option("-R", "--runas-group", dest="runas_group", default=None, help="UNIX group which will run service. If AUTO, use default (consult the code)")
    group.add_option("-d", "--post-download", dest="post_download", default=None, help="Script executed after service is downloaded by target machine. (Probably easier to do this by hand if the script is complex.) Can be NONE for an empty template or AUTO for the default (python) template.")
    group.add_option("-a", "--post-activate", dest="post_activate", default=None, help="Script executed after service is activated by target machine. (Probably easier to do this by hand if the script is complex.) Can be NONE for an empty template or AUTO for the default (python) template.")
    group.add_option("-S", "--runs-on", dest="runs_on", default=None, help="Comma-separated list of machines where the service runs. You can use shortnames and I will translate to FQDN as needed. Can be empty string ('') for no machines, or AUTO for the default set. AUTO requires --yelpsoa-config-root.")
    group.add_option("-D", "--deploys-on", dest="deploys_on", default=None, help="Comma-separated list of machines where the service is deployed but the init script isn't run. This is useful e.g. for running batches. You can use shortnames and I will translated to FQDN as needed. Can be empty string ('') for no machines, or AUTO for the default set. AUTO requires --yelpsoa-config-root.")
    parser.add_option_group(group)

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args

def validate_options(parser, opts):
    """Does sys.exit() if an invalid combination of options is specified.
    Otherwise returns None (implicitly)."""

    if opts.enable_yelpsoa_config:
        if not opts.yelpsoa_config_root:
            parser.print_usage()
            sys.exit("ERROR: yelpsoa-configs is enabled but --yelpsoa-config-root is not set!")
        if not os.path.exists(opts.yelpsoa_config_root):
            parser.print_usage()
            sys.exit("ERROR: --yelpsoa-config-root %s does not exist!" % opts.yelpsoa_config_root)

    if opts.enable_puppet:
        if not opts.puppet_root:
            parser.print_usage()
            sys.exit("ERROR: Puppet is enabled but --puppet-root is not set!")
        if not os.path.exists(opts.puppet_root):
            parser.print_usage()
            sys.exit("ERROR: --puppet-root %s does not exist!" % opts.puppet_root)

    if not opts.yelpsoa_config_root and not opts.port:
        parser.print_usage()
        sys.exit("ERROR: Must provide either --yelpsoa-config-root or --port!")

    # There's a weird dependency here because --smartstack-only requires
    # --yelpsoa-config-root but is itself exclusive with --vip. So we must do
    # --smartstack-only checks before the --vip check.
    if not opts.yelpsoa_config_root and opts.smartstack_only:
        parser.print_usage()
        sys.exit("ERROR: --smartstack-only requires --yelpsoa-config-root!")
    if opts.smartstack_only and opts.vip:
        parser.print_usage()
        sys.exit("ERROR: --smartstack-only cannot be used with --vip.")
    if not opts.yelpsoa_config_root and not opts.vip:
        parser.print_usage()
        sys.exit("ERROR: Must provide either --yelpsoa-config-root or --vip!")

    if opts.vip and not (opts.vip.startswith("vip") or opts.vip == "AUTO"):
        parser.print_usage()
        sys.exit("ERROR: --vip must start with 'vip'!")

def setup_config_paths(yelpsoa_config_root, puppet_root):
    config.TEMPLATE_DIR = os.path.join(os.path.dirname(sys.argv[0]), 'templates')
    assert os.path.exists(config.TEMPLATE_DIR)
    # config.YELPSOA_CONFIG_ROOT is deprecated! Don't add it to new things!
    # Just pass the value to your functions explicitly!
    config.YELPSOA_CONFIG_ROOT = yelpsoa_config_root
    config.PUPPET_ROOT = puppet_root

def do_yelpsoa_config_steps(srv, port, status_port, vip, runas, runas_group, post_download, post_activate, runs_on, deploys_on, smartstack):
    srv.io.write_file('runas', runas)
    srv.io.write_file('runas_group', runas_group)
    srv.io.write_file('port', port)
    srv.io.write_file('status_port', status_port)
    srv.io.write_file('post-download', post_download, executable=True)
    srv.io.write_file('post-activate', post_activate, executable=True)
    service_yaml_contents = get_service_yaml_contents(runs_on, deploys_on, smartstack)
    srv.io.write_file('service.yaml', service_yaml_contents)
    if vip is not None:
        srv.io.write_file('vip', vip)
        srv.io.write_file('lb.yaml', '')

def do_puppet_steps(srv, port, vip):
    if vip is not None:
        srv.io.write_healthcheck(
            Template('healthcheck').substitute(
                {'srvname': srv.name, 'port': port}))


def main(opts, args):
    setup_config_paths(opts.yelpsoa_config_root, opts.puppet_root)

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
        opts.smartstack_only = opts.smartstack_only or "AUTO"

    srvname = get_srvname(opts.srvname, opts.auto)
    srv = Service(srvname, opts.yelpsoa_config_root)

    port = ask_port(opts.port)

    vip, smartstack = ask_lbs(opts.yelpsoa_config_root, opts.vip, opts.smartstack_only)

    runs_on = ask_runs_on(opts.runs_on)

    # Ask all the questions (and do all the validation) first so we don't have to bail out and undo later.
    if opts.enable_yelpsoa_config:
        status_port, runas, runas_group, post_download, post_activate, deploys_on = ask_yelpsoa_config_questions(srv.name, port, opts.status_port, opts.runas, opts.runas_group, opts.post_download, opts.post_activate, opts.deploys_on)

    if opts.enable_yelpsoa_config:
        do_yelpsoa_config_steps(srv, port, status_port, vip, runas, runas_group, post_download, post_activate, runs_on, deploys_on, smartstack)
    if opts.enable_puppet:
        do_puppet_steps(srv, port, vip)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)

# vim: set expandtab tabstop=4 sts=4 shiftwidth=4:
