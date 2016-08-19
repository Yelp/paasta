# -*- coding: utf8 -*-
""" Interface for working with qdiscs """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import struct

import pyroute2
from plumbum.cmd import grep
from plumbum.cmd import iptables
from plumbum.cmd import tc
from pyroute2 import IPRoute
from pyroute2.iproute import transform_handle
from pyroute2.netlink import NLM_F_ACK
from pyroute2.netlink import NLM_F_REQUEST
from pyroute2.netlink.rtnl.tcmsg import tcmsg


log = logging.getLogger(__name__)


def stat(interface_name):
    """ Show status of existing qdisc and iptables rules """
    for tc_type in ('qdisc', 'class', 'filter'):
        print('=' * 20 + ' tc {0} '.format(tc_type) + '=' * 20)
        print(tc['-s', tc_type, 'show', 'dev', interface_name]())

    print('=' * 20 + ' iptables rules ' + '=' * 20)
    print(iptables['-L', '-t', 'mangle']())
    return 0


def check_setup(interface_name):
    """ Checks the existing qdisc and iptables rules """
    tc_cmd = tc['-s', 'qdisc', 'show', 'dev', interface_name]
    tc_grep_cmd = grep['qdisc']
    tc_chain = (tc_cmd | tc_grep_cmd)

    iptables_cmd = iptables['-L', '-t', 'mangle']
    iptables_grep_cmd = grep['MARK xset 0x' + IPTABLES_MARK]
    iptables_chain = (iptables_cmd | iptables_grep_cmd)

    tc_run = tc_chain.run(retcode=None)
    iptables_run = iptables_chain.run(retcode=None)

    if tc_run[0] != 0 or iptables_run[0] != 0:
        log.info('No existing setup for {0}'.format(interface_name))
        return 1

    tc_result = tc_run[1].count('\n')
    iptables_result = iptables_run[1].count('\n')

    if tc_result == 5 and iptables_result > 0:
        log.info('Expected setup exists for {0}'.format(interface_name))
    else:
        log.error('An unexpected setup exists for {0}'.format(interface_name))
        return 2
    return 0


def needs_setup(interface_name):
    """ Checks if there are no existing qdisc and iptables rules """
    check_result = check_setup(interface_name)
    if check_result == 0:
        return 1
    return 0


# Create a traffic control setup as follows
#            1: root qdisc
#          /-|-\--------\
#         /  |  \        |
#        /   |   \       |
#      1:1  1:2  1:3    1:4  classes
#       |    |    |      |
#      10:  20:  30:    40:  qdiscs
#    pfifo pfifo pfifo  plug
# band  0    1    2      4

# This in combination with an iptables rule allows us to
# redirect SYN packets to the plug during a restart of a
# process sensitivew to that (e.g. haproxy), and then
# unplug later

ROOT = '1:'
PRIO_CLASS_FASTEST = '1:1'
PRIO_CLASS_FAST = '1:2'
PRIO_CLASS_SLOW = '1:3'
PLUG_CLASS = '1:4'
PRIO_QDISC_FASTEST = '10:'
PRIO_QDISC_FAST = '20:'
PRIO_QDISC_SLOW = '30:'
PLUG_QDISC = '40:'
IPTABLES_MARK = '1'


def _apply_tc_rules(interface_name):
    log.info('Creating prio qdisc with a plug lane for {0}'.format(
        interface_name))
    tc['qdisc', 'add', 'dev', interface_name,
       'root', 'handle', ROOT, 'prio', 'bands', '4']()
    tc['qdisc', 'add', 'dev', interface_name,
       'parent', PRIO_CLASS_FASTEST,
       'handle', PRIO_QDISC_FASTEST,
       'pfifo', 'limit', '1000']()
    tc['qdisc', 'add', 'dev', interface_name,
       'parent', PRIO_CLASS_FAST,
       'handle', PRIO_QDISC_FAST,
       'pfifo', 'limit', '1000']()
    tc['qdisc', 'add', 'dev', interface_name,
       'parent', PRIO_CLASS_SLOW,
       'handle', PRIO_QDISC_SLOW,
       'pfifo', 'limit', '1000']()
    try:
        tc['qdisc', 'add', 'dev', interface_name,
           'parent', PLUG_CLASS,
           'handle', PLUG_QDISC, 'plug']()
    except:
        # If we can't create a plug because of an older
        # kernel, just make a fifo
        tc['qdisc', 'add', 'dev', interface_name,
           'parent', PLUG_CLASS,
           'handle', PLUG_QDISC,
           'pfifo', 'limit', '1000']()

    tc['filter', 'add', 'dev', interface_name,
       'protocol', 'ip', 'parent', ROOT, 'prio', '1',
       'handle', IPTABLES_MARK, 'fw', 'classid', PLUG_CLASS]()
    # Ensure the device is unplugged by default
    manage_plug(interface_name, enable_plug=False)


def _apply_iptables_rule(source_ip):
    log.info('Creating iptables rule to mark outgoing syns on {0}'.format(
        source_ip))
    iptables[
        '-t', 'mangle', '-I', 'OUTPUT', '-p', 'tcp',
        '-s', source_ip, '--syn', '-j', 'MARK', '--set-mark', IPTABLES_MARK
    ]()


def setup(interface_name, source_ip):
    """ Sets up qdisc and iptables rules on the provided devices

    This effectively creates a normal prio qdisc with an extra plug lane.
    The plug lane always gets traffic based on iptables marks.
    The extra lane can be plugged or unplugged using manage_plug.
    """
    status = check_setup(interface_name)
    if status != 0:
        log.info('Clearing any existing config before attempting setup')
        clear(interface_name, source_ip)
    else:
        log.info('Doing nothing')
        return 0
    _apply_tc_rules(interface_name)
    _apply_iptables_rule(source_ip)
    return 0


def clear(interface_name, source_ip):
    try:
        tc['qdisc', 'del', 'dev', interface_name, 'root']()
    except:
        pass

    # Ensure all iptables rules are purged on the output device
    while True:
        try:
            iptables[
                '-t', 'mangle', '-D', 'OUTPUT', '-p', 'tcp',
                '-s', source_ip, '--syn', '-j',
                'MARK', '--set-mark', IPTABLES_MARK
            ]()
        except:
            break


def _manage_plug_via_netlink(interface_name, action='unplug'):
    """ Manipulates the plug qdisc via netlink

    FIXME: Once we have a modern userpace, replace this with appropriate
    calls to nl-qdisc-add
    """
    ip = IPRoute()
    index = ip.link_lookup(ifname=interface_name)[0]
    # See the linux source at include/uapi/linux/pkt_sched.h
    # #define TCQ_PLUG_BUFFER                0
    # #define TCQ_PLUG_RELEASE_ONE           1
    # #define TCQ_PLUG_RELEASE_INDEFINITE    2
    # #define TCQ_PLUG_LIMIT                 3
    action = {'unplug': 2, 'plug': 0}[action]
    packet_limit = 10000
    handle = transform_handle(PLUG_QDISC)
    parent = transform_handle(PLUG_CLASS)
    flags = NLM_F_REQUEST | NLM_F_ACK
    command = pyroute2.netlink.rtnl.RTM_NEWQDISC
    # This is a bit of magic sauce, inspired by xen's remus project
    opts = struct.pack('iI', action, packet_limit)

    msg = tcmsg()
    msg['index'] = index
    msg['handle'] = handle
    msg['parent'] = parent
    msg['attrs'] = [['TCA_KIND', 'plug']]
    msg['attrs'].append(['TCA_OPTIONS', opts])
    try:
        nlm_response = ip.nlm_request(msg, msg_type=command, msg_flags=flags)
    except pyroute2.netlink.NetlinkError as nle:
        if nle.code == 22:
            # This is an old kernel and we're talking to a qfifo, chill
            log.warn('Detected a non plug qdisc, likely due to an old kernel. '
                     'If you wish to have zero downtime haproxy restarts, '
                     'upgrade your kernel. '
                     'Doing nothing to the SYN traffic lane...')
            return
        else:
            raise

    # As per the netlink manpage (man 7 netlink), we expect an
    # acknowledgment as a NLMSG_ERROR packet with the error field being 0,
    # which it looks like pyroute2 treats as None. Really we want it to be
    # non negative.
    if not(len(nlm_response) > 0 and
           nlm_response[0]['event'] == 'NLMSG_ERROR' and
           nlm_response[0]['header']['error'] is None):
        raise RuntimeError(
            'Had an error while communicating with netlink: {0}'.format(
                nlm_response))


def manage_plug(interface, enable_plug):
    """ Enable or disable traffic flowing through the plug lane

    Note that when enable_plug is True, traffic is queued, and when
    enable_plug is False, traffic flows normally.
    """
    if enable_plug:
        log.info('Plugging traffic on the plug lane ...')
        _manage_plug_via_netlink(interface, 'plug')
        log.info('Done.')
    else:
        log.info('Unplugging traffic on the plug lane ...')
        _manage_plug_via_netlink(interface, 'unplug')
        log.info('Done.')
    return 0
