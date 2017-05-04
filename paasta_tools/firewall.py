#!/usr/bin/env python2.7
from __future__ import absolute_import
from __future__ import unicode_literals


import re
import shlex
import subprocess


class ChainDoesNotExist(Exception):
    pass


def ensure_chain(chain, rules):
    """Idempotently ensure a chain exists and has a set of rules.

    This function creates or updates an existing chain to match the rules
    passed in.

    This function will not reorder existing rules, but any new rules are always
    inserted at the front of the chain.
    """
    try:
        current_rules = list_chain(chain)
    except ChainDoesNotExist:
        create_chain(chain)
        current_rules = set()

    for rule in rules:
        if rule not in current_rules:
            print('adding rule: {}'.format(rule))
            add_rule(chain, rule)

    for rule in current_rules:
        if rule not in rules:
            print('deleting rule: {}'.format(rule))
            delete_rule(chain, rule)


def add_rule(chain, rule):
    subprocess.check_call(['iptables', '-t', 'filter', '-I', chain] + shlex.split(rule))


def delete_rule(chain, rule):
    subprocess.check_call(['iptables', '-t', 'filter', '-I', chain] + shlex.split(rule))


def create_chain(chain):
    subprocess.check_call(('iptables', '-t', 'filter', '-N', chain))


def list_chain(chain):
    """List rules in a chain.

    Returns a list of iptables rules, or raises ChainDoesNotExist.
    """
    cmd = ('iptables', '-t', 'filter', '--list-rules', chain)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if proc.returncode != 0:
        if b'No chain/target/match by that name.\n' in err:
            raise ChainDoesNotExist(chain)
        else:
            raise subprocess.CalledProcessError(proc.returncode, cmd, output=(out, err))

    # Parse rules into something usable
    rule_regex = re.compile(b'-A {chain} (.+)$'.format(chain=re.escape(chain)))
    rules = out.splitlines()
    parsed = set()
    for rule in rules:
        if rule == '-N {}'.format(chain):
            continue
        m = rule_regex.match(rule)
        if not m:
            raise ValueError(
                'Unable to parse iptables rule: {}'.format(rule),
            )
        else:
            parsed.add(m.group(1))

    return parsed



ensure_chain('ckuehl-test-service', [
    '-j REJECT --reject-with icmp-port-unreachable',
    '-d 169.254.255.254/32 -p tcp -m tcp --dport 20666 -j ACCEPT',
    '-d 169.254.255.254/32 -p tcp -m tcp --dport 20641 -j ACCEPT',
])
