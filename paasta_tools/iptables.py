"""iptables helper functions.

Unlike the `firewall` module, these functions know nothing about PaaSTA and
could effectively be a third-party library. They just make working with
iptables a little bit easier.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import re
import shlex
import subprocess


class ChainDoesNotExist(Exception):
    pass


def all_chains():
    output = subprocess.check_output(
        ('iptables', '-t', 'filter', '--list-rules'),
    )
    chain_regex = re.compile('-N ([^ ]+)$')
    chains = set()
    for line in output.splitlines():
        m = chain_regex.match(line)
        if m:
            chains.add(m.group(1))
    return chains


def ensure_chain(chain, rules=None):
    """Idempotently ensure a chain exists and has an exact set of rules.

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

    if rules is not None:
        for rule in rules:
            if rule not in current_rules:
                add_rule(chain, rule)

        for rule in current_rules:
            if rule not in rules:
                delete_rule(chain, rule)


def ensure_rule(chain, rule):
    rules = list_chain(chain)
    if rule not in rules:
        add_rule(chain, rule)


def add_rule(chain, rule):
    print('adding rule to {}: {}'.format(chain, rule))
    subprocess.check_call(['iptables', '-t', 'filter', '-I', chain] + shlex.split(rule))


def delete_rule(chain, rule):
    print('deleting rule from {}: {}'.format(chain, rule))
    subprocess.check_call(['iptables', '-t', 'filter', '-D', chain] + shlex.split(rule))


def create_chain(chain):
    print('creating chain: {}'.format(chain))
    subprocess.check_call(('iptables', '-t', 'filter', '-N', chain))


def delete_chain(chain):
    print('deleting chain: {}'.format(chain))
    # remove rules from the chain
    subprocess.check_call(('iptables', '-t', 'filter', '-F', chain))
    # delete the chain
    subprocess.check_call(('iptables', '-t', 'filter', '-X', chain))


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
        m = rule_regex.match(rule)
        if m:
            parsed.add(m.group(1))

    return parsed
