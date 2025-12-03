"""iptables helper functions.

Unlike the `firewall` module, these functions know nothing about PaaSTA and
could effectively be a third-party library. They just make working with
iptables a little bit easier.
"""
import collections
import contextlib
import logging

import iptc

log = logging.getLogger(__name__)


RULE_TARGET_SORT_ORDER = {
    # all else defaults to '0'
    "LOG": 1,
    "REJECT": 2.0,
}


_RuleBase = collections.namedtuple(
    "_RuleBase", ("protocol", "src", "dst", "target", "matches", "target_parameters")
)


class Rule(_RuleBase):
    """Rule representation.

    Working with iptc's rule classes directly doesn't work well, since rules
    represent actual existing iptables rules, and changes are applied
    immediately. They're also difficult to compare.
    """

    def __new__(cls, *args, **kwargs):
        result = _RuleBase.__new__(cls, *args, **kwargs)
        result.validate()
        return result

    def _replace(self, **kwargs):
        result = super()._replace(**kwargs)
        result.validate()
        return result

    def validate(self):
        if self.target == "REJECT":
            assert any(
                name == "reject-with" for name, _ in self.target_parameters
            ), "REJECT rules must specify reject-with"
        assert tuple(sorted(self.matches)) == self.matches, "matches should be sorted"
        for match_name, params in self.matches:
            for param_name, param_value in params:
                assert (
                    "_" not in param_name
                ), f"use dashes instead of underscores in {param_name}"
                assert isinstance(
                    param_value, tuple
                ), f"value of {param_name} should be tuple"
        assert (
            tuple(sorted(self.target_parameters)) == self.target_parameters
        ), "target_parameters should be sorted"
        for param_name, param_value in self.target_parameters:
            assert (
                "_" not in param_name
            ), f"use dashes instead of underscores in {param_name}"
            assert isinstance(
                param_value, tuple
            ), f"value of {param_name} should be tuple"

    @classmethod
    def from_iptc(cls, rule):
        fields = {
            "protocol": rule.protocol,
            "src": rule.src,
            "dst": rule.dst,
            "target": rule.target.name,
            "matches": (),
            "target_parameters": (),
        }

        for param_name, param_value in sorted(rule.target.get_all_parameters().items()):
            fields["target_parameters"] += ((param_name, tuple(param_value)),)

        matches = []
        for match in rule.matches:
            matches.append(
                (
                    match.name,
                    tuple(
                        (param, tuple(value))
                        for param, value in sorted(match.get_all_parameters().items())
                    ),
                )
            )
        # ensure that matches are sorted for consistency with matching
        fields["matches"] = tuple(sorted(matches))

        return cls(**fields)

    def to_iptc(self):
        rule = iptc.Rule()
        rule.protocol = self.protocol
        rule.src = self.src
        rule.dst = self.dst
        target = rule.create_target(self.target)
        for param_name, param_value in self.target_parameters:
            target.set_parameter(param_name, param_value)
        for name, params in self.matches:
            match = rule.create_match(name)
            for param_name, param_value in params:
                match.set_parameter(param_name, param_value)
        return rule


@contextlib.contextmanager
def iptables_txn(table):
    """Temporarily disable autocommit and commit at the end.

    If an exception occurs, changes are rolled back.

    By default, changes to iptables rules are applied immediately. In some
    cases, we want to avoid that.

    https://github.com/ldx/python-iptables#autocommit
    """
    assert table.autocommit is True, table.autocommit
    try:
        table.autocommit = False
        yield
        table.commit()
    finally:
        table.refresh()
        table.autocommit = True


class ChainDoesNotExist(Exception):
    pass


def all_chains():
    return {chain.name for chain in iptc.Table(iptc.Table.FILTER).chains}


def ensure_chain(chain, rules):
    """Idempotently ensure a chain exists and has an exact set of rules.

    This function creates or updates an existing chain to match the rules
    passed in.

    This function will not reorder existing rules, but any new rules are always
    inserted at the front of the chain.
    """
    try:
        current_rules = set(list_chain(chain))
    except ChainDoesNotExist:
        create_chain(chain)
        current_rules = set()

    for rule in rules:
        if rule not in current_rules:
            insert_rule(chain, rule)

    extra_rules = current_rules - set(rules)
    if extra_rules:
        delete_rules(chain, extra_rules)


def _rule_sort_key(rule_tuple):
    old_index, rule = rule_tuple
    target_name = rule.target
    return (RULE_TARGET_SORT_ORDER.get(target_name, 0), old_index)


def reorder_chain(chain_name):
    """Ensure that any REJECT rules are last, and any LOG rules are second-to-last"""

    table = iptc.Table(iptc.Table.FILTER)
    with iptables_txn(table):
        rules = list_chain(chain_name)
        chain = iptc.Chain(table, chain_name)

        # sort the rules by rule_key, which uses (RULE_TARGET_SORT_ORDER, idx)
        sorted_rules_with_indices = sorted(enumerate(rules), key=_rule_sort_key)

        for new_index, (old_index, rule) in enumerate(sorted_rules_with_indices):
            if new_index == old_index:
                continue
            log.debug(f"reordering chain {chain_name} rule {rule} to #{new_index}")
            chain.replace_rule(rule.to_iptc(), new_index)


def ensure_rule(chain, rule):
    rules = list_chain(chain)
    if rule not in rules:
        insert_rule(chain, rule)


def insert_rule(chain_name, rule):
    log.debug(f"adding rule to {chain_name}: {rule}")
    chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), chain_name)
    chain.insert_rule(rule.to_iptc())


def delete_rules(chain_name, rules):
    log.debug(f"deleting rules from {chain_name}: {rules}")
    table = iptc.Table(iptc.Table.FILTER)
    with iptables_txn(table):
        chain = iptc.Chain(table, chain_name)
        for potential_rule in chain.rules:
            if Rule.from_iptc(potential_rule) in rules:
                chain.delete_rule(potential_rule)


def create_chain(chain_name):
    log.debug(f"creating chain: {chain_name}")
    iptc.Table(iptc.Table.FILTER).create_chain(chain_name)


def delete_chain(chain_name):
    log.debug(f"deleting chain: {chain_name}")
    chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), chain_name)
    chain.flush()
    chain.delete()


def list_chain(chain_name):
    """List rules in a chain.

    Returns a list of iptables rules, or raises ChainDoesNotExist.
    """
    table = iptc.Table(iptc.Table.FILTER)
    chain = iptc.Chain(table, chain_name)
    # TODO: is there any way to do this without listing all chains? (probably slow)
    # If the chain doesn't exist, chain.rules will be an empty list, so we need
    # to make sure the chain actually _does_ exist.
    if chain in table.chains:
        return tuple(Rule.from_iptc(rule) for rule in chain.rules)
    else:
        raise ChainDoesNotExist(chain_name)
