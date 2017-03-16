#!/usr/bin/env python
# Without this, the import of mesos.interface breaks because paasta_tools.mesos exists
from __future__ import absolute_import
from __future__ import unicode_literals

import re

from paasta_tools.utils import paasta_print

# lambda args: [constraint value, offer value, attribute, state]
CONS_OPS = {
    'EQUALS': lambda pair: pair[0] == pair[1],
    'LIKE': lambda pair: re.match(pair[0], pair[1]),
    'UNLIKE': lambda pair: not(re.match(pair[0], pair[1])),

    # example: ['pool', 'MAX_PER', 5]
    #   constraint value: 5
    #   offer value:      default
    #   attribute:        pool
    #   state:            {'MAX_PER' => {'pool' => {'default' => 6}}}
    'MAX_PER': lambda quad: quad[3]['MAX_PER'][quad[2]][quad[1]] <= quad[0],
    'UNIQUE': lambda quad: quad[3]['UNIQUE'][quad[2]][quad[1]] <= 1,
}


def nested_inc(op, args):
    """Increments relevant counter by step from args array"""
    _, attr_val, attr_name, state, step = args
    oph = state.setdefault(op, {})
    nameh = oph.setdefault(attr_name, {})
    nameh.setdefault(attr_val, 0)
    nameh[attr_val] += step
    return state


# lambda args same as CONS_OPS + update step
UPDATE_OPS = {
    'EQUALS': lambda _: None,
    'LIKE': lambda _: None,
    'UNLIKE': lambda _: None,
    'MAX_PER': lambda x: nested_inc('MAX_PER', x),
    'UNIQUE': lambda x: nested_inc('UNIQUE', x),
}


def test_offer_constraints(offer, constraints, state):
    """Returns True if all constraints are satisfied by offer's attributes,
    returns False otherwise. Prints a error message and re-raises if an error
    was thrown."""
    for (attr, op, val) in constraints:
        try:
            offer_attr = next(
                (x for x in offer.attributes if x.name == attr), None)
            if offer_attr is None:
                paasta_print("Attribute not found for a constraint: %s" % attr)
                return False
            elif not(CONS_OPS[op]([val, offer_attr.text.value,
                                   offer_attr.name, state])):
                paasta_print("Constraint not satisfied: [%s %s %s for %s]" % (
                    attr, op, val, offer_attr.text.value, state))
                return False
        except Exception as err:
            paasta_print("Error while mathing constraint: [%s %s %s] %s" % (
                attr, op, val, str(err)))
            raise err

    return True


def update_constraint_state(offer, constraints, state, step=1):
    """Mutates state for each offer attribute found in constraints by calling
    relevant UPDATE_OP lambda"""
    for (attr, op, val) in constraints:
        for oa in offer.attributes:
            if attr == oa.name:
                UPDATE_OPS[op]([val, oa.text.value, attr, state, step])
