#!/usr/bin/env python
import re
from typing import Any
from typing import Callable
from typing import Dict

from paasta_tools.utils import paasta_print


ConstraintState = Dict[str, Dict[str, Any]]
ConstraintOp = Callable[[str, str, str, ConstraintState], bool]


def max_per(constraint_value, offer_value, attribute, state: ConstraintState):
    if not constraint_value:
        constraint_value = 1
    state_value = state.get('MAX_PER', {}).get(attribute, {}).get(offer_value, 0)
    return state_value <= int(constraint_value)


# lambda arg: [constraint value, offer value, attribute, state]
# example constraint: ['pool', 'MAX_PER', 5]
#   constraint value: 5
#   offer value:      default
#   attribute:        pool
#   state:            {'MAX_PER' => {'pool' => {'default' => 6}}}
CONS_OPS: Dict[str, ConstraintOp] = {
    'EQUALS': lambda cv, ov, *_: cv == ov,
    'LIKE': lambda cv, ov, *_: bool(re.match(cv, ov)),
    'UNLIKE': lambda cv, ov, *_: not(re.match(cv, ov)),
    'MAX_PER': max_per,
    'UNIQUE': max_per,
}


def nested_inc(op, _, attr_val, attr_name, state, step=1):
    """Increments relevant counter by step from args array"""
    oph = state.setdefault(op, {})
    nameh = oph.setdefault(attr_name, {})
    nameh.setdefault(attr_val, 0)
    nameh[attr_val] += step
    return state


# lambda args same as CONS_OPS + update step
UPDATE_OPS = {
    'EQUALS': lambda *_: None,
    'LIKE': lambda *_: None,
    'UNLIKE': lambda *_: None,
    'MAX_PER': lambda *args: nested_inc('MAX_PER', *args),
    'UNIQUE': lambda *args: nested_inc('MAX_PER', *args),
}


def check_offer_constraints(offer, constraints, state):
    """Returns True if all constraints are satisfied by offer's attributes,
    returns False otherwise. Prints a error message and re-raises if an error
    was thrown."""
    for (attr, op, val) in constraints:
        try:
            offer_attr = next(
                (x for x in offer.attributes if x.name == attr), None,
            )
            if offer_attr is None:
                paasta_print("Attribute not found for a constraint: %s" % attr)
                return False
            elif not(CONS_OPS[op](val, offer_attr.text.value, offer_attr.name, state)):
                paasta_print("Constraint not satisfied: [{} {} {}] for {} with {}".format(
                    attr, op, val, offer_attr.text.value, state,
                ))
                return False
        except Exception as err:
            paasta_print("Error while matching constraint: [{} {} {}] {}".format(
                attr, op, val, str(err),
            ))
            raise err

    return True


def update_constraint_state(offer, constraints, state, step=1):
    """Mutates state for each offer attribute found in constraints by calling
    relevant UPDATE_OP lambda"""
    for (attr, op, val) in constraints:
        for oa in offer.attributes:
            if attr == oa.name:
                UPDATE_OPS[op](val, oa.text.value, attr, state, step)
