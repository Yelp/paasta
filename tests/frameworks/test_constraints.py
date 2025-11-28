from unittest.mock import Mock

from paasta_tools.frameworks import constraints


def test_nested_inc_increments_by_step():
    op = "MAX_PER"
    av = "default"
    an = "pool"
    st: constraints.ConstraintState = {}
    constraints.nested_inc(op, None, av, an, st, 3)
    assert st["MAX_PER"]["pool"]["default"] == 3

    constraints.nested_inc(op, None, av, an, st, -1)
    assert st["MAX_PER"]["pool"]["default"] == 2


def test_check_offer_constraints_returns_true_when_satisfied():
    attr = Mock(text=Mock(value="test"))
    attr.configure_mock(name="pool")
    offer = Mock(attributes=[attr])
    cons = [
        ["pool", "MAX_PER", "5"],
        ["pool", "EQUALS", "test"],
        ["pool", "LIKE", "te.*$"],
        ["pool", "UNLIKE", "ta.*"],
    ]
    state = {"MAX_PER": {"pool": {"test": 0}}}
    assert constraints.check_offer_constraints(offer, cons, state) is True
    state = {"MAX_PER": {"pool": {"test": 6}}}
    assert constraints.check_offer_constraints(offer, cons, state) is False


def test_update_constraint_state_increments_counters():
    attr = Mock(text=Mock(value="test"))
    attr.configure_mock(name="pool")
    offer = Mock(attributes=[attr])
    cons = [["pool", "MAX_PER", "5"]]
    state: constraints.ConstraintState = {}
    constraints.update_constraint_state(offer, cons, state)
    assert state["MAX_PER"]["pool"]["test"] == 1
