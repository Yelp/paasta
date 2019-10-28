import operator
from datetime import timedelta
from functools import lru_cache

import arrow
import mock
import pytest

from clusterman.math.piecewise import hour_transform
from clusterman.math.piecewise import piecewise_max
from clusterman.math.piecewise import PiecewiseConstantFunction


def sorteddict_values_assert(sdict, values):
    assert list(sdict.values()) == values


def test_construct_function(fn):
    fn.add_delta(2, 2)
    fn.add_delta(3, 1)
    fn.add_delta(1, -3)

    assert fn.call(0) == 1
    assert fn.call(1) == -2
    assert fn.call(2) == 0
    assert fn.call(3) == 1
    assert fn.call(4) == 1


@pytest.mark.parametrize('squash', [True, False])
def test_add_duplicate_bp_values(fn, squash):
    fn.add_breakpoint(2, 2, squash=squash)
    fn.add_breakpoint(3, 2, squash=squash)
    assert fn.call(0) == 1
    assert fn.call(2) == 2
    assert fn.call(2.5) == 2
    assert fn.call(3) == 2
    assert fn.call(4) == 2
    assert len(fn.breakpoints) == 1 if squash else 2


def test_values_no_points_1(fn):
    sorteddict_values_assert(fn.values(0, 10.5, 1), [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])


def test_values_no_points_2(fn):
    sorteddict_values_assert(fn.values(0, 11, 1), [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])


def test_values_one_point(fn):
    fn.add_delta(2.5, 1)
    fn.add_delta(15, 1)
    sorteddict_values_assert(fn.values(0, 10.5, 1), [1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2])
    sorteddict_values_assert(fn.values(0, 11, 1), [1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2])


def test_values_two_points(fn):
    fn.add_delta(2.5, 1)
    fn.add_delta(5, 1)
    sorteddict_values_assert(fn.values(0, 10.5, 1), [1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3])
    sorteddict_values_assert(fn.values(0, 11, 1), [1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3])


def test_integrals_no_points(fn):
    sorteddict_values_assert(fn.integrals(0, 10.5, 1), [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0.5])


def test_integrals_whole_range(fn):
    sorteddict_values_assert(fn.integrals(0, 2, 2), [2])


def test_integrals_one_point(fn):
    fn.add_delta(2, 2)
    fn.add_delta(15, 2)
    sorteddict_values_assert(fn.integrals(0, 10.5, 1), [1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 1.5])
    sorteddict_values_assert(fn.integrals(0, 11, 1), [1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3])
    sorteddict_values_assert(fn.integrals(0.5, 11, 1), [1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 1.5])


def test_integrals_two_point(fn):
    fn.add_delta(2, 2)
    fn.add_delta(10.25, -2)
    sorteddict_values_assert(fn.integrals(0, 10.5, 1), [1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 1])
    sorteddict_values_assert(fn.integrals(0, 11, 1), [1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 1.5])
    sorteddict_values_assert(fn.integrals(0.5, 11, 1), [1, 2, 3, 3, 3, 3, 3, 3, 3, 2.5, 0.5])


def test_integrals_multi_points(fn):
    fn.add_delta(1.5, 2)
    fn.add_delta(2, -1)
    fn.add_delta(3, -1)
    fn.add_delta(4, 2)
    fn.add_delta(7, -1)
    sorteddict_values_assert(fn.integrals(0, 10.5, 3), [5, 7, 7, 3])


def test_integrals_with_timedeltas(fn):
    for i in range(10):
        fn.add_delta(arrow.get(i * 60), 1)
    x = fn.integrals(arrow.get(0), arrow.get(10 * 60), timedelta(seconds=60), transform=hour_transform)
    sorteddict_values_assert(x, pytest.approx([60 / 3600 * i for i in range(2, 12)]))


@pytest.mark.parametrize('initial_value', (0, 1))
def test_constant_integral(initial_value):
    fn = PiecewiseConstantFunction(initial_value)
    assert fn.integral(0, 1) == initial_value


@pytest.mark.parametrize('xval,result', ((0, 2), (1, 1)))
def test_one_step_integral(xval, result):
    fn = PiecewiseConstantFunction()
    fn.add_delta(xval, 1)
    assert fn.integral(0, 2) == result
    assert fn.integral(3, 4) == 1
    assert fn.integral(-2, -1) == 0


def test_one_step_integral_two_changes():
    fn = PiecewiseConstantFunction()
    fn.add_delta(1, 10)
    fn.add_delta(1, -5)
    assert fn.integral(0, 2) == 5


def test_multistep_integral(fn):
    fn.add_delta(1, 19)
    fn.add_delta(2, -10)
    fn.add_delta(2, 5)
    fn.add_delta(3, -10)
    sorteddict_values_assert(fn.integrals(0, 4, 4), [41])
    assert fn.integral(0, 4) == 41
    assert fn.integral(2, 3) == 15


def test_cache(fn):
    inside_values_func = mock.Mock()
    fn.values = lru_cache()(inside_values_func)
    fn.values(0, 10, 1)
    fn.values(0, 10, 1)
    fn.values(0, 10, 1)
    fn.values(0, 10, 1)
    fn.values(0, 10, 1)
    assert inside_values_func.call_count == 1


def test_cache_invalidated(fn):
    inside_values_func = mock.Mock()
    fn.values = lru_cache()(inside_values_func)
    fn.values(0, 10, 1)
    fn.values(0, 10, 1)
    fn.add_delta(2, 2)
    fn.values(0, 10, 1)
    fn.values(0, 10, 1)
    assert inside_values_func.call_count == 2


@pytest.mark.parametrize('op', [operator.add, operator.sub, operator.mul, operator.truediv])
def test_combine_no_breakpoints(op):
    fn1 = PiecewiseConstantFunction(1)
    fn2 = PiecewiseConstantFunction(2)
    fn3 = op(fn1, fn2)
    assert fn3._initial_value == op(fn1._initial_value, fn2._initial_value)
    assert len(fn3.breakpoints) == 0


@pytest.mark.parametrize('op', [operator.add, operator.sub, operator.mul, operator.truediv])
def test_combine_with_breakpoints_in_one_fn(op):
    fn1 = PiecewiseConstantFunction(1)
    fn1.add_breakpoint(2, 7)
    fn1.add_breakpoint(4, 4)
    fn1.add_breakpoint(7, 1)
    fn2 = PiecewiseConstantFunction(2)
    fn3 = op(fn1, fn2)

    assert fn3._initial_value == op(fn1._initial_value, fn2._initial_value)
    assert fn3.breakpoints[2] == op(fn1.breakpoints[2], fn2._initial_value)
    assert fn3.breakpoints[4] == op(fn1.breakpoints[4], fn2._initial_value)
    assert fn3.breakpoints[7] == op(fn1.breakpoints[7], fn2._initial_value)


@pytest.mark.parametrize('op', [operator.add, operator.sub, operator.mul, operator.truediv])
def test_combine_with_breakpoints_in_both_fns(op):
    fn1 = PiecewiseConstantFunction(1)
    fn1.add_breakpoint(2, 7)
    fn1.add_breakpoint(4, 4)
    fn1.add_breakpoint(7, 1)
    fn2 = PiecewiseConstantFunction(2)
    fn2.add_breakpoint(-1, 4)
    fn2.add_breakpoint(7, 1)
    fn2.add_breakpoint(8, 0)
    fn3 = op(fn1, fn2)

    assert fn3._initial_value == op(fn1._initial_value, fn2._initial_value)
    assert fn3.breakpoints[-1] == op(fn1._initial_value, fn2.breakpoints[-1])
    assert fn3.breakpoints[2] == op(fn1.breakpoints[2], fn2.breakpoints[-1])
    assert fn3.breakpoints[4] == op(fn1.breakpoints[4], fn2.breakpoints[-1])
    assert fn3.breakpoints[7] == op(fn1.breakpoints[7], fn2.breakpoints[7])
    try:
        assert fn3.breakpoints[8] == op(fn1.breakpoints[7], fn2.breakpoints[8])
    except ZeroDivisionError:
        assert fn3.breakpoints[8] == 0


def test_piecewise_max():
    fn1 = PiecewiseConstantFunction(1)
    fn1.add_breakpoint(2, 7)
    fn1.add_breakpoint(4, 4)
    fn1.add_breakpoint(7, 1)
    fn2 = PiecewiseConstantFunction(2)
    fn2.add_breakpoint(-1, 3)
    fn2.add_breakpoint(7, 1)
    fn2.add_breakpoint(8, 0)
    fn3 = piecewise_max(fn1, fn2)

    assert fn3._initial_value == 2
    assert fn3.breakpoints[-1] == 3
    assert fn3.breakpoints[2] == 7
    assert fn3.breakpoints[4] == 4
    assert fn3.breakpoints[7] == 1
