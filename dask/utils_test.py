from __future__ import absolute_import, division, print_function

from .core import get
import unittest

def inc(x):
    return x + 1

def add(x, y):
    return x + y

class GetFunctionTestCase(unittest.TestCase):
    """
    The GetFunctionTestCase class can be imported and used to test foreign
    implementations of the `get` function specification. It aims to enforce all
    known expecctations of `get` functions.

    To use the class, inherit from it and override the `get` function. For
    example:

    > from dask.utils_test import GetFunctionTestCase
    > class CustomGetTestCase(GetFunctionTestCase):
         get = staticmethod(myget)

    Note that the foreign `myget` function has to be explicitly decorated as a
    staticmethod.
    """
    get = staticmethod(get)

    def runTest(self):
        """This method is needed for compatibility with PY2 unittest.TestCase"""
        pass

    def test_get(self):
        d = {':x': 1,
             ':y': (inc, ':x'),
             ':z': (add, ':x', ':y')}

        assert self.get(d, ':x') == 1
        assert self.get(d, ':y') == 2
        assert self.get(d, ':z') == 3

    def test_badkey(self):
        d = {':x': 1,
             ':y': (inc, ':x'),
             ':z': (add, ':x', ':y')}
        try:
            result = self.get(d, 'badkey')
        except KeyError:
            pass
        else:
            msg = 'Expected `{}` with badkey to raise KeyError.\n'
            msg += "Obtained '{}' instead.".format(result)
            self.assertTrue(False, msg=msg.format(self.get.__name__))

    def test_nested_badkey(self):
        d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

        try:
            result = self.get(d, [['badkey'], 'y'])
        except KeyError:
            pass
        else:
            msg = 'Expected `{}` with badkey to raise KeyError.\n'
            msg += "Obtained '{}' instead.".format(result)
            self.assertTrue(False, msg=msg.format(self.get.__name__))

    def test_data_not_in_dict_is_ok(self):
        d = {'x': 1, 'y': (add, 'x', 10)}
        assert self.get(d, 'y') == 11

    def test_get_with_list(self):
        d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

        assert self.get(d, ['x', 'y']) == (1, 2)
        assert self.get(d, 'z') == 3

    def test_get_with_nested_list(self):
        d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

        assert self.get(d, [['x'], 'y']) == ((1,), 2)
        assert self.get(d, 'z') == 3

    def test_get_works_with_unhashables_in_values(self):
        f = lambda x, y: x + len(y)
        d = {'x': 1, 'y': (f, 'x', set([1]))}

        assert self.get(d, 'y') == 2

    def test_nested_tasks(self):
        d = {'x': 1,
             'y': (inc, 'x'),
             'z': (add, (inc, 'x'), 'y')}

        assert self.get(d, 'z') == 4

    def test_get_stack_limit(self):
        d = dict(('x%s' % (i+1), (inc, 'x%s' % i)) for i in range(10000))
        d['x0'] = 0
        assert self.get(d, 'x10000') == 10000
        # introduce cycle
        d['x5000'] = (inc, 'x5001')

        try:
            self.get(d, 'x10000')
        except (RuntimeError, ValueError) as e:
            if isinstance(e, RuntimeError):
                assert str(e) == 'Cycle detected in Dask: x5001->x5000->x5001'
            elif isinstance(e, ValueError):
                assert str(e).startswith('Found no accessible jobs in dask')
        else:
            msg = 'dask with infinite cycle should have raised an exception.'
            self.assertTrue(False, msg=msg)

        assert self.get(d, 'x4999') == 4999
