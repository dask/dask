import operator
import re
import sys
from functools import partial
from inspect import ismodule

import numpy
from toolz.functoolz import Compose
from .compatibility import apply
from .core import get_dependencies, flatten
from .sharedict import ShareDict
from .utils import ensure_dict


__all__ = ('compiled', 'preserve_keys', 'preserve_deps')


BINARY_OP_MAP = {
    operator.add: ' + ',
    operator.sub: ' - ',
    operator.mul: ' * ',
    operator.truediv: ' / ',
    operator.floordiv: ' // ',
    operator.mod: ' % ',
    operator.pow: '**',
    operator.lshift: ' << ',
    operator.rshift: ' >> ',
    operator.or_: ' | ',
    operator.xor: ' ^ ',
    operator.and_: ' & ',
    operator.matmul: ' @ ',
    operator.eq: ' == ',
    operator.ne: ' != ',
    operator.lt: ' < ',
    operator.le: ' <= ',
    operator.gt: ' > ',
    operator.ge: ' >= ',
}


MODULE_REPLACEMENTS = {
    'numpy.core.multiarray': numpy,
    'numpy.core.fromnumeric': numpy,
    'numpy.core.numeric': numpy,
    'numpy.lib.stride_tricks': numpy,
}


def __preserve_key__(arg):
    """Marker used by :func:`compiled` to prevent optimizing a dask key away.
    """
    return arg


def __preserve_deps__(arg):
    """Marker used by :func:`compiled` to
    prevent optimizing the dependencies of a dask key away
    """
    return arg


def apply_marker(dsk, keys, marker, fast=True):
    """Mark selected keys of the target dask graph to prevent them from
    being optimized away by :func:`compiled`.

    Parameters
    ----------
    dsk : dict-like
        Dask graph
    keys: list
        Keys of the graph that need to be preserved
    marker: callable
        A dummy function to add on top of the graph callable
    fast: bool
        If True and dsk is a :class:`~dask.sharedict.ShareDict`, do not inspect
        dicts that are not expected to contain the keys. This is safe only
        if all keys are top-level keys of dask collections and the ShareDict
        was built in a standard way.

    Returns
    -------
    If fast is True and dsk is a ShareDict, a new ShareDict where some of
    the dicts have been replaced with new ones. Otherwise, a new dict.
    In both cases, the original is unaltered.
    """
    keys = set(flatten(keys))

    def _pk(this_dsk):
        return {
            k: ((marker, v) if k in keys else v)
            for k, v in this_dsk.items()
        }

    if not fast or not isinstance(dsk, ShareDict):
        return _pk(ensure_dict(dsk))

    names = {k[0] if type(k) is tuple else k for k in keys}
    out = ShareDict()
    out.dicts = {
        key_i: (_pk(dsk_i) if key_i in names else dsk_i)
        for key_i, dsk_i in dsk.dicts.items()
    }
    return out


preserve_keys = partial(apply_marker, marker=__preserve_key__)
preserve_deps = partial(apply_marker, marker=__preserve_deps__)


def compiled(dsk, keys):
    dsk = ensure_dict(dsk, copy=True)
    if not isinstance(keys, (list, set)):
        keys = [keys]
    keys = set(flatten(keys))
    while keys:
        new_keys = set()
        for key in keys:
            builder = SourceBuilder(dsk, key)
            new_keys |= builder.sourcebuilder_keys
        assert not new_keys & keys
        keys = new_keys
    # Discard remaining __preserve_key__ markers
    for k, v in dsk.items():
        if type(v) is tuple and v and v[0] is __preserve_key__:
            dsk[k] = v[1]
    return dsk


compiled_function_cache = {}
replacement_funcs = set()


def add_replacement(func):
    replacement_funcs.add(func)


def add_replacement_regex(pattern, repl, count=0, flags=0):
    func = partial(re.sub, pattern, repl, count=count, flags=flags)
    replacement_funcs.add(func)


class Compiled:
    __slots__ = ('_source', '_base', '_func')

    def __init__(self, source, base=None):
        if source:
            for func in replacement_funcs:
                source = func(source)

        if not base:
            base = compiled_function_cache.get(source)

        if base:
            self._source = None
            self._base = base
            self._func = None
        else:
            assert source
            # print(source)
            exec(source)
            for k, v in locals().items():
                if ismodule(v):
                    globals()[k] = v
            self._source = source
            self._base = None
            self._func = locals()['_compiled']
            compiled_function_cache[source] = self

    def __getstate__(self):
        return self._source, self._base

    def __setstate__(self, state):
        self.__init__(*state)

    def __call__(self, *args):
        if self._base:
            return self._base(*args)
        return self._func(*args)

    def __hash__(self):
        if self._base:
            return hash(self._base)
        return hash(self._source)

    @property
    def source(self):
        if self._base:
            return self._base.source
        return self._source

    def __repr__(self):
        h = abs(hash(self)) % 2**32
        try:
            h = h.to_bytes(4, 'little').hex()
        except AttributeError:
            # Python 2
            pass
        return "<Compiled-%s>" % h


class SourceBuilder:
    def __init__(self, dsk, key):
        # print(f'SourceBuilder {key}')
        self.dsk = dsk
        self.imports = set()
        self.assigns = []
        self.arg_names = []
        self.arg_values = []
        self.obj_names = {}
        self.name_counters = {}
        self.dsk_key_map = {}
        self.sourcebuilder_keys = set()

        val = dsk[key]
        if not(type(val) is tuple and val and callable(val[0])):
            return
        if val[0] is Compiled:
            return
        if val[0] is __preserve_key__:
            # convert to Compiled
            dsk[key] = val[1]

        # Start recursion
        self._traverse(key)
        if not self.assigns:
            # Avoid building trivial functions
            # def f(x):
            #     return x
            return

        self.assigns[-1] = 'return ' + self.assigns[-1].partition(' = ')[2]

        # Python does not support more than 255 args in a function - handle
        # the special case where there's more.
        if len(self.arg_names) > 255:
            row = ", ".join(self.arg_names[254:]) + " = __long_args__"
            self.assigns.insert(0, row)
            self.arg_names = self.arg_names[:254] + ['__long_args__']
            self.arg_values = self.arg_values[:254] + [self.arg_values[254:]]

        rows = []
        for modname in sorted(self.imports):
            rows.append('import ' + modname)
        if self.imports:
            rows += ['', '']
        rows.append('def _compiled(%s):' % ', '.join(self.arg_names))
        for assign in self.assigns:
            rows.append('    ' + assign)

        source = '\n'.join(rows) + '\n'
        func = Compiled(source)
        self.dsk[key] = tuple([func] + self.arg_values)

    def _traverse(self, key):
        if key in self.dsk_key_map:
            # Already existing variable
            return self.dsk_key_map[key]

        val = self.dsk[key]

        # When encountering preserve_key or Compiled, stop recursion
        # and add it as a new arg. For preserve_key, recursively spawn a new
        # SourceBuilder for all the dependencies of the node.
        if type(val) is tuple and val:
            if val[0] is Compiled:
                name = self._add_arg(key)
                self.dsk_key_map[key] = name
                return name
            if val[0] is __preserve_key__:
                self.sourcebuilder_keys.add(key)
                name = self._add_arg(key)
                self.dsk_key_map[key] = name
                return name
            if val[0] is __preserve_deps__:
                for dep_key in get_dependencies(self.dsk, key,
                                                as_list=False):
                    dep_val = self.dsk[dep_key]
                    if type(dep_val) is tuple and dep_val and callable(
                            dep_val[0]):
                        self.dsk[dep_key] = __preserve_key__, dep_val
                val = val[1]

        # Recursively convert dsk value to a line of source code
        source = self._to_source(val)

        if re.match(r'^[a-zA-Z_]+[a-zA-Z0-9_]*$', source):
            # a = b
            return source

        if self.arg_names[-1:] == [source]:
            # constant arg
            return source

        # row is an expression; assign it to a new variable name
        name = self._unique_name(key)
        self.dsk_key_map[key] = name
        self.assigns.append("%s = %s" % (name, source))
        return name

    def _unique_name(self, obj):
        if isinstance(obj, tuple):
            name = "dsk_" + obj[0].split('-')[0]
        else:
            try:
                name = obj.__name__
            except AttributeError:
                name = type(obj).__name__

        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        name = re.sub(r'__*', '_', name)
        name = name.lower()

        cnt = self.name_counters.get(name, -1)
        cnt += 1
        self.name_counters[name] = cnt
        return "%s_%d" % (name, cnt)

    def _to_source(self, v):
        # print(f"to_source({v})")
        vtype = type(v)

        if vtype in (int, float):
            if numpy.isnan(v):
                self.imports.add('numpy')
                return 'numpy.nan'
            return repr(v)  # Prevent improper rounding in Python 2
        if vtype in (bool, bytes, slice, range) or v is None:
            return repr(v)
        if vtype is str:
            # Is it a reference to another dask graph node?
            if v in self.dsk:
                return self._traverse(v)
            return repr(v)  # Add quotes
        if vtype is set:
            if not v:
                return 'set()'
            if sys.version < '3':
                return 'set([%s])' % ', '.join(self._to_source(x) for x in v)
            return '{%s}' % ', '.join(self._to_source(x) for x in v)
        if vtype is list:
            return '[%s]' % ', '.join(self._to_source(x) for x in v)
        if vtype is dict:
            if sys.version < '3':
                return 'dict(%s)' % ', '.join(
                    '(%s, %s)' % (self._to_source(key), self._to_source(val))
                    for key, val in v.items())
            return '{%s}' % ', '.join(
                '%s: %s' % (self._to_source(key), self._to_source(val))
                for key, val in v.items())

        if vtype is tuple:
            # Is it a reference to another dask graph node?
            try:
                is_key = v in self.dsk
            except TypeError:
                # Unhashable
                is_key = False
            if is_key:
                return self._traverse(v)

            if v and callable(v[0]):
                return self._dsk_function_to_source(v[0], v[1:], {})

            # Generic tuple
            if len(v) == 1:
                return '(%s, )' % self._to_source(v[0])
            return '(%s)' % ', '.join(self._to_source(x) for x in v)

        # Additional objects explicitly handled for convenience
        # This section is not strictly necessary - if you remove it,
        # these types will be processed as constant args.
        if vtype is numpy.dtype:
            # Attempt a numba.jit-friendly version first
            try:
                if getattr(numpy, v.name) == v:
                    return 'numpy.' + v.name
            except AttributeError:
                pass
            return repr(v)  # str and repr aren't equivalent

        # Generic object - processed as import or constant arg
        return self._add_local(v)

    def _dsk_function_to_source(self, func, args, kwargs):
        args = tuple(args)
        kwargs = kwargs.copy()

        # Objects explicitly handled for convenience This is not strictly
        # necessary - you could remove everything but the final section
        # "Generic Callable" and these types would be processed as
        # either imports or constant args.

        # Unpack partials
        if func is apply:
            if len(args) == 3:
                kwargs.update(args[2])
            else:
                assert len(args) == 2
            return self._dsk_function_to_source(args[0], args[1], kwargs)

        if isinstance(func, partial):
            kwargs.update(func.keywords)
            return self._dsk_function_to_source(
                func.func, func.args + args, kwargs)

        if isinstance(func, Compose):
            assert not kwargs
            tup = (func.first,) + args
            for f in func.funcs:
                tup = (f, tup)
            return self._to_source(tup)

        # Convert binary ops
        try:
            op = BINARY_OP_MAP[func]
        except KeyError:
            pass
        else:
            assert not kwargs
            assert len(args) == 2
            return self._to_source(args[0]) + op + self._to_source(args[1])

        # operator.invert
        if func is operator.invert:
            assert not kwargs
            assert len(args) == 1
            return '~' + self._to_source(args[0])

        # operator.getitem
        if func is operator.getitem:
            assert len(args) == 2
            assert not kwargs

            def idx_to_source(idx):
                if isinstance(idx, tuple):
                    if not idx:
                        return None
                    return ', '.join(idx_to_source(i) for i in idx)
                if isinstance(idx, slice):
                    start, stop, step = [
                        self._to_source(s) if s is not None else ''
                        for s in (idx.start, idx.stop, idx.step)
                    ]
                    if step:
                        return '%s:%s:%s' % (start, stop, step)
                    return '%s:%s' % (start, stop)
                return self._to_source(idx)

            idx = idx_to_source(args[1])
            if idx:
                return '%s[%s]' % (self._to_source(args[0]), idx)
            return self._to_source(args[0])

        # Generic callable
        args_source = [
            self._to_source(x) for x in args
        ] + [
            '%s=%s' % (key, self._to_source(val))
            for key, val in kwargs.items()
        ]
        return '%s(%s)' % (self._to_source(func), ', '.join(args_source))

    def _add_local(self, obj):
        try:
            lookup_key = hash(obj)
        except TypeError:
            # Unhashable type
            lookup_key = id(obj)
        try:
            return self.obj_names[lookup_key]
        except KeyError:
            pass

        try:
            in_module = getattr(sys.modules[obj.__module__], obj.__name__) is obj
        except (KeyError, AttributeError):
            in_module = False

        if in_module and obj.__module__ == 'builtins':
            res = obj.__name__
        elif in_module:
            mod = obj.__module__
            try:
                try_mod = MODULE_REPLACEMENTS[mod]
                if getattr(try_mod, obj.__name__) is obj:
                    mod = try_mod.__name__
            except (KeyError, AttributeError):
                pass

            self.imports.add(mod)
            res = mod + '.' + obj.__name__
        else:
            if isinstance(obj, numpy.ufunc):
                try:
                    is_ufunc = getattr(numpy, obj.__name__) is obj
                except AttributeError:
                    is_ufunc = False
            else:
                is_ufunc = False

            if is_ufunc:
                self.imports.add('numpy')
                res = 'numpy.' + obj.__name__
            else:
                # It's not an object existing in a module
                # (e.g. it's an instance)
                res = self._add_arg(obj)

        self.obj_names[lookup_key] = res
        return res

    def _add_arg(self, val):
        name = self._unique_name(val)
        self.arg_names.append(name)
        self.arg_values.append(val)
        return name
