import operator
import re
import sys
from functools import partial
from inspect import ismodule

import ast
import numpy
from toolz.functoolz import Compose
from .array.core import getter, getter_inline, getter_nofancy
from .compatibility import apply
from .core import flatten
from .optimization import cull
from .utils import ensure_dict


__all__ = ('compile_ast', )


UNARY_OP_MAP = {
    operator.not_: ast.Not,
    operator.invert: ast.Invert,
}

BINARY_OP_MAP = {
    operator.add: ast.Add,
    operator.sub: ast.Sub,
    operator.mul: ast.Mult,
    operator.truediv: ast.Div,
    operator.floordiv: ast.FloorDiv,
    operator.mod: ast.Mod,
    operator.pow: ast.Pow,
    operator.lshift: ast.LShift,
    operator.rshift: ast.RShift,
    operator.or_: ast.BitOr,
    operator.xor: ast.BitXor,
    operator.and_: ast.BitAnd,
    operator.matmul: ast.MatMult,
}

COMPARE_OP_MAP = {
    operator.eq: ast.Eq,
    operator.ne: ast.NotEq,
    operator.lt: ast.Lt,
    operator.le: ast.LtE,
    operator.gt: ast.Gt,
    operator.ge: ast.GtE,
}


MODULE_REPLACEMENTS = {
    'numpy.core.multiarray': numpy,
    'numpy.core.fromnumeric': numpy,
    'numpy.core.numeric': numpy,
}


def compile_ast(dsk, keys):
    dsk = ensure_dict(dsk)
    keys = set(flatten(keys))
    result = {}
    for key in keys:
        dsk_i, _ = cull(dsk, [key])
        builder = ASTDaskBuilder(dsk_i, key)
        result.update(builder.dsk)
    return result


class ASTFunction:
    __slots__ = ('_code', '_source', '_func')


    def __init__(self, tree):
        # Do not store the AST tree as an attribute, because it takes ages to
        # pickle. Besides, its only purpose is to generate the source - which
        # is only used for debugging, but it's much faster to generate and
        # instantaneous to pickle.
        # FIXME: is there a way to generate the source code from the compiled
        # code?
        try:
            import astor
        except ImportError:
            self._source = None
        else:
            self._source = astor.to_source(tree)

        self._code = compile(tree, filename='<ast>', mode='exec')
        self._setup()


    @property
    def source(self):
        if self._source is None:
            raise ImportError("AST source inspection requires astor")
        return self._source


    def __getstate__(self):
        # Compiled bytecode is hashable; functions extracted from it aren't
        return self._code, self._source


    def __setstate__(self, state):
        self._code, self._source = state
        self._setup()


    def _setup(self):
        exec(self._code)
        for k, v in locals().items():
            if ismodule(v):
                globals()[k] = v
        self._func = locals()['_ast_compiled']


    def __call__(self, *args):
        return self._func(*args)


    def __hash__(self):
        return hash(self._code)


    def __repr__(self):
        return "<ASTFunction %d>" % hash(self)


class ASTDaskBuilder:
    def __init__(self, dsk, key):
        self.dsk = ensure_dict(dsk)
        self.imports = set()
        self.assigns = []
        self.arg_names = []
        self.arg_keys = []
        self.constant_arg_names = []
        self.constant_arg_values = []
        self.obj_names = {}
        self.name_counters = {}
        self.dsk_key_map = {}
        self.delete_keys = set()

        # Start recursion
        root = self._traverse(key)
        self.assigns.append(ast.Return(root))

        imports = [ast.Import([ast.alias(modname, None)])
                   for modname in sorted(self.imports)]

        func = ast.FunctionDef(
            name='_ast_compiled',
            args=ast.arguments(
                args=[ast.arg(a, None) for a in
                      self.constant_arg_names + self.arg_names],
                defaults=[],
                kwarg=None,
                kwonlyargs=[],
                kw_defaults=[],
                vararg=None),
            body=self.assigns,
            decorator_list=[],
            returns=None)

        self.tree = ast.fix_missing_locations(
            ast.Module(body=imports + [func]))

        func = ASTFunction(self.tree)
        self.dsk = {
            k: v for k, v in self.dsk.items()
            if k not in self.delete_keys
        }
        self.dsk[key] = tuple([func] + self.constant_arg_values +
                              self.arg_keys)


    def _traverse(self, key):
        # Avoid try... except Keyerror to keep stack traces clean
        if key in self.dsk_key_map:
            name = self.dsk_key_map[key]
        else:
            name = self._unique_name(key)
            self.dsk_key_map[key] = name
            val = self.dsk[key]
            # Stop recursion when a ASTFunction is found
            if (isinstance(val, tuple) and val and
                    isinstance(val[0], ASTFunction)):
                self.arg_names.append(name)
                self.arg_keys.append(key)
            else:
                ast_code = ast.Assign([ast.Name(name, ast.Store())],
                                      self._to_ast(val))
                self.assigns.append(ast_code)
                self.delete_keys.add(key)

        return ast.Name(name, ast.Load())


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


    def _to_ast(self, v):
        # print(f"to_ast({v})")
        vtype = type(v)

        if vtype in (int, float):
            if numpy.isnan(v):
                self.imports.add('numpy')
                return ast.Attribute(ast.Name('numpy', ast.Load()),
                                     'nan', ast.Load())
            return ast.Num(v)
        if vtype is bool or v is None:
            return ast.NameConstant(v)
        if vtype is bytes:
            return ast.Bytes(v)
        if vtype is str:
            # Is it a reference to another dask graph node?
            if v in self.dsk:
                return self._traverse(v)
            return ast.Str(v)
        if vtype is set:
            return ast.Set([self._to_ast(x) for x in v])
        if vtype is list:
            return ast.List([self._to_ast(x) for x in v], ast.Load())
        if vtype is dict:
            return ast.Dict([self._to_ast(k) for k in v.keys()],
                            [self._to_ast(k) for k in v.values()])
        if vtype is tuple:
            # Is it a reference to another dask graph node?
            try:
                is_key = v in self.dsk
            except TypeError:
                # Unhashable
                pass
            else:
                if is_key:
                    return self._traverse(v)

            if v and callable(v[0]):
                return self._dsk_function_to_ast(v[0], v[1:], {})

            # Generic tuple
            return ast.Tuple([self._to_ast(x) for x in v], ast.Load())

        # Additional objects explicitly handled for convenience
        # This section is not strictly necessary - if you remove it,
        # these types will be processed as constant kwargs.
        if vtype in (slice, range):
            return ast.Call(
                ast.Name(vtype.__name__, ast.Load()),
                [self._to_ast(x) for x in (v.start, v.stop, v.step)], [])

        if vtype is numpy.dtype:
            return self._to_ast((numpy.dtype, v.name))

        # Generic object - processed as import or constant kwarg
        return self._add_local(v)


    def _dsk_function_to_ast(self, func, args, kwargs):
        args = tuple(args)
        kwargs = kwargs.copy()

        # Objects explicitly handled for convenience This is not strictly
        # necessary - you could remove everything but the final section
        # "Generic Callable" and these types would be processed as
        # either imports or constant kwargs.

        # Unpack partials
        if func is apply:
            if len(args) == 3:
                kwargs.update(args[2])
            return self._dsk_function_to_ast(args[0], args[1], kwargs)

        if isinstance(func, partial):
            kwargs.update(func.keywords)
            return self._dsk_function_to_ast(func.func, func.args + args,
                                             kwargs)

        if isinstance(func, Compose):
            assert not kwargs
            funcs = ((func.first,) + func.funcs)
            tup = (funcs[0],) + args
            for func in funcs[1:]:
                tup = (func, tup)
            return self._to_ast(tup)

        # Convert unary ops
        try:
            op = UNARY_OP_MAP[func]
        except KeyError:
            pass
        else:
            assert not kwargs
            assert len(args) == 1
            return ast.UnaryOp(op(), self._to_ast(args[0]))

        # Convert binary ops
        try:
            op = BINARY_OP_MAP[func]
        except KeyError:
            pass
        else:
            assert not kwargs
            assert len(args) == 2
            return ast.BinOp(self._to_ast(args[0]), op(),
                             self._to_ast(args[1]))

        # Convert comparison ops
        try:
            op = COMPARE_OP_MAP[func]
        except KeyError:
            pass
        else:
            assert not kwargs
            assert len(args) == 2
            return ast.Compare(self._to_ast(args[0]), [op()],
                               [self._to_ast(args[1])])

        # operator.getitem
        if func is operator.getitem:
            assert len(args) == 2
            assert not kwargs

            def slice_to_ast(s):
                if s in (tuple, list):
                    return ast.ExtSlice([slice_to_ast(i) for i in s])
                if isinstance(s, slice):
                    return ast.Slice(*(ast.Num(i) if i is not None else None
                                     for i in (s.start, s.stop, s.step)))
                return ast.Index(self._to_ast(s))

            return ast.Subscript(self._to_ast(args[0]), slice_to_ast(args[1]),
                                 ast.Load())

        # dask getters
        if func in (getter, getter_nofancy, getter_inline):
            assert 1 < len(args) < 5
            a, b = args[:2]
            asarray = args[2] if len(args) > 2 else kwargs.get('asarray', True)
            lock = args[3] if len(args) > 3 else kwargs.get('lock', False)
            nested = isinstance(b, tuple) and any(x is None for x in b)

            if not lock and not nested:
                tup = (operator.getitem, a, b)
                if asarray:
                    tup = (numpy.asarray, tup)
                return self._to_ast(tup)

        # Generic callable
        return ast.Call(self._to_ast(func),
                        [self._to_ast(x) for x in args],
                        [ast.keyword(arg=key, value=self._to_ast(val))
                         for key, val in kwargs.items()])


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
            res = ast.Name(obj.__name__, ast.Load())
        elif in_module:
            mod = obj.__module__
            try:
                try_mod = MODULE_REPLACEMENTS[mod]
                if getattr(try_mod, obj.__name__) is obj:
                    mod = try_mod.__name__
            except (KeyError, AttributeError):
                pass

            self.imports.add(mod)
            path = mod.split('.') + [obj.__name__]
            path[0] = ast.Name(path[0], ast.Load())
            while len(path) > 1:
                path = [ast.Attribute(path[0], path[1], ast.Load())] + path[2:]
            res = path[0]
        elif isinstance(obj, numpy.ufunc):
            assert getattr(numpy, obj.__name__) is obj
            self.imports.add('numpy')
            res = ast.Attribute(ast.Name('numpy', ast.Load()),
                                obj.__name__, ast.Load())
        else:
            # It's not an object existing in a module (e.g. it's an instance)
            name = self._unique_name(obj)
            self.constant_arg_names.append(name)
            self.constant_arg_values.append(obj)
            res = ast.Name(name, ast.Load())

        self.obj_names[lookup_key] = res
        return res
