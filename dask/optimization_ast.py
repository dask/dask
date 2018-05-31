import operator
import pickle
import re
from functools import partial
from inspect import ismodule

import ast
from numpy import dtype
from toolz.functoolz import Compose
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
    __slots__ = ('_code', '_source', '_func', 'constant_kwargs')


    def __init__(self, tree, constant_kwargs):
        self.constant_kwargs = constant_kwargs

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
        return self._code, self._source, self.constant_kwargs


    def __setstate__(self, state):
        self._code, self._source, self.constant_kwargs = state
        self._setup()


    def _setup(self):
        exec(self._code)
        for k, v in locals().items():
            if ismodule(v):
                globals()[k] = v
        self._func = locals()['_ast_compiled']


    def __call__(self, *args):
        return self._func(*args, **self.constant_kwargs)


    def __hash__(self):
        kwargs = []
        for k, v in self.constant_kwargs.items():
            try:
                kwargs += [k, hash(v)]
            except TypeError:
                # Unhashable type
                kwargs += [k, id(v)]
        return hash((self._code, ) + tuple(kwargs))


class ASTDaskBuilder:
    def __init__(self, dsk, key):
        self.dsk = ensure_dict(dsk)
        self.imports = set()
        self.assigns = []
        self.args = []
        self.arg_keys = []
        self.constant_kwargs = {}
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
                args=[ast.arg(a, None) for a in self.args + list(
                    self.constant_kwargs.keys())],
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

        func = ASTFunction(self.tree, self.constant_kwargs)
        self.dsk = {
            k: v for k, v in self.dsk.items()
            if k not in self.delete_keys
        }
        self.dsk[key] = (func, ) + tuple(self.arg_keys)


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
                self.args.append(name)
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
                name = 'noname'

        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if name not in self.name_counters:
            self.name_counters[name] = 0
            return name
        self.name_counters[name] += 1
        return name + '_' + str(self.name_counters[name])


    def _to_ast(self, v):
        # print(f"to_ast({v})")
        vtype = type(v)

        if vtype in (int, float):
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
        # This is unnecessary - if you remove this section, they
        # will be processed as constant kwargs
        if vtype is dtype:
            return ast.Str(v.name)

        # Generic object - processed as import or constant kwarg
        return self._add_local(v)


    def _dsk_function_to_ast(self, func, args, kwargs):
        args = tuple(args)
        kwargs = kwargs.copy()

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
            if pickle.loads(pickle.dumps(obj)) is not obj:
                raise AttributeError()
        except (AttributeError, pickle.PicklingError):
            # Can't be pickled, or it's not an object existing
            # in a module (e.g. it's an instance)
            name = self._unique_name(obj)
            self.constant_kwargs[name] = obj
            res = ast.Name(name, ast.Load())
        else:
            # pickle->unpickle round-trip returns identical object
            # use a simple import
            self.imports.add(obj.__module__)
            path = obj.__module__.split('.') + [obj.__name__]
            path[0] = ast.Name(path[0], ast.Load())
            while len(path) > 1:
                path = [ast.Attribute(path[0], path[1], ast.Load())] + path[2:]
            res = path[0]

        self.obj_names[lookup_key] = res
        return res
