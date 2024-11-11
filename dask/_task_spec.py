from __future__ import annotations

""" Task specification for dask

This module contains the task specification for dask. It is used to represent
runnable (task) and non-runnable (data) nodes in a dask graph.

Simple examples of how to express tasks in dask
-----------------------------------------------

.. code-block:: python

    func("a", "b") ~ Task("key", func, "a", "b")

    [func("a"), func("b")] ~ [Task("key-1", func, "a"), Task("key-2", func, "b")]

    {"a": func("b")} ~ {"a": Task("a", func, "b")}

    "literal-string" ~ DataNode("key", "literal-string")


Keys, Aliases and TaskRefs
-------------------------

Keys are used to identify tasks in a dask graph. Every `GraphNode` instance has a
key attribute that _should_ reference the key in the dask graph.

.. code-block:: python

    {"key": Task("key", func, "a")}

Referencing other tasks is possible by using either one of `Alias` or a
`TaskRef`.

.. code-block:: python

    # TaskRef can be used to provide the name of the reference explicitly
    t = Task("key", func, TaskRef("key-1"))

    # If a task is still in scope, the method `ref` can be used for convenience
    t2 = Task("key2", func2, t.ref())

Nested tasks and inlining
-------------------------

Tasks can be nested in a dict or list. This is useful for expressing more complex tasks.

.. code-block:: python

    func(func2("a", "b"), "c") ~ Task("key", func, [Task("key-1", func2, "a", "b"), "c"])

    {"a": func("b")} ~ {"a": Task("a", func, "b")}

This kind of nesting is often a byproduct of an optimizer step that inlines the
tasks explicitly. This is done by calling the `inline` method on a task.

.. code-block:: python

    t1 = Task("key-1", func, "a")
    t2 = Task("key-2", func, TaskRef("key-1"))

    t2.inline({"key-1": t1}) ~ Task("key-2", func, Task("key-1", func, "a"))

Executing a task
----------------

A task can be executed by calling it with a dictionary of values. The values
should contain the dependencies of the task.

.. code-block:: python

    t = Task("key", add, TaskRef("a"), TaskRef("b"))
    assert t.dependencies == {"a", "b"}
    t({"a": 1, "b": 2}) == 3

"""
import itertools
import sys
from collections import defaultdict
from collections.abc import Callable, Container, Iterable, Mapping, MutableMapping
from functools import partial
from typing import Any, TypeVar, cast, overload

from dask.sizeof import sizeof
from dask.typing import Key as KeyType
from dask.utils import funcname, is_namedtuple_instance

_T = TypeVar("_T")
_T_GraphNode = TypeVar("_T_GraphNode", bound="GraphNode")
_T_Iterable = TypeVar("_T_Iterable", bound=Iterable)


def identity(*args):
    return args


def _identity_cast(*args, typ):
    return typ(args)


_anom_count = itertools.count()


@overload
def parse_input(obj: _T_Iterable) -> _T_Iterable: ...


@overload
def parse_input(obj: TaskRef) -> Alias: ...


@overload
def parse_input(obj: _T_GraphNode) -> _T_GraphNode: ...


def parse_input(obj: Any) -> object:
    """Tokenize user input into GraphNode objects

    Note: This is similar to `convert_old_style_task` but does not
    - compare any values to a global set of known keys to infer references/futures
    - parse tuples and interprets them as runnable tasks
    - Deal with SubgraphCallables

    Parameters
    ----------
    obj : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    if isinstance(obj, GraphNode):
        return obj
    elif isinstance(obj, dict):
        return {k: parse_input(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set, frozenset)):
        if is_namedtuple_instance(obj):
            return _wrap_namedtuple_task(None, obj, parse_input)
        return type(obj)(map(parse_input, obj))

    if isinstance(obj, TaskRef):
        return Alias(obj.key)

    return obj


def _wrap_namedtuple_task(k, obj, parser):
    if hasattr(obj, "__getnewargs_ex__"):
        new_args, kwargs = obj.__getnewargs_ex__()
        kwargs = {k: parser(v) for k, v in kwargs.items()}
    elif hasattr(obj, "__getnewargs__"):
        new_args = obj.__getnewargs__()
        kwargs = {}

    args_converted = type(new_args)(map(parser, new_args))

    return Task(
        k, partial(_instantiate_named_tuple, type(obj)), *args_converted, **kwargs
    )


def _instantiate_named_tuple(typ, *args, **kwargs):
    return typ(*args, **kwargs)


class _MultiContainer(Container):
    container: tuple
    __slots__ = ("container",)

    def __init__(self, *container):
        self.container = container

    def __contains__(self, o: object) -> bool:
        for c in self.container:
            if o in c:
                return True
        return False


def _to_dict(*args: Any) -> dict:
    vals = args[:-1]
    keys = args[-1]
    return {key: v for key, v in zip(keys, vals)}


def _wrap_dict_in_task(d: dict) -> Task:
    return Task(None, _to_dict, *d.values(), d.keys())


SubgraphType = None


def _execute_subgraph(inner_dsk, outkey, inkeys, external_deps):
    final = {}
    final.update(inner_dsk)
    for k, v in inkeys.items():
        final[k] = DataNode(None, v)
    for k, v in external_deps.items():
        final[k] = DataNode(None, v)
    res = execute_graph(final, keys=[outkey])
    return res[outkey]


def convert_legacy_task(
    k: KeyType | None,
    task: _T,
    all_keys: Container,
) -> GraphNode | _T:
    global SubgraphType
    if SubgraphType is None:
        from dask.optimization import SubgraphCallable

        SubgraphType = SubgraphCallable

    if isinstance(task, GraphNode):
        return task

    if type(task) is tuple and task and callable(task[0]):
        if k is None:
            k = ("anom", next(_anom_count))

        func, args = task[0], task[1:]
        if isinstance(func, SubgraphType):
            subgraph = func
            all_keys_inner = _MultiContainer(
                subgraph.dsk, set(subgraph.inkeys), all_keys
            )
            sub_dsk = subgraph.dsk
            converted_graph = convert_legacy_graph(sub_dsk, all_keys_inner)
            external_deps = (
                _get_dependencies(converted_graph)
                - set(converted_graph)
                - set(subgraph.inkeys)
            )

            converted_sub_dsk = DataNode(None, converted_graph)

            return Task(
                k,
                _execute_subgraph,
                converted_sub_dsk,
                func.outkey,
                {
                    k: convert_legacy_task(None, target, all_keys)
                    for k, target in zip(subgraph.inkeys, args)
                },
                {ext_dep: Alias(ext_dep) for ext_dep in external_deps},
            )
        else:
            new_args = tuple(convert_legacy_task(None, a, all_keys) for a in args)
            return Task(k, func, *new_args)
    try:
        if isinstance(task, (bytes, int, float, str, tuple)):
            if task in all_keys:
                if k is None:
                    return Alias(task)
                else:
                    return Alias(k, target=task)
    except TypeError:
        # Unhashable
        pass

    if isinstance(task, (list, tuple, set, frozenset)):
        if is_namedtuple_instance(task):
            return _wrap_namedtuple_task(
                k,
                task,
                partial(convert_legacy_task, None, all_keys=all_keys),
            )
        else:
            parsed_args = tuple(convert_legacy_task(None, t, all_keys) for t in task)
            if any(isinstance(a, GraphNode) for a in parsed_args):
                return Task(
                    k, _identity_cast, *parsed_args, typ=DataNode(None, type(task))
                )
            else:
                return cast(_T, type(task)(parsed_args))
    elif isinstance(task, dict):
        return _wrap_dict_in_task(
            {k: convert_legacy_task(k, v, all_keys) for k, v in task.items()}
        )

    elif isinstance(task, TaskRef):
        if k is None:
            return Alias(task.key)
        else:
            return Alias(k, target=task.key)
    else:
        return task


def convert_legacy_graph(dsk: Mapping, all_keys: Container | None = None):
    if not all_keys:
        all_keys = set(dsk)
    new_dsk = {}
    for k, arg in dsk.items():
        t = convert_legacy_task(k, arg, all_keys)
        if isinstance(t, Alias) and isinstance(arg, TaskRef) and t.key == arg.key:
            # This detects cycles?
            continue
        new_dsk[k] = t
    new_dsk2 = {
        k: v if isinstance(v, GraphNode) else DataNode(k, v) for k, v in new_dsk.items()
    }
    return new_dsk2


def resolve_aliases(dsk: dict, keys: set, dependents: dict) -> dict:
    """Remove trivial sequential alias chains

    Example:

        dsk = {'x': 1, 'y': Alias('x'), 'z': Alias('y')}

        resolve_aliases(dsk, {'z'}, {'x': {'y'}, 'y': {'z'}}) == {'z': 1}

    """
    if not keys:
        raise ValueError("No keys provided")
    dsk = dict(dsk)
    work = list(keys)
    seen = set()
    while work:
        k = work.pop()
        if k in seen or k not in dsk:
            continue
        seen.add(k)
        t = dsk[k]
        if isinstance(t, Alias):
            target_key = t.target.key
            # Rules for when we allow to collapse an alias
            # 1. The target key is not in the keys set. The keys set is what the
            #    user is requesting and by collapsing we'd no longer be able to
            #    return that result.
            # 2. The target key is in fact part of dsk. If it isnt' this could
            #    point to a persisted dependency and we cannot collapse it.
            # 3. The target key has only one dependent which is the key we're
            #    currently looking at. This means that there is a one to one
            #    relation between this and the target key in which case we can
            #    collapse them.
            #    Note: If target was an alias as well, we could continue with
            #    more advanced optimizations but this isn't implemented, yet
            if (
                target_key not in keys
                and target_key in dsk
                # Note: whenever we're performing a collapse, we're not updating
                # the dependents. The length == 1 should still be sufficient for
                # chains of these aliases
                and len(dependents[target_key]) == 1
            ):
                tnew = dsk.pop(target_key).copy()

                dsk[k] = tnew
                tnew.key = k
                if isinstance(tnew, Alias):
                    work.append(k)
                    seen.discard(k)
                else:
                    work.extend(tnew.dependencies)

        work.extend(t.dependencies)
    return dsk


class TaskRef:
    val: KeyType
    __slots__ = ("key",)

    def __init__(self, key: KeyType):
        self.key = key

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return f"{type(self).__name__}({self.key!r})"

    def __hash__(self) -> int:
        return hash(self.key)


class GraphNode:
    key: KeyType
    _dependencies: frozenset

    __slots__ = tuple(__annotations__)

    def ref(self):
        return Alias(self.key)

    def copy(self):
        raise NotImplementedError

    @property
    def dependencies(self) -> frozenset:
        return self._dependencies

    def _verify_values(self, values: tuple | dict) -> None:
        if not self.dependencies:
            return
        if missing := set(self.dependencies) - set(values):
            raise RuntimeError(f"Not enough arguments provided: missing keys {missing}")

    def inline(self, dsk) -> GraphNode:
        raise NotImplementedError("Not implemented")

    def __call__(self, values) -> Any:
        raise NotImplementedError("Not implemented")

    def __eq__(self, value: object) -> bool:
        if type(value) is not type(self):
            return False
        from dask.tokenize import tokenize

        return tokenize(self) == tokenize(value)

    @property
    def is_coro(self) -> bool:
        return False

    def __sizeof__(self) -> int:
        return sum(
            sizeof(getattr(self, sl)) for sl in type(self).__slots__
        ) + sys.getsizeof(type(self))


_no_deps: frozenset = frozenset()


class Alias(GraphNode):
    __weakref__: Any = None
    target: TaskRef
    __slots__ = tuple(__annotations__)

    def __init__(self, key: KeyType, target: Alias | TaskRef | KeyType | None = None):
        self.key = key
        if target is None:
            target = key
        if isinstance(target, Alias):
            target = target.target
        if not isinstance(target, TaskRef):
            target = TaskRef(target)
        self.target = target
        self._dependencies = frozenset([target.key])

    def copy(self):
        return Alias(self.key, self.target)

    def __call__(self, values=()):
        self._verify_values(values)
        return values[self.target.key]

    def inline(self, dsk) -> GraphNode:
        if self.key in dsk:
            # This can otherwise cause recursion errors
            new_dsk = dsk.copy()
            new_dsk.pop(self.key)
            if isinstance(dsk[self.key], GraphNode):
                return dsk[self.key].inline(new_dsk)
            else:
                return dsk[self.key]
        else:
            return self

    def __repr__(self):
        return f"Alias({self.key}->{self.target})"

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Alias):
            return False
        if self.key != value.key:
            return False
        if self.target != value.target:
            return False
        return True


class DataNode(GraphNode):
    value: Any
    typ: type
    __slots__ = tuple(__annotations__)

    def __init__(self, key: Any, value: Any):
        if key is None:
            key = (type(value).__name__, next(_anom_count))
        self.key = key
        self.value = value
        self.typ = type(value)
        self._dependencies = _no_deps

    def inline(self, dsk) -> DataNode:
        return self

    def copy(self):
        return DataNode(self.key, self.value)

    def __call__(self, values=()):
        return self.value

    def __repr__(self):
        return f"DataNode({self.key}, type={self.typ}, {self.value})"

    def __reduce__(self):
        return (DataNode, (self.key, self.value))

    def __dask_tokenize__(self):
        from dask.base import tokenize

        return (type(self).__name__, tokenize(self.value))

    def __iter__(self):
        return iter(self.value)


def _inline_recursively(obj, values, seen=None):
    seen = seen or set()
    if id(obj) in seen:
        return obj
    seen.add(id(obj))
    if isinstance(obj, GraphNode):
        return obj.inline(values)
    elif isinstance(obj, dict):
        return {k: _inline_recursively(v, values, seen=seen) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, frozenset, set)):
        return type(obj)(_inline_recursively(v, values, seen=seen) for v in obj)
    return obj


def _call_recursively(obj, values):
    if isinstance(obj, GraphNode):
        return obj(values)
    elif isinstance(obj, dict):
        return {k: _call_recursively(v, values) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, frozenset, set)):
        return type(obj)(_call_recursively(v, values) for v in obj)
    return obj


def _get_dependencies(obj: object) -> set | frozenset:
    if isinstance(obj, GraphNode):
        return obj.dependencies
    elif isinstance(obj, dict):
        if not obj:
            return _no_deps
        return set().union(*map(_get_dependencies, obj.values()))
    elif isinstance(obj, (list, tuple, frozenset, set)):
        if not obj:
            return _no_deps
        return set().union(*map(_get_dependencies, obj))
    return _no_deps


class Task(GraphNode):
    func: Callable
    args: tuple
    kwargs: dict
    _token: str | None
    _is_coro: bool | None
    _repr: str | None

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        key: Any,
        func: Callable,
        /,
        *args: Any,
        **kwargs: Any,
    ):
        self.key = key
        self.func = func
        self.args = parse_input(args)
        self.kwargs = parse_input(kwargs)
        dependencies: set = set()
        dependencies.update(_get_dependencies(self.args))
        dependencies.update(_get_dependencies(tuple(self.kwargs.values())))
        if dependencies:
            self._dependencies = frozenset(dependencies)
        else:
            self._dependencies = _no_deps
        self._is_coro = None
        self._token = None
        self._repr = None

    def copy(self):
        return Task(self.key, self.func, *self.args, **self.kwargs)

    def __hash__(self):
        return hash(self._get_token())

    def _get_token(self) -> str:
        if self._token:
            return self._token
        from dask.base import tokenize

        self._token = tokenize(
            (
                type(self).__name__,
                self.func,
                self.args,
                self.kwargs,
            )
        )
        return self._token

    def __dask_tokenize__(self):
        return self._get_token()

    def __repr__(self) -> str:
        # When `Task` is deserialized the constructor will not run and
        # `self._repr` is thus undefined.
        if not hasattr(self, "_repr") or not self._repr:
            head = funcname(self.func)
            tail = ")"
            label_size = 40
            args = self.args
            kwargs = self.kwargs
            if args or kwargs:
                label_size2 = int(
                    (label_size - len(head) - len(tail) - len(str(self.key)))
                    // (len(args) + len(kwargs))
                )
            if args:
                if label_size2 > 5:
                    args_repr = ", ".join(repr(t) for t in args)
                else:
                    args_repr = "..."
            else:
                args_repr = ""
            if kwargs:
                if label_size2 > 5:
                    kwargs_repr = ", " + ", ".join(
                        f"{k}={repr(v)}" for k, v in sorted(kwargs.items())
                    )
                else:
                    kwargs_repr = ", ..."
            else:
                kwargs_repr = ""
            self._repr = f"<Task {self.key!r} {head}({args_repr}{kwargs_repr}{tail}>"
        return self._repr

    def __call__(self, values=()):
        self._verify_values(values)
        new_argspec = _call_recursively(self.args, values)
        if self.kwargs:
            kwargs = _call_recursively(self.kwargs, values)
            return self.func(*new_argspec, **kwargs)
        return self.func(*new_argspec)

    def inline(self, dsk) -> Task:
        new_args = _inline_recursively(self.args, dsk)
        new_kwargs = _inline_recursively(self.kwargs, dsk)
        assert self.func is not None
        return Task(self.key, self.func, *new_args, **new_kwargs)

    @property
    def is_coro(self):
        if self._is_coro is None:
            # Note: Can't use cached_property on objects without __dict__
            try:
                from distributed.utils import iscoroutinefunction

                self._is_coro = iscoroutinefunction(self.func)
            except Exception:
                self._is_coro = False
        return self._is_coro

    @staticmethod
    def fuse(*tasks: Task, key: KeyType | None = None) -> Task:
        leafs = set()
        all_keys = set()
        all_deps: set[KeyType] = set()
        for t in tasks:
            all_deps.update(t.dependencies)
            all_keys.add(t.key)
        external_deps = all_deps - all_keys
        leafs = all_keys - all_deps
        if len(leafs) > 1:
            raise ValueError("Cannot fuse tasks with multiple outputs")

        outkey = leafs.pop()

        return Task(
            key or outkey,
            _execute_subgraph,
            DataNode(None, {t.key: t for t in tasks}),
            outkey,
            {k: Alias(k) for k in external_deps},
            {},
        )


class DependenciesMapping(MutableMapping):
    def __init__(self, dsk):
        self.dsk = dsk
        self._removed = set()
        # Set a copy of dsk to avoid dct resizing
        self._cache = dsk.copy()
        self._cache.clear()

    def __getitem__(self, key):
        if (val := self._cache.get(key)) is not None:
            return val
        else:
            v = self.dsk[key]
            try:
                deps = v.dependencies
            except AttributeError:
                from dask.core import get_dependencies

                deps = get_dependencies(self.dsk, task=v)

            if self._removed:
                # deps is a frozenset but for good measure, let's not use -= since
                # that _may_ perform an inplace mutation
                deps = deps - self._removed
            self._cache[key] = deps
            return deps

    def __iter__(self):
        return iter(self.dsk)

    def __delitem__(self, key: Any) -> None:
        self._cache.clear()
        self._removed.add(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        return len(self.dsk)


class _DevNullMapping(MutableMapping):
    def __getitem__(self, key):
        raise KeyError(key)

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __len__(self):
        return 0

    def __iter__(self):
        return iter(())


def execute_graph(
    dsk: Iterable[GraphNode] | Mapping[KeyType, GraphNode],
    cache: MutableMapping[KeyType, object] | None = None,
    keys: Container[KeyType] | None = None,
) -> MutableMapping[KeyType, object]:
    """Execute a given graph.

    The graph is exceuted in topological order as defined by dask.order until
    all leaf nodes, i.e. nodes without any dependents, are reached. The returned
    dictionary contains the results of the leaf nodes.

    If keys are required that are not part of the graph, they can be provided in the `cache` argument.

    If `keys` is provided, the result will contain only values that are part of the `keys` set.

    """
    if isinstance(dsk, (list, tuple, set, frozenset)):
        dsk = {t.key: t for t in dsk}
    else:
        assert isinstance(dsk, dict)

    refcount: defaultdict[KeyType, int] = defaultdict(int)
    for vals in DependenciesMapping(dsk).values():
        for val in vals:
            refcount[val] += 1

    cache = cache or {}
    from dask.order import order

    priorities = order(dsk)

    for key, node in sorted(dsk.items(), key=lambda it: priorities[it[0]]):
        cache[key] = node(cache)
        for dep in node.dependencies:
            refcount[dep] -= 1
            if refcount[dep] == 0 and keys and dep not in keys:
                del cache[dep]

    return cache
