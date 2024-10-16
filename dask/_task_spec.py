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
from contextlib import contextmanager
from functools import partial
from typing import Any, TypeVar, cast, overload

from dask.base import tokenize
from dask.core import reverse_dict
from dask.sizeof import sizeof
from dask.typing import Key as KeyType
from dask.utils import is_namedtuple_instance

try:
    from distributed.collections import LRU
except ImportError:

    class LRU(dict):  # type: ignore[no-redef]
        def __init__(self, *args, maxsize=None, **kwargs):
            super().__init__(*args, **kwargs)


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
    res = execute_graph(final)
    return res[outkey]


def convert_legacy_task(
    k: KeyType | None,
    task: _T,
    all_keys: Container,
    only_refs: bool,
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
            converted_graph = convert_legacy_graph(sub_dsk, all_keys_inner, only_refs)
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
                    k: convert_legacy_task(None, target, all_keys, only_refs)
                    for k, target in zip(subgraph.inkeys, args)
                },
                {ext_dep: Alias(ext_dep) for ext_dep in external_deps},
            )
        else:
            new_args = tuple(
                convert_legacy_task(None, a, all_keys, only_refs) for a in args
            )
            if only_refs:
                if any(isinstance(a, GraphNode) for a in new_args):
                    return Task(k, identity, func, *new_args)
                else:
                    return DataNode(k, (func, *new_args))
            return Task(k, func, *new_args)
    try:
        if isinstance(task, (bytes, int, float, str, tuple)):
            if task in all_keys:
                return Alias(task)
    except TypeError:
        # Unhashable
        pass

    if isinstance(task, (list, tuple, set, frozenset)):
        if is_namedtuple_instance(task):
            return _wrap_namedtuple_task(
                k,
                task,
                partial(
                    convert_legacy_task, None, all_keys=all_keys, only_refs=only_refs
                ),
            )
        else:
            parsed_args = tuple(
                convert_legacy_task(None, t, all_keys, only_refs) for t in task
            )
            if any(isinstance(a, GraphNode) for a in parsed_args):
                return Task(
                    k, _identity_cast, *parsed_args, typ=DataNode(None, type(task))
                )
            else:
                return cast(_T, type(task)(parsed_args))
    elif isinstance(task, dict):
        return _wrap_dict_in_task(
            {
                k: convert_legacy_task(k, v, all_keys, only_refs=True)
                for k, v in task.items()
            }
        )

    elif isinstance(task, TaskRef):
        return Alias(task.key)
    else:
        return task


def convert_legacy_graph(
    dsk: Mapping,
    all_keys: Container | None = None,
    only_refs: bool = False,
):
    if not all_keys:
        all_keys = set(dsk)
    new_dsk = {}
    for k, arg in dsk.items():
        t = convert_legacy_task(k, arg, all_keys, only_refs)
        if not only_refs and isinstance(t, Alias) and t.key == k:
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


_func_cache: MutableMapping = LRU(maxsize=1000)
_func_cache_reverse: MutableMapping = LRU(maxsize=1000)


class GraphNode:
    key: KeyType
    dependencies: set | frozenset

    __slots__ = tuple(__annotations__)

    def ref(self):
        return Alias(self.key)

    def copy(self):
        raise NotImplementedError

    def _verify_values(self, values: tuple | dict) -> None:
        if not self.dependencies:
            return
        if missing := set(self.dependencies) - set(values):
            raise RuntimeError(f"Not enough arguments provided: missing keys {missing}")

    def inline(self, dsk) -> GraphNode:
        raise NotImplementedError("Not implemented")

    def __call__(self, values) -> Any:
        raise NotImplementedError("Not implemented")

    @property
    def is_coro(self) -> bool:
        return False

    def __sizeof__(self) -> int:
        return sys.getsizeof(type(self)) + sizeof(self.key) + sizeof(self.dependencies)


_no_deps: frozenset = frozenset()


class Alias(GraphNode):
    __weakref__: Any = None
    _dependencies: set | None
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
        self._dependencies = None

    @property
    def dependencies(self):
        if self._dependencies is None:
            self._dependencies = {self.target.key}
        return self._dependencies

    def copy(self):
        return Alias(self.key, self.target)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return Alias, (self.key, self.target)

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
        return f"Alias(key={self.key}, target={self.target})"

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Alias):
            return False
        if self.key != value.key:
            return False
        if self.key != value.key:
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
        self.dependencies = _no_deps

    def inline(self, dsk) -> DataNode:
        return self

    def copy(self):
        return DataNode(self.key, self.value)

    def __call__(self, values=()):
        return self.value

    def __repr__(self):
        return f"DataNode({self.key}, type={self.typ}, {self.value})"

    def __dask_tokenize__(self):
        return (type(self).__name__, tokenize(self.value))

    def __reduce__(self) -> str | tuple[Any, ...]:
        return DataNode, (self.key, self.value)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, DataNode):
            return False
        if self.value != value.value:
            return False
        return True

    def __sizeof__(self) -> int:
        return super().__sizeof__() + sizeof(self.value) + sizeof(self.typ)

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
    func: Callable | None
    args: tuple
    kwargs: dict
    packed_func: None | bytes
    _token: str | None
    _is_coro: bool | None

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
        self.packed_func = None
        self.args = parse_input(args)
        self.kwargs = parse_input(kwargs)
        dependencies: set = set()
        dependencies.update(_get_dependencies(self.args))
        dependencies.update(_get_dependencies(tuple(self.kwargs.values())))
        if dependencies:
            self.dependencies = dependencies
        else:
            self.dependencies = _no_deps
        self._is_coro = None
        self._token = None

    def copy(self):
        self.unpack()
        return Task(self.key, self.func, *self.args, **self.kwargs)

    def __hash__(self):
        return hash(self._get_token())

    def is_packed(self):
        return self.packed_func is not None

    def _get_token(self) -> str:
        if self._token:
            return self._token
        self.unpack()
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

    def __sizeof__(self) -> int:
        return (
            super().__sizeof__()
            + sizeof(self.func or self.packed_func)
            + sizeof(self.args)
            + sizeof(self.kwargs)
        )

    def __repr__(self) -> str:
        return f"Task({self.key!r})"

    def __call__(self, values=()):
        self._verify_values(values)
        try:
            self.unpack()
        except Exception as exc:
            raise RuntimeError(
                f"Exception occured during deserialization of function for task {self.key}"
            ) from exc
        assert self.func is not None
        new_argspec = _call_recursively(self.args, values)
        if self.kwargs:
            kwargs = _call_recursively(self.kwargs, values)
            return self.func(*new_argspec, **kwargs)
        return self.func(*new_argspec)

    def pack(self):
        # TODO: pack args and kwargs as well. Probably with a sizeof threshold
        if self.is_packed():
            return self
        try:
            from distributed.protocol.pickle import dumps
        except ImportError:
            from cloudpickle import dumps

        try:
            self.packed_func = _func_cache[self.func]
            self.func = None
            return self
        except (KeyError, TypeError):
            # We're not handling the below in this except to simplify traceback
            # and cause
            pass
        try:
            self.packed_func = dumps(self.func)
        except Exception as exc:
            raise RuntimeError(
                f"Error during serialization of function of task {self.key}"
            ) from exc
        try:
            _func_cache_reverse[self.packed_func], _func_cache[self.func] = (
                self.func,
                self.packed_func,
            )
        except TypeError:
            pass
        self.func = None
        return self

    def lazy_pack(self):
        # TODO: This could dispatch to a TPE and de/serialize the arguments
        # lazily such that pack only waits for the result. This way we can use
        # spare CPU cycles and parallelize on multiple CPUs.
        # Thread safety should not be an issue if we guarantee idempotency and
        # that the non-lazy sync also uses this
        # This may block the GIL pretty aggressively
        raise NotImplementedError("Not implemented")

    def lazy_unpack(self):
        raise NotImplementedError("Not implemented")

    def inline(self, dsk) -> Task:
        self.unpack()
        new_args = _inline_recursively(self.args, dsk)
        new_kwargs = _inline_recursively(self.kwargs, dsk)
        assert self.func is not None
        return Task(self.key, self.func, *new_args, **new_kwargs)

    def unpack(self):
        if not self.is_packed():
            return
        assert self.packed_func is not None

        try:
            from distributed.protocol.pickle import loads
        except ImportError:
            from cloudpickle import loads
        try:
            self.func = _func_cache_reverse[self.packed_func]
            self.packed_func = None
            return self
        except KeyError:
            # We're not handling the below in this except to simplify traceback
            # and cause
            pass
        try:
            self.func = loads(self.packed_func)
        except Exception as exc:
            raise RuntimeError(
                f"Error during deserialization of function of task {self.key}"
            ) from exc
        try:
            _func_cache_reverse[self.packed_func], _func_cache[self.func] = (
                self.func,
                self.packed_func,
            )
        except TypeError:
            pass
        self.packed_func = None
        return self

    def __getstate__(self):
        self.pack()
        return {
            "key": self.key,
            "packed_func": self.packed_func,
            "dependencies": self.dependencies,
            "kwargs": self.kwargs,
            "args": self.args,
            "_is_coro": self._is_coro,
            "_token": self._token,
        }

    def __setstate__(self, state):
        self.key = state["key"]
        self.packed_func = state["packed_func"]
        self.dependencies = state["dependencies"]
        self.kwargs = state["kwargs"]
        self.args = state["args"]
        self._is_coro = state["_is_coro"]
        self._token = state["_token"]
        self.func = None

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Task):
            return False
        if self.key != value.key:
            return False
        if self.packed_func != value.packed_func:
            return False
        if self.func != value.func:
            return False
        if self.dependencies != value.dependencies:
            return False
        if self.kwargs != value.kwargs:
            return False
        if self.args != value.args:
            return False
        return True

    @property
    def is_coro(self):
        if self._is_coro is None:
            self.unpack()
            # Note: Can't use cached_property on objects without __dict__
            try:
                from distributed.utils import iscoroutinefunction

                self._is_coro = iscoroutinefunction(self.func)
            except Exception:
                self._is_coro = False
        return self._is_coro


class DependenciesMapping(Mapping):
    def __init__(self, dsk):
        self.dsk = dsk
        self.blocklist = None
        self.removed_keys = set()

    def __getitem__(self, key):
        if key in self.removed_keys:
            raise KeyError(key)
        v = self.dsk[key]
        if not isinstance(v, GraphNode):
            from dask.core import get_dependencies

            return get_dependencies(self.dsk, task=self.dsk[key])
        if self.blocklist and self.blocklist[key]:
            return self.dsk[key].dependencies - self.blocklist[key]
        return self.dsk[key].dependencies

    def __iter__(self):
        return iter(self.dsk)

    def copy(self):
        return DependenciesMapping(self.dsk)

    def __delitem__(self, key):
        self.removed_keys.add(key)

    def remove_dependency(self, key, dep):
        if self.blocklist is None:
            self.blocklist = defaultdict(set)
        self.blocklist[key].add(dep)

    def __len__(self) -> int:
        return len(self.dsk)


def get_deps(dsk):
    # FIXME: I think we don't need this function
    assert all(isinstance(v, GraphNode) for v in dsk.values())
    dependencies = DependenciesMapping(dsk)
    dependents = reverse_dict(dependencies)
    return dependencies, dependents


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


@contextmanager
def no_function_cache():
    """Everything in this context will ignore the function cache on both
    serialization and deserialization.

    This is not threadsafe!
    """
    global _func_cache, _func_cache_reverse
    cache_before = _func_cache, _func_cache_reverse
    _func_cache, _func_cache_reverse = _DevNullMapping(), _DevNullMapping()
    try:
        yield
    finally:
        _func_cache, _func_cache_reverse = cache_before


def execute_graph(dsk: list[GraphNode] | dict[KeyType, GraphNode]) -> dict:
    if isinstance(dsk, (list, tuple, set, frozenset)):
        dsk = {t.key: t for t in dsk}
    else:
        assert isinstance(dsk, dict)
    dependencies = dict(DependenciesMapping(dsk))

    refcount: defaultdict[KeyType, int] = defaultdict(int)
    for vals in dependencies.values():
        for val in vals:
            refcount[val] += 1

    cache: dict[KeyType, object] = {}
    from dask.order import order

    priorities = order(dsk)

    for key, node in sorted(dsk.items(), key=lambda it: priorities[it[0]]):
        cache[key] = node(cache)
        for dep in node.dependencies:
            refcount[dep] -= 1
            if refcount[dep] == 0:
                del cache[dep]

    return cache
