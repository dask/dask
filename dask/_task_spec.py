from __future__ import annotations

""" Task specification for dask

This module contains the task specification for dask. It is used to represent
runnable and non-runnable (i.e. literals) tasks in a dask graph.

Simple examples of how to express tasks in dask
-----------------------------------------------

.. code-block:: python

    func("a", "b") ~ Task("key", func, "a", "b")

    [func("a"), func("b")] ~ SequenceOfTasks(
        "key",
        [Task("key-1", func, "a"), Task("key-2", func, "b")]
    )

    {"a": func("b")} ~ DictOfTasks("key", {"a": Task("a", func, "b")})

    "literal-string" ~ Literal("key", "literal-string")


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

    {"a": func("b")} ~ DictOfTasks("key", {"a": Task("a", func, "b")})

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
from typing import TYPE_CHECKING, Any, overload

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


if TYPE_CHECKING:
    from typing import TypeVar

    _T = TypeVar("_T", bound="GraphNode")


def identity(*args):
    return args


_anom_count = itertools.count()


@overload
def parse_input(obj: dict) -> DictOfTasks | DataNode: ...


@overload
def parse_input(
    obj: list | tuple | set | frozenset,
) -> SequenceOfTasks | DataNode: ...


@overload
def parse_input(obj: TaskRef) -> Alias: ...


@overload
def parse_input(obj: _T) -> _T: ...


def parse_input(obj: Any) -> GraphNode:
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
        return DictOfTasks(None, obj).propagate_literal()
    elif isinstance(obj, (list, tuple, set, frozenset)):
        if is_namedtuple_instance(obj):
            return _wrap_namedtuple_task(None, obj, parse_input)
        return SequenceOfTasks(None, obj).propagate_literal()

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

    args_converted = SequenceOfTasks(
        None, map(parser, new_args), typ=type(new_args)
    ).propagate_literal()

    return Task(
        k, partial(_instantiate_named_tuple, type(obj)), *args_converted, **kwargs
    )


def _instantiate_named_tuple(typ, *args, **kwargs):
    return typ(*args, **kwargs)


class _MultiSet(Container):
    sets: tuple
    __slots__ = ("sets",)

    def __init__(self, *sets):
        self.sets = sets

    def __contains__(self, o: object) -> bool:
        return any(o in s for s in self.sets)


SubgraphType = None


def convert_legacy_task(
    k: KeyType | None,
    arg: Any,
    all_keys: Container,
    only_refs: bool,
    cache: MutableMapping,
) -> GraphNode:
    global SubgraphType
    if SubgraphType is None:
        from dask.optimization import SubgraphCallable

        SubgraphType = SubgraphCallable

    if isinstance(arg, GraphNode):
        return arg
    rv: GraphNode
    inner = partial(convert_legacy_task, None, only_refs=only_refs, cache=cache)

    inner_args = partial(inner, all_keys=all_keys)
    if type(arg) is tuple and arg and callable(arg[0]):
        if k is None:
            k = ("anom", next(_anom_count))

        func, args = arg[0], arg[1:]
        if isinstance(func, SubgraphType):
            # FIXME: This parsing is inlining tasks of the subgraph, i.e. it can
            # multiply work if the subgraph has keys that are reused in the
            # subgraph
            subgraph = func
            all_keys_inner = _MultiSet(all_keys, set(subgraph.inkeys), subgraph.dsk)
            if subgraph.name in cache:
                func = cache[subgraph.name]
            else:
                sub_dsk = convert_legacy_graph(
                    subgraph.dsk, all_keys_inner, only_refs, cache
                )
                cache[subgraph.name] = func = sub_dsk[subgraph.outkey].inline(sub_dsk)
                del sub_dsk

            new_args_l = map(partial(inner, all_keys=all_keys_inner), args)

            func2 = func.inline(dict(zip(subgraph.inkeys, new_args_l)))
            dct = {k: Alias(k) for k in func2.dependencies}
            return Task(k, func2, dct)
        else:
            new_args = map(inner_args, args)
            if only_refs:
                # this can't be a literal since the literal wouldn't execute
                # new_args in case there were runnable tasks or other literals
                # See test_convert_task_only_ref
                return Task(k, identity, func, *new_args)
            return Task(k, func, *new_args)
    try:
        if isinstance(arg, (bytes, int, float, str, tuple)) and arg in all_keys:
            if arg in cache:
                return cache[arg]
            cache[arg] = rv = Alias(arg)
            return rv
    except TypeError:
        # Unhashable
        pass

    if isinstance(arg, (list, tuple, set, frozenset)):
        if is_namedtuple_instance(arg):
            rv = _wrap_namedtuple_task(k, arg, inner_args)
        else:
            rv = SequenceOfTasks(k, map(inner_args, arg), typ=type(arg))
        return rv.propagate_literal()
    elif isinstance(arg, dict):
        new_dsk = convert_legacy_graph(
            arg, all_keys=all_keys, only_refs=True, cache=cache
        )

        return DictOfTasks(None, new_dsk).propagate_literal()

    elif isinstance(arg, TaskRef):
        return Alias(arg.key)
    else:
        return arg


def convert_legacy_graph(
    dsk: Mapping,
    all_keys: Container | None = None,
    only_refs: bool = False,
    cache: MutableMapping | None = None,
):
    if not all_keys:
        all_keys = set(dsk)
    new_dsk = {}
    if cache is None:
        cache = {}
    for k, arg in dsk.items():
        t = convert_legacy_task(k, arg, all_keys, only_refs, cache=cache)
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
    dependencies: set | frozenset | tuple

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

    def propagate_literal(self) -> GraphNode:
        """Return a literal if all tasks are literals"""
        return self

    def __call__(self, values) -> Any:
        raise NotImplementedError("Not implemented")

    @property
    def is_coro(self) -> bool:
        return False

    def __sizeof__(self) -> int:
        return sys.getsizeof(type(self)) + sizeof(self.key) + sizeof(self.dependencies)


_no_deps: set = set()


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
        return f"Literal({self.key}, type={self.typ}, {self.value})"

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


class Task(GraphNode):
    func: Callable | None
    args: SequenceOfTasks | DataNode
    kwargs: DictOfTasks | DataNode
    packed_func: None | bytes
    _token: tuple | None
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
        dependencies: set = set()
        self.args = parse_input(args)
        dependencies.update(self.args.dependencies)
        self.kwargs = parse_input(kwargs)
        dependencies.update(self.kwargs.dependencies)
        if dependencies:
            self.dependencies = dependencies
        else:
            self.dependencies = _no_deps
        self._is_coro = None
        self._token = None

    def copy(self):
        self.unpack()
        kwargs = self.kwargs.value if isinstance(self.kwargs, DataNode) else self.kwargs
        return Task(self.key, self.func, *self.args, **kwargs)

    def __hash__(self):
        return hash(self._get_token())

    def is_packed(self):
        return self.packed_func is not None

    def _get_token(self):
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
        new_argspec = self.args(values)
        if self.kwargs:
            kwargs = self.kwargs(values)
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
        new_args = self.args.inline(dsk)
        new_kwargs = self.kwargs.inline(dsk)
        if isinstance(new_kwargs, DataNode):
            kwargs = new_kwargs.value
        else:
            kwargs = new_kwargs.tasks
        assert self.func is not None
        return Task(self.key, self.func, *new_args, **kwargs)

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


class DictOfTasks(GraphNode):
    tasks: dict
    __slots__ = tuple(__annotations__)

    def __init__(self, key: Any, nested_tasks: dict):
        self.key = key
        self.dependencies = set()
        self.tasks = {}
        for k, task in nested_tasks.items():
            self.tasks[k] = task = parse_input(task)
            if isinstance(task, GraphNode):
                self.dependencies.update(task.dependencies)

    def inline(self, dsk) -> DictOfTasks:
        new_tasks = {k: task.inline(dsk) for k, task in self.tasks.items()}
        return DictOfTasks(self.key, new_tasks)

    def __iter__(self):
        return iter(self.tasks)

    def __call__(self, values=()) -> dict:
        self._verify_values(values)
        return {
            k: task(values) if isinstance(task, GraphNode) else task
            for k, task in self.tasks.items()
        }

    def copy(self):
        return DictOfTasks(self.key, self.tasks)

    def propagate_literal(self) -> GraphNode:
        """Return a literal if all tasks are literals"""
        if any(
            isinstance(t, GraphNode) and not isinstance(t, DataNode)
            for t in self.tasks.values()
        ):
            return self
        return DataNode(self.key, self())

    def __sizeof__(self):
        return super().__sizeof__() + sizeof(self.tasks)


class SequenceOfTasks(GraphNode):
    typ: type
    _isnamed_tuple: bool
    tasks: list | tuple
    _constructor_kwargs: dict

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        key: Any,
        nested_tasks: list | tuple | Iterable,
        typ: type | None = None,
    ):
        self.key = key
        self.dependencies = set()
        self.tasks = []
        self.typ = typ or type(nested_tasks)
        for task in nested_tasks:
            task = parse_input(task)
            self.tasks.append(task)
            if isinstance(task, GraphNode):
                self.dependencies.update(task.dependencies)

    def __iter__(self):
        return iter(self.tasks)

    def copy(self):
        return SequenceOfTasks(self.key, self.tasks, self.typ)

    def inline(self, dsk: dict) -> SequenceOfTasks | DataNode:
        new_tasks = []
        all_literals = True
        for task in self.tasks:
            if not isinstance(task, GraphNode):
                new_tasks.append(task)
                continue
            new_tasks.append(inner := task.inline(dsk))
            if all_literals and not isinstance(inner, DataNode):
                all_literals = False
        rv = SequenceOfTasks(self.key, new_tasks)
        if all_literals:
            return DataNode(self.key, rv())
        return rv

    def __dask_tokenize__(self):
        return (type(self).__name__, tokenize(self.tasks))

    def __call__(self, values=()) -> list | tuple:
        self._verify_values(values)
        return self.typ(
            task(values) if isinstance(task, GraphNode) else task for task in self.tasks
        )

    def propagate_literal(self):
        """Return a literal if all tasks are literals"""
        if any(
            isinstance(t, GraphNode) and not isinstance(t, DataNode) for t in self.tasks
        ):
            return self
        return DataNode(self.key, self())

    def __sizeof__(self):
        return super().__sizeof__() + sizeof(self.tasks) + sizeof(self.typ)


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
