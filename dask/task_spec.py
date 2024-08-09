from __future__ import annotations

""" Task specification for dask

This module contains the task specification for dask. It is used to represent
runnable and non-runnable (i.e. literals) tasks in a dask graph.

Simple examples of how to express tasks in dask
-----------------------------------------------

.. code-block:: python

    func("a", "b") ~ Task("key", func, ["a", "b"])

    [func("a"), func("b")] ~ SequenceOfTasks(
        "key",
        [Task("key-1", func, "a"), Task("key-2", func, "b")]
    )

    {"a": func("b")} ~ DictOfTasks("key", {"a": Task("a", func, "b")})

    "literal-string" ~ Literal("key", "literal-string")


Keys, Aliases and KeyRefs
-------------------------

Keys are used to identify tasks in a dask graph. Every `BaseTask` instance has a
key attribute that _should_ reference the key in the dask graph.

.. code-block:: python

    {"key": Task("key", func, "a")}

Referencing other tasks is possible by using either one of `Alias` or a
`KeyRef`. (both can be used interchangeably; The KeyRef is a proxy for the
distributed WrappedKey subclass and is used here as a stand-in replacement. The
implementation deals with both transparently).

.. code-block:: python

    # KeyRef can be used to provide the name of the reference explicitly
    t = Task("key", func, KeyRef("key-1"))

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
    t2 = Task("key-2", func, KeyRef("key-1"))

    t2.inline({"key-1": t1}) ~ Task("key-2", func, Task("key-1", func, "a"))

Executing a task
----------------

A task can be executed by calling it with a dictionary of values. The values
should contain the dependencies of the task.

.. code-block:: python

    t = Task("key", add, [KeyRef("a"), KeyRef("b")])
    assert t.dependencies == {"a", "b"}
    t({"a": 1, "b": 2}) == 3

"""

import itertools
import pickle
import weakref
from collections import defaultdict
from collections.abc import Callable, Container, Iterable, Mapping, MutableMapping
from functools import partial
from typing import TYPE_CHECKING, Any, overload

import cloudpickle

from dask.base import normalize_token
from dask.core import reverse_dict
from dask.typing import Key as KeyType
from dask.utils import is_namedtuple_instance

if TYPE_CHECKING:
    from typing import TypeVar

    _T = TypeVar("_T", bound="BaseTask")


class WrappedKey:
    """Interface for a key in a dask graph.

    Subclasses must have .key attribute that refers to a key in a dask graph.

    Sometimes we want to associate metadata to keys in a dask graph.  For
    example we might know that that key lives on a particular machine or can
    only be accessed in a certain way.  Schedulers may have particular needs
    that can only be addressed by additional metadata.
    """

    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return f"{type(self).__name__}('{self.key}')"


def identity(*args):
    return args


_anom_count = itertools.count()


@overload
def parse_input(obj: dict) -> DictOfTasks:
    ...


@overload
def parse_input(obj: list | tuple | set | frozenset) -> SequenceOfTasks:
    ...


@overload
def parse_input(obj: KeyRef | WrappedKey) -> Alias:
    ...


@overload
def parse_input(obj: _T) -> _T:
    ...


def parse_input(obj: Any) -> BaseTask:
    """Tokenize user input into BaseTask objects

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
    if isinstance(obj, BaseTask):
        return obj
    elif isinstance(obj, dict):
        return DictOfTasks(None, obj).propagate_literal()
    elif isinstance(obj, (list, tuple, set, frozenset)):
        if is_namedtuple_instance(obj):
            raise NotImplementedError("Namedtuples are not supported")
        return SequenceOfTasks(None, obj).propagate_literal()

    if isinstance(obj, (WrappedKey, KeyRef)):
        return Alias(obj.key)

    # TODO: We should be able to return obj which should also be much faster
    return LiteralTask(None, obj)


def _instantiate_named_tuple(typ, *args, **kwargs):
    return typ(*args, **kwargs)


class _MultiSet(Container):
    sets: tuple
    __slots__ = ("sets",)

    def __init__(self, *sets):
        self.sets = sets

    def __contains__(self, o: object) -> bool:
        return any(o in s for s in self.sets)


def convert_old_style_task(
    k: KeyType | None,
    arg: Any,
    all_keys: Container,
    only_refs: bool,
    cache: MutableMapping,
) -> BaseTask:
    from dask.optimization import SubgraphCallable

    if isinstance(arg, BaseTask):
        return arg
    rv: BaseTask
    inner = partial(convert_old_style_task, None, only_refs=only_refs, cache=cache)

    inner_args = partial(inner, all_keys=all_keys)
    if type(arg) is tuple and arg and callable(arg[0]):
        if k is None:
            k = ("anom", next(_anom_count))

        func, args = arg[0], arg[1:]
        if isinstance(func, SubgraphCallable):
            # FIXME: This parsing is inlining tasks of the subgraph, i.e. it can
            # multiply work if the subgraph has keys that are reused in the
            # subgraph
            subgraph = func
            all_keys_inner = _MultiSet(all_keys, set(subgraph.inkeys), subgraph.dsk)
            if subgraph.name in cache:
                func = cache[subgraph.name]
            else:
                sub_dsk = convert_old_style_dsk(
                    subgraph.dsk, all_keys_inner, only_refs, cache
                )
                cache[subgraph.name] = func = sub_dsk[subgraph.outkey].inline(sub_dsk)
                del sub_dsk

            new_args_l = map(partial(inner, all_keys=all_keys_inner), args)

            func2 = func.inline(dict(zip(subgraph.inkeys, new_args_l)))
            dct = {k: Alias(k) for k in func2.dependencies}
            return Task(key=k, func=func2, args=[dct])
        else:
            new_args = map(inner_args, args)
            if only_refs:
                # this can't be a literal since the literal wouldn't execute
                # new_args in case there were runnable tasks or other literals
                # See test_convert_task_only_ref
                return Task(k, identity, (func, *new_args))
            return Task(key=k, func=func, args=tuple(new_args))
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
            if hasattr(arg, "__getnewargs_ex__"):
                new_args, kwargs = arg.__getnewargs_ex__()
                kwargs = DictOfTasks(
                    None,
                    {k: inner_args(v) for k, v in kwargs.items()},
                )
            elif hasattr(arg, "__getnewargs__"):
                new_args = arg.__getnewargs__()
                kwargs = {}

            args_converted = SequenceOfTasks(
                None, map(inner_args, new_args), typ=type(new_args)
            )

            rv = Task(
                k, partial(_instantiate_named_tuple, type(arg)), args_converted, kwargs
            )
        else:
            rv = SequenceOfTasks(k, map(inner_args, arg), typ=type(arg))
        return rv.propagate_literal()
    elif isinstance(arg, dict):
        new_dsk = convert_old_style_dsk(
            arg, all_keys=all_keys, only_refs=True, cache=cache
        )

        return DictOfTasks(None, new_dsk).propagate_literal()

    elif isinstance(arg, WrappedKey):
        return Alias(arg.key)
    else:
        return arg


def convert_old_style_dsk(
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
        t = convert_old_style_task(k, arg, all_keys, only_refs, cache=cache)
        if not only_refs and isinstance(t, Alias) and t.key == k:
            continue
        new_dsk[k] = t
    new_dsk2 = {
        k: v if isinstance(v, BaseTask) else LiteralTask(k, v)
        for k, v in new_dsk.items()
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
        if (
            isinstance(t, Alias)
            and t.key not in keys
            and t.key != k
            and t.key in dsk
            and len(dependents[t.key]) == 1
        ):
            t = dsk[k] = dsk.pop(t.key)
            seen.discard(k)
            if isinstance(t, Alias):
                work.append(k)

        work.extend(t.dependencies)
    return dsk


class KeyRef:
    val: KeyType
    __slots__ = ("key",)

    def __init__(self, key: KeyType):
        self.key = key

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return f"Key({repr(self.key)})"

    def __eq__(self, value: object) -> bool:
        if isinstance(value, KeyRef):
            return self.key == value.key
        return self.key == value

    def __hash__(self) -> int:
        return hash(self.key)


# TODO: Consider a weakvalue dict or clean up otherwise
# This would of course not work if we maintained a two way cache
_func_cache: dict = {}
_func_cache_reverse: dict = {}


class BaseTask:
    key: KeyType
    dependencies: set | frozenset | tuple

    __slots__ = tuple(__annotations__)

    def ref(self):
        return Alias(self.key)

    def _verify_values(self, values: tuple | dict) -> None:
        if not self.dependencies:
            return
        if missing := set(self.dependencies) - set(values):
            raise RuntimeError(f"Not enough arguments provided: missing keys {missing}")

    def inline(self, dsk) -> BaseTask:
        raise NotImplementedError("Not implemented")

    def propagate_literal(self) -> BaseTask:
        """Return a literal if all tasks are literals"""
        return self

    def __call__(self, values) -> Any:
        raise NotImplementedError("Not implemented")

    @property
    def is_coro(self) -> bool:
        return False


_no_deps: set = set()


class Alias(BaseTask):
    __weakref__: Any = None
    __slots__ = tuple(__annotations__)

    _instances: weakref.WeakValueDictionary = weakref.WeakValueDictionary()

    def __new__(cls, key):
        # TODO: This needs more profiling. the key in instances check plus
        # adding the instance to the weakref dict is not for free and recent
        # profiling might suggest this is all a zero sum game
        # We create many of these and the initialization is relatively expensive
        # for something that isn't offering much value (the set is "expensive")
        if key in cls._instances:
            return cls._instances[key]
        return super().__new__(cls)

    def __init__(self, key: KeyRef | KeyType):
        if isinstance(key, KeyRef):
            key = key.key
        self.key = key
        self.dependencies = {key}
        Alias._instances[key] = self

    def __reduce__(self) -> str | tuple[Any, ...]:
        return Alias, (self.key,)

    def __call__(self, values=()):
        self._verify_values(values)
        return values[self.key]

    def inline(self, dsk) -> BaseTask:
        if self.key in dsk:
            # This can otherwise cause recursion errors
            new_dsk = dsk.copy()
            new_dsk.pop(self.key)
            if isinstance(dsk[self.key], BaseTask):
                return dsk[self.key].inline(new_dsk)
            else:
                return dsk[self.key]
        else:
            return self

    def __repr__(self):
        return f"Alias({self.key})"

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Alias):
            return False
        if self.key != value.key:
            return False
        if self.key != value.key:
            return False
        return True


class LiteralTask(BaseTask):
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

    def inline(self, dsk) -> LiteralTask:
        return self

    def __call__(self, values=()):
        return self.value

    def __repr__(self):
        return f"Literal({self.key}, type={self.typ}, {self.value})"

    def __dask_tokenize__(self):
        return (type(self).__name__, normalize_token(self.value))

    def __reduce__(self) -> str | tuple[Any, ...]:
        return LiteralTask, (self.key, self.value)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, LiteralTask):
            return False
        if self.key != value.key:
            return False
        if self.value != value.value:
            return False
        return True


class Task(BaseTask):
    func: Callable | None
    args: SequenceOfTasks | LiteralTask
    kwargs: DictOfTasks | None
    packed_func: None | bytes
    _token: tuple | None
    _is_coro: bool | None

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        key: Any,
        func: Callable,
        args: list | tuple | LiteralTask | SequenceOfTasks,
        kwargs: dict | DictOfTasks | None = None,
    ):
        self.key = key
        self.func = func
        self.packed_func = None
        dependencies: set = set()
        self.args = parse_input(args)
        dependencies.update(self.args.dependencies)
        self.kwargs = None
        if kwargs is not None:
            self.kwargs = parse_input(kwargs)
            dependencies.update(self.kwargs.dependencies)
        if dependencies:
            self.dependencies = dependencies
        else:
            self.dependencies = _no_deps
        self._is_coro = None
        self._token = None

    def __hash__(self):
        return hash(self._get_token())

    def is_packed(self):
        return self.packed_func is not None

    def _get_token(self):
        if self._token:
            return self._token
        self.unpack()
        from dask.base import normalize_token

        self._token = normalize_token(
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
        import pickle

        import cloudpickle

        try:
            self.packed_func = _func_cache[self.func]
        except (KeyError, TypeError):
            try:
                self.packed_func = pickle.dumps(self.func)
            except Exception:
                self.packed_func = cloudpickle.dumps(self.func)
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
        new_kwargs = None
        if self.kwargs:
            new_kwargs = self.kwargs.inline(dsk)
        assert self.func is not None
        return Task(self.key, self.func, new_args, new_kwargs)

    def unpack(self):
        if not self.is_packed():
            return
        assert self.packed_func is not None

        try:
            self.func = _func_cache_reverse[self.packed_func]
        except KeyError:
            try:
                self.func = pickle.loads(self.packed_func)
            except Exception:
                self.func = cloudpickle.loads(self.packed_func)
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


class DictOfTasks(BaseTask):
    tasks: dict
    __slots__ = tuple(__annotations__)

    def __init__(self, key: Any, nested_tasks: dict):
        self.key = key
        self.dependencies = set()
        self.tasks = {}
        for k, task in nested_tasks.items():
            if isinstance(task, KeyRef):
                task = Alias(task)
            self.tasks[k] = task
            if isinstance(task, BaseTask):
                self.dependencies.update(task.dependencies)

    def inline(self, dsk) -> DictOfTasks:
        new_tasks = {k: task.inline(dsk) for k, task in self.tasks.items()}
        return DictOfTasks(self.key, new_tasks)

    def __call__(self, values=()) -> dict:
        self._verify_values(values)
        return {
            k: task(values) if isinstance(task, BaseTask) else task
            for k, task in self.tasks.items()
        }

    def propagate_literal(self) -> BaseTask:
        """Return a literal if all tasks are literals"""
        if any(
            isinstance(t, BaseTask) and not isinstance(t, LiteralTask)
            for t in self.tasks.values()
        ):
            return self
        return LiteralTask(self.key, self())


class SequenceOfTasks(BaseTask):
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
            if isinstance(task, BaseTask):
                self.dependencies.update(task.dependencies)

    def inline(self, dsk: dict) -> SequenceOfTasks | LiteralTask:
        new_tasks = []
        all_literals = True
        for task in self.tasks:
            if not isinstance(task, BaseTask):
                continue
            new_tasks.append(inner := task.inline(dsk))
            if all_literals and not isinstance(inner, LiteralTask):
                all_literals = False
        rv = SequenceOfTasks(self.key, new_tasks)
        if all_literals:
            return LiteralTask(self.key, rv())
        return rv

    def __call__(self, values=()) -> list | tuple:
        self._verify_values(values)
        return self.typ(
            task(values) if isinstance(task, BaseTask) else task for task in self.tasks
        )

    def propagate_literal(self):
        """Return a literal if all tasks are literals"""
        if any(
            isinstance(t, BaseTask) and not isinstance(t, LiteralTask)
            for t in self.tasks
        ):
            return self
        return LiteralTask(self.key, self())


class DependenciesMapping(Mapping):
    def __init__(self, dsk):
        self.dsk = dsk
        self.blocklist = None
        self.removed_keys = set()

    def __getitem__(self, key):
        if key in self.removed_keys:
            raise KeyError(key)
        v = self.dsk[key]
        if not isinstance(v, BaseTask):
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
    assert all(isinstance(v, BaseTask) for v in dsk.values())
    dependencies = DependenciesMapping(dsk)
    dependents = reverse_dict(dependencies)
    return dependencies, dependents
