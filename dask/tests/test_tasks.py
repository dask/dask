from collections import namedtuple
import pickle

import pytest

from dask.core import Task, get

from dask.task import EmptyDict
from dask.utils import apply

def inc(x, extra=None):
    if extra:
        return x + 1 + extra

    return x + 1


def test_empty_dict():
    d = EmptyDict()

    with pytest.raises(NotImplementedError):
        d["a"] = 5


def test_task_pickle():
    task = Task.from_call(inc, 1, extra=.1, annotations={'resource': 'GPU'})
    assert pickle.loads(pickle.dumps(task)) == task


def test_task_repr_and_str():
    task_1 = Task.from_call(inc, 1, extra=.1, annotations={'resource': 'GPU'})
    task_2 = Task.from_call(inc, task_1)
    assert str(task_2) == "inc(inc(1, extra=0.1, annotions={'resource': 'GPU'}))"
    assert repr(task_2) == "Task(inc, Task(inc, 1, extra=0.1, annotions={'resource': 'GPU'}))"


def test_task_get():
    task_1 = Task.from_call(inc, 1, extra=.1, annotations={'resource': 'GPU'})
    task_2 = Task.from_call(inc, task_1)
    dsk = {'x': task_2}
    assert get(dsk, 'x') == 3.1


def test_task_from_call():
    # Simple task tuple
    task = Task.from_call(inc, 1)
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {}
    assert task.annotations == {}

    task = Task.from_call(inc, 1, extra=.1)
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {'extra': .1}
    assert task.annotations == {}

    task = Task.from_call(inc, 1, extra=.1, annotations={'a': 1})
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {'extra': .1}
    assert task.annotations == {'a': 1}

    # Apply task, (apply, function)
    task = Task.from_call(apply, inc)

    # Apply task tuple, (apply, function, args)
    task = Task.from_call(apply, inc, (1,))
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {}
    assert task.annotations == {}

    # Apply task tuple, (apply, function, args, kwargs)
    task = Task.from_call(apply, inc, (1,), {'extra': .1})
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {'extra': .1}
    assert task.annotations == {}

    # Apply task tuple, (apply, function, args, kwargs)
    task = Task.from_call(apply, inc, (1,), {'extra': .1},
                          annotations={'a': 1})
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {'extra': .1}
    assert task.annotations == {'a': 1}

def test_task_from_tuple():
    # Simple task tuple
    task = Task.from_tuple((inc, 1))
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {}
    assert task.annotations == {}

    # Apply args only case
    task = Task.from_tuple((apply, inc, (1,)))
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {}
    assert task.annotations == {}

    # Apply args and kwargs case
    task = Task.from_tuple((apply, inc, (1,), {'extra': .1}))
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {'extra': .1}
    assert task.annotations == {}

    # Dask graphs
    dsk = Task.from_tuple({"x": (apply, inc, (1,), {'extra': .1})})
    assert dsk["x"].function is inc
    assert dsk["x"].args == (1,)
    assert dsk["x"].kwargs == {'extra': .1}
    assert dsk["x"].annotations == {}

    # Nesting
    N = namedtuple("N", ("a b"))

    dsk = Task.from_tuple({
        "v": N(1, (inc, 1)),
        "w": N(inc, 1),
        "x": (apply, inc, (1,), {'kwtask': (apply, inc, (1,), {'extra': .1})}),
        "y": [(inc, 5), (inc, (inc, 2))],
        "z": (inc, (inc, (inc, Task(inc, (1,)))))
    })

    # namedtuple reproduce in this case
    assert dsk["v"]._fields == ("a", "b")
    assert dsk["v"] == N(1, Task.from_call(inc, 1))
    # namedtuple not reproduced here
    assert not hasattr(dsk["w"], "_fields")
    assert dsk["w"] == Task.from_call(inc, 1)
    # nested applies
    kwtask = Task.from_call(inc, 1, extra=.1)
    assert dsk["x"] == Task.from_call(inc, 1, kwtask=kwtask)
    # lists
    task = Task.from_call(inc, 2)
    task = Task.from_call(inc, task)
    assert dsk["y"] == [Task.from_call(inc, 5), task]
    # nested inc tuples
    task = Task.from_call(inc, 1)
    task = Task.from_call(inc, task)
    task = Task.from_call(inc, task)
    task = Task.from_call(inc, task)
    assert dsk["z"] == task


def test_task_can_fuse():
    task_1 = Task.from_call(inc, 1)
    task_a = Task.from_call(inc, 1, annotations={"a": 1})
    task_b = Task.from_call(inc, 1, annotations={"b": 1})

    # no annotations with no annotations
    assert task_1.can_fuse(task_1)
    # no annotations with annotations
    assert task_1.can_fuse(task_a)
    # annotations with annotations
    assert task_a.can_fuse(task_1)
    # same annotation with same annotation
    assert task_a.can_fuse(task_a)
    # different annotations
    assert not task_a.can_fuse(task_b)