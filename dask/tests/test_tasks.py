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


def test_execute_task():
    task_1 = Task.from_call(inc, 1, extra=.1, annotations={'resource': 'GPU'})
    task_2 = Task.from_call(inc, task_1)
    dsk = {'x': task_2}
    assert get(dsk, 'x') == 3.1


def test_from_call():
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

def test_from_tuple():
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
