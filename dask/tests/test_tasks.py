from collections import namedtuple
import pickle

import pytest

from dask.core import get

from dask.task import EmptyDict, Task, annotate
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
    task = Task.from_call(inc, 1, extra=0.1, annotations={"resource": "GPU"})
    assert pickle.loads(pickle.dumps(task)) == task


def test_task_repr_and_str():
    task_1 = Task.from_call(inc, 1, extra=0.1, annotations={"resource": "GPU"})
    task_2 = Task.from_call(inc, task_1)
    assert str(task_2) == "inc(inc(1, extra=0.1, annotions={'resource': 'GPU'}))"
    assert (
        repr(task_2)
        == "Task(inc, Task(inc, 1, extra=0.1, annotions={'resource': 'GPU'}))"
    )


def test_task_get():
    task_1 = Task.from_call(inc, 1, extra=0.1, annotations={"resource": "GPU"})
    task_2 = Task.from_call(inc, task_1)
    dsk = {"x": task_2}
    assert get(dsk, "x") == 3.1


def test_task_from_call():
    # Simple task tuple
    task = Task.from_call(inc, 1)
    assert task.function is inc
    assert task.args == [1]
    assert task.kwargs == {}
    assert task.annotations == {}

    task = Task.from_call(inc, 1, extra=0.1, annotations={"a": 1})
    assert task.function is inc
    assert task.args == [1]
    assert task.kwargs == {"extra": 0.1}
    assert task.annotations == {"a": 1}

    # Apply task, (apply, function)
    task = Task.from_call(apply, inc)

    # Apply task tuple, (apply, function, args)
    task = Task.from_call(apply, inc, (1,))
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {}
    assert task.annotations == {}

    # Apply task tuple, (apply, function, args, kwargs)
    task = Task.from_call(apply, inc, (1,), {"extra": 0.1})
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {"extra": 0.1}
    assert task.annotations == {}

    # Apply task tuple, (apply, function, args, kwargs)
    task = Task.from_call(apply, inc, (1,), {"extra": 0.1}, annotations={"a": 1})
    assert task.function is inc
    assert task.args == (1,)
    assert task.kwargs == {"extra": 0.1}
    assert task.annotations == {"a": 1}


def test_task_from_spec():
    # Simple task tuple
    task = Task.from_spec((inc, 1))
    assert task.function is inc
    assert task.args == [1]
    assert task.kwargs == {}
    assert task.annotations == {}

    # Simple task tuple with annotation
    annot_inc = annotate(inc, {"b": 1})
    task = Task.from_spec((annot_inc, 1))
    assert task.function is annot_inc
    assert task.args == [1]
    assert task.kwargs == {}
    assert task.annotations == {"b": 1}

    # Apply args only case
    task = Task.from_spec((apply, inc, [1]))
    assert task.function is inc
    assert task.args == [1]
    assert task.kwargs == {}
    assert task.annotations == {}

    # Apply args and kwargs case
    task = Task.from_spec((apply, inc, [1], {"extra": 0.1}))
    assert task.function is inc
    assert task.args == [1]
    assert task.kwargs == {"extra": 0.1}
    assert task.annotations == {}

    # Dask graphs
    dsk = Task.from_spec({"x": (apply, inc, [1], {"extra": 0.1})})
    assert dsk["x"].function is inc
    assert dsk["x"].args == [1]
    assert dsk["x"].kwargs == {"extra": 0.1}
    assert dsk["x"].annotations == {}

    # Nesting
    N = namedtuple("N", ("a b"))

    dsk = Task.from_spec(
        {
            "a": 0.1,
            "w": N(inc, 1),
            "x": (
                apply,
                inc,
                [1],
                (dict, [["extra", (apply, inc, [1], (dict, [["extra", "a"]]))]]),
            ),
            "y": [(inc, 5), (inc, (inc, 2))],
            "z": (inc, (inc, (inc, Task(inc, [1])))),
        }
    )

    # namedtuples passed through
    assert dsk["w"] == N(inc, 1)
    # nested applies
    extra_dict = Task(dict, [[["extra", "a"]]])
    kw_inc = Task(inc, [1], extra_dict)
    extra_dict = Task(dict, [[["extra", kw_inc]]])
    assert dsk["x"] == Task(inc, [1], extra_dict)
    assert get(dsk, "x") == inc(1, extra=inc(1, extra=0.1))
    # lists
    task = Task.from_call(inc, 2)
    task = Task.from_call(inc, task)
    assert dsk["y"] == [Task.from_call(inc, 5), task]
    assert get(dsk, "y") == [inc(5), inc(inc(2))]
    # nested inc tuples
    task = Task.from_call(inc, 1)
    task = Task.from_call(inc, task)
    task = Task.from_call(inc, task)
    task = Task.from_call(inc, task)
    assert dsk["z"] == task
    assert get(dsk, "z") == inc(inc(inc(inc(1))))

    from dask.highlevelgraph import HighLevelGraph

    layer_a = {("a", i): (inc, i) for i in range(4)}
    layer_b = {("b", i): (inc, ("a", i)) for i in range(4)}
    layers = {"a": layer_a, "b": layer_b}
    deps = {"a": set(), "b": set(["a"])}
    dsk = HighLevelGraph(layers, deps)

    hlg = Task.from_spec(dsk)
    assert type(hlg) is HighLevelGraph
    assert hlg.dependencies == dsk.dependencies
    assert hlg.layers["a"] == {("a", i): Task.from_call(inc, i) for i in range(4)}
    assert hlg.layers["b"] == {
        ("b", i): Task.from_call(inc, ("a", i)) for i in range(4)
    }


def test_task_to_spec():
    from dask.core import get

    binc = annotate(inc, {"b": 1})
    cinc = annotate(inc, {"c": 1})
    dsk = {"a": 0.1, "out": (binc, (apply, cinc, [1], {"extra": "a"}))}
    dsk2 = Task.from_spec(dsk)
    dsk3 = Task.to_spec(dsk2)

    assert dsk2["out"].annotations == {"b": 1}
    assert dsk2["out"].args[0].annotations == {"c": 1}
    # (annot_inc, (apply, annot_inc, [1], (dict, [['extra', 'a']])))
    assert dsk3["out"][0]._dask_annotations == {"b": 1}
    assert dsk3["out"][1][1]._dask_annotations == {"c": 1}
    assert get(dsk3, "out") == get(dsk2, "out")

    from dask.highlevelgraph import HighLevelGraph

    layer_a = {("a", i): Task.from_call(inc, i) for i in range(4)}
    layer_b = {("b", i): Task.from_call(inc, ("a", i)) for i in range(4)}
    layers = {"a": layer_a, "b": layer_b}
    deps = {"a": set(), "b": set(["a"])}
    dsk = HighLevelGraph(layers, deps)

    hlg = Task.to_spec(dsk)
    assert type(hlg) is HighLevelGraph
    assert hlg.dependencies == dsk.dependencies
    assert hlg.layers["a"] == {("a", i): (inc, i) for i in range(4)}
    assert hlg.layers["b"] == {("b", i): (inc, ("a", i)) for i in range(4)}


def test_task_to_tuple():
    tuple_task = (apply, inc, [1], {"extra": 0.1})
    task = Task.from_spec(tuple_task)
    assert task == Task(inc, [1], {"extra": 0.1})
    assert task.to_tuple() == (apply, inc, [1], (dict, [["extra", 0.1]]))


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


def test_task_complex():
    from dask.core import get_dependencies, get

    dsk = {
        ("a", i): (tuple, [(apply, slice, ("b", i)), (slice, 0, "c")],)
        for i in range(4)
    }

    dsk.update(
        {
            "c": 10,
            ("b", 0): (1, 2),
            ("b", 1): (2, 3),
            ("b", 2): (3, 4),
            ("b", 3): (4, 5),
        }
    )

    # Convert to tuples
    dsk2 = Task.from_spec(dsk)

    keys = [("a", i) for i in range(4)]

    assert get(dsk, keys) == get(dsk2, keys)

    for i in range(4):
        deps = get_dependencies(dsk, ("a", i))
        assert deps == {("b", i), "c"}
        assert get_dependencies(dsk2, ("a", i)) == deps


def test_annotate_function():
    from dask.task import annotate

    ainc = annotate(inc, {"a": 1})
    assert ainc is not inc
    assert ainc._dask_annotations == {"a": 1}
    aainc = annotate(ainc, {"b": 1})
    assert aainc._dask_annotations == {"a": 1, "b": 1}
    assert aainc is ainc


def test_annotate_task():
    task = Task.from_call(inc, 1)
    assert len(task.annotations) == 0

    task.annotate({"a": 1})
    assert task.annotations == {"a": 1}
    task.annotate({"b": 1})
    assert task.annotations == {"a": 1, "b": 1}
