"""
Annotation testing script.

DO NOT MERGE THIS FILE IN IT'S CURRENT STATE

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import bz2
from collections import namedtuple
from functools import partial
import timeit


try:
    from cloudpickle import dumps, HIGHEST_PROTOCOL
except ImportError:
    try:
        from cPickle import dumps, HIGHEST_PROTOCOL
    except ImportError:
        from pickle import dumps, HIGHEST_PROTOCOL

import dask  # noqa
from dask.core import istask, ishashable, has_tasks
import mock

TaskAnnotation = namedtuple("TaskAnnotation", ["an"])


# class Task(tuple):
#     __slots__ = ()

#     _index_names = ("function", "args", "kwargs", "annotations")

#     def __new__(cls, fn, *args, **kwargs):
#         annots = kwargs.pop("annotations", None)
#         return tuple.__new__(Task, (fn, args, kwargs, annots))

#     @property
#     def function(self):
#         return self[0]

#     @property
#     def args(self):
#         return self[1]

#     @property
#     def kwargs(self):
#         return self[2]

#     @property
#     def annotations(self):
#         return self[3]

#     def __repr__(self):
#         details = ", ".join("%s=%s" % (n, repr(self[i]))
#                             for i, n in enumerate(self._index_names)
#                             if self[i])
#         return 'Task({})'.format(details)


class Task(object):
    __slots__ = ("function", "args", "kwargs", "annotations")

    def __init__(self, function, *args, **kwargs):
        self.function = function
        self.args = args
        self.annotations = kwargs.pop("annotations", None)
        self.kwargs = kwargs

    def __getstate__(self):
        return (self.function, self.args, self.kwargs, self.annotations)

    def __setstate__(self, state):
        self.function = state[0]
        self.args = state[1]
        self.kwargs = state[2]
        self.annotations = state[3]

    def __repr__(self):
        details = ", ".join("%s=%s" % (n, repr(self[i]))
                            for i, n in enumerate(self.__slots__)
                            if self[i])
        return 'Task({})'.format(details)


def execute_task_std(arg, cache, dsk=None):
    if isinstance(arg, list):
        return [execute_task_std(a, cache) for a in arg]
    elif istask(arg):
        func, args = arg[0], arg[1:]
        args2 = [execute_task_std(a, cache) for a in args]
        return func(*args2)
    elif not ishashable(arg):
        return arg
    elif arg in cache:
        return cache[arg]
    else:
        return arg


def execute_task_annots(arg, cache, dsk=None):
    if isinstance(arg, list):
        return [execute_task_annots(a, cache) for a in arg]
    elif istask(arg):
        # Strip out annotations for the moment
        # https://github.com/dask/dask/issues/3783
        if type(arg[-1]) == TaskAnnotation:
            func, args, annots = arg[0], arg[1:-1], arg[-1]
        else:
            func, args, annots = arg[0], arg[1:], None  # noqa

        args2 = [execute_task_annots(a, cache) for a in args]
        return func(*args2)
    elif not ishashable(arg):
        return arg
    elif arg in cache:
        return cache[arg]
    else:
        return arg


def execute_task_obj(arg, cache, dsk=None):
    if isinstance(arg, list):
        return [execute_task_obj(a, cache) for a in arg]
    elif type(arg) == Task:
        # function, args + kwargs properties
        args2 = [execute_task_obj(a, cache) for a in arg.args]
        kwargs2 = {k: execute_task_obj(v, cache)
                   for k, v in arg.kwargs.items()}
        return arg.function(*args2, **kwargs2)
    elif istask(arg):
        func, args = arg[0], arg[1:]
        args2 = [execute_task_obj(a, cache) for a in args]
        return func(*args2)
    elif not ishashable(arg):
        return arg
    elif arg in cache:
        return cache[arg]
    else:
        return arg


def has_tasks_obj(dsk, x):
    if type(x) == Task or istask(x):
        return True
    try:
        if x in dsk:
            return True
    except Exception:
        pass
    if isinstance(x, list):
        for i in x:
            if has_tasks(dsk, i):
                return True
    return False


def f(x, *args, **kwargs):
    return x + 1


units = {"nanoseconds": 1e-9,
         "microseconds": 1e-6,
         "milliseconds": 1e-3,
         "seconds": 1.0}


def format_time(t):
    precision = 3
    scales = [(scale, unit) for unit, scale in units.items()]
    scales.sort(reverse=True)

    for scale, unit in scales:
        if t >= scale:
            break

    return "%.*g %s" % (precision, t / scale, unit)


def create_tasks(N, style="task"):
    if style.lower() == "task":
        return {('task', i): Task(f, i, bob="foo",
                                  annotations={"resource": {"GPU": 1},
                                               "size": i})
                for i in range(N)}
    elif style.lower() == "annotated_tuple":
        return {('task', i): (f, i, {"bob": "foo"},
                              TaskAnnotation({"resource": {"GPU": 1},
                                              "size": i}))
                for i in range(N)}
    elif style.lower() == "tuple":
        return {('task', i): (f, i, {"bob": "foo"},
                              {"resource": {"GPU": 1}, "size": i})
                for i in range(N)}
    else:
        raise ValueError("Invalid style %s" % style)


def create_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-l", "--loops", default=1000, type=int)
    p.add_argument("-t", "--tasks", default=1000, type=int)
    return p


if __name__ == "__main__":
    args = create_parser().parse_args()
    ntasks = args.tasks
    nloops = args.loops
    styles = ("tuple", "annotated_tuple", "task")

    print("\nTASK EXECUTION TIMING")

    exec_tasks_fns = (execute_task_std, execute_task_annots, execute_task_obj)
    has_tasks_fn = (has_tasks, has_tasks, has_tasks_obj)

    for style, exec_fn, htasks_fn in zip(styles, exec_tasks_fns, has_tasks_fn):
        setup = ("from __main__ import create_tasks, TaskAnnotation\n"
                 "import dask\n"
                 "tasks = create_tasks({ntasks}, '{style}') "
                 .format(exec_fn=exec_fn, has_tasks_fn=htasks_fn,
                         ntasks=ntasks, style=style))

        exec_task_patch = mock.patch("dask.local._execute_task",
                                     side_effect=exec_fn)
        has_tasks_patch = mock.patch("dask.local.has_tasks",
                                     side_effect=htasks_fn)

        with exec_task_patch as etp, has_tasks_patch as htp:
            t = timeit.timeit("dask.get(tasks, tasks.keys())",
                              setup=setup, number=nloops)

        assert etp.called
        assert htp.called

    print("\nTASK CREATION TIMING")

    for style in styles:
        setup = "from __main__ import create_tasks"
        t = timeit.timeit("create_tasks(%d, '%s')" % (ntasks, style),
                          setup=setup, number=nloops)

        print(style, format_time(t / (nloops * ntasks)))

    dumps = partial(dumps, protocol=HIGHEST_PROTOCOL)

    print("\nPICKLING TASK TIMING")

    for style in styles:
        setup = ("from __main__ import create_tasks, TaskAnnotation; "
                 "from cPickle import dumps; "
                 "tasks = create_tasks(%d, '%s') " % (ntasks, style))

        t = timeit.timeit("dumps(tasks)", setup=setup, number=nloops)

        print(style, format_time(t / (nloops * ntasks)))

    print("\nPICKLING SIZES")

    for style in styles:
        print(style, len(dumps(create_tasks(ntasks, style))), "bytes")

    print("\nBZIP2 PICKLED SIZES")

    for style in styles:
        nbytes = len(bz2.compress(dumps(create_tasks(ntasks, style))))
        print(style, nbytes, "bytes")
