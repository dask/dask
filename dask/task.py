import functools
import types

EMPTY_DICT = dict()


def annotate(fn, annotation=None):
    """ Annotate a function object """
    if not annotation:
        return fn

    # Attribute already exists on fn, update it and return fn
    try:
        fn._dask_annotations.update(annotation)
    except AttributeError:
        pass
    else:
        return fn

    # Create a copy of the function, add the annotation
    # Based on http://stackoverflow.com/a/6528148/190597
    g = types.FunctionType(
        fn.__code__,
        fn.__globals__,
        name=fn.__name__,
        argdefs=fn.__defaults__,
        closure=fn.__closure__,
    )
    g = functools.update_wrapper(g, fn)
    g.__kwdefaults__ = fn.__kwdefaults__
    g._dask_annotations = annotation

    return g


class TupleTask:
    """ Means :code:`type(x) is tuple and x and callable(x[0])` """

    pass


def spec_type(x):
    t = type(x)
    return TupleTask if (t is tuple and x and callable(x[0])) else t


class Task:
    """
    Task object

    .. code-block:: python

        def fn(a, b, extra=0.1)
            return a + b + extra

        task = Task(fn, [1, 2], {'extra': 0.1}, {'priority': 100})
        assert task.function(*task.args, **task.kwargs) == 3.1

    Parameters
    ----------
    function : callable
        Function executed by the task.
    args : list or object
        Arguments supplied to the function upon execution of the task.
    kwargs : dict or object
        Keyword arguments supplied to the function upon execution of the task.
    annotations : dict
        Metadata associated with the task. Used for specifying additional
        information about the task to the dask schedulers,
        or scheduler plugins.
    """

    __slots__ = ("function", "args", "kwargs", "annotations")

    def __init__(self, function, args=None, kwargs=None, annotations=None):
        if not callable(function):
            raise TypeError("function (%s) must be callable" % function)

        kwargs = kwargs or EMPTY_DICT
        annotations = annotations or EMPTY_DICT
        args = args or ()

        if not isinstance(annotations, dict):
            raise TypeError("annotations (%s) must be a dict" % annotations)

        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.annotations = annotations

    @classmethod
    def from_call(cls, function, *args, annotations=None, **kwargs):
        """ Create Task from (function, args, kwargs, annotations) """
        # TODO(sjperkins)
        # Figure out how to make this import work globally
        from .utils import apply

        if function is apply:
            # Convert apply calls
            if len(kwargs) != 0:
                raise ValueError("apply kwargs should in args[2]")

            if len(args) == 1:
                # (apply, function)
                return Task(args[0])
            elif len(args) == 2:
                # (apply, function, args)
                return Task(args[0], args[1], annotations=annotations)
            elif len(args) == 3:
                # (apply, function, args, kwargs)
                return Task(args[0], args[1], args[2], annotations=annotations)
            raise ValueError("Invalid apply call")
        elif callable(function):
            return Task(function, list(args), kwargs, annotations)
        else:
            raise TypeError("function is not callable")

    @classmethod
    def from_spec(cls, dsk):
        """
        Transform objects from the the `dask graph specification
        <https://docs.dask.org/en/latest/spec.html>_`_
        by converting task tuples into Task objects

        """
        # TODO(sjperkins)
        # Figure out how to make these imports work globally
        from .core import spec_type
        from .utils import apply
        from .highlevelgraph import HighLevelGraph
        from .optimization import SubgraphCallable

        fs = Task.from_spec
        typ = spec_type(dsk)

        if typ is TupleTask:
            # task of the form (apply, function [, args [, kwargs]])
            if dsk[0] is apply:
                annot = getattr(dsk[1], "_dask_annotations", None)

                if len(dsk) == 2:
                    # (apply, function)
                    return Task(dsk[1], annotations=annot)
                elif len(dsk) == 3:
                    # (apply, function, args)
                    return Task(dsk[1], fs(dsk[2]), annotations=annot)
                elif len(dsk) == 4:
                    # (apply, function, args, kwargs)
                    return Task(dsk[1], fs(dsk[2]), fs(dsk[3]), annotations=annot)

                raise ValueError("Invalid apply dsk %s" % dsk)
            # task of the form (function, arg1, arg2, ..., argn)
            else:
                if type(dsk[0]) is SubgraphCallable:
                    fn = SubgraphCallable(
                        fs(dsk[0].dsk), dsk[0].outkey, dsk[0].inkeys, dsk[0].name
                    )
                else:
                    fn = dsk[0]

                return Task(
                    fn,
                    list(fs(a) for a in dsk[1:]),
                    annotations=getattr(dsk[0], "_dask_annotations", None),
                )
        elif typ is list:
            return [fs(t) for t in dsk]
        elif typ is dict:
            return {k: fs(v) for k, v in dsk.items()}
        elif typ is HighLevelGraph:
            layers = {
                name: {k: fs(v) for k, v in layer.items()}
                for name, layer in dsk.layers.items()
            }

            return HighLevelGraph(layers, dsk.dependencies)
        else:
            # Task or Key or literal
            return dsk

    @classmethod
    def to_spec(cls, dsk):
        # TODO(sjperkins)
        # Figure out how to make these imports work globally
        from .utils import apply
        from .highlevelgraph import HighLevelGraph
        from .optimization import SubgraphCallable

        typ = type(dsk)
        ts = Task.to_spec

        if typ is Task:
            # Get an annotated funtion
            fn = annotate(dsk.function, dsk.annotations)
            kw = ts(dsk.kwargs)

            # Convert SubgraphCallable graph's
            if type(fn) is SubgraphCallable:
                fn.dsk = ts(fn.dsk)

            # No keywords, output (function, args1, ..., argns)
            if len(kw) == 0:
                return (fn,) + tuple(ts(dsk.args))
            # Keywords, output (apply, function, args, kwargs)
            else:
                # Convert any dicts to (dict, [[k, v]]) task
                if type(kw) is dict:
                    kw = (dict, [[k, v] for k, v in kw.items()])
                return (apply, fn, ts(dsk.args), kw)
        elif typ is list:
            return [ts(e) for e in dsk]
        elif typ is dict:
            return {k: ts(v) for k, v in dsk.items()}
        elif typ is HighLevelGraph:
            layers = {
                name: {k: ts(v) for k, v in layer.items()}
                for name, layer in dsk.layers.items()
            }

            return HighLevelGraph(layers, dsk.dependencies)
        else:
            return dsk

    def annotate(self, annotations):
        if type(annotations) is not dict:
            raise TypeError("annotations is not a dict")
        elif self.annotations is EMPTY_DICT:
            self.annotations = annotations
        else:
            self.annotations.update(annotations)

    def from_tuple(self, task):
        task_type = spec_type(task)

        if task_type is Task:
            return task
        elif task_type is TupleTask:
            return Task.from_spec(task)
        else:
            raise TypeError("task is not a (fn, *args) task tuple")

    def to_tuple(self):
        return Task.to_spec(self)

    def dependencies(self):
        return (self.args if isinstance(self.args, list) else [self.args]) + (
            list(self.kwargs.values())
            if isinstance(self.kwargs, dict)
            else [self.kwargs]
        )

    @classmethod
    def _is_complex(cls, task):
        task_type = type(task)

        if task_type is list:
            return any(cls._is_complex(t) for t in task)
        elif task_type is dict:
            return any(cls._is_complex(t) for t in task.values())
        elif task_type is Task:
            return True
        else:
            return False

    def is_complex(self):
        """ Does this task contain nested tasks? """
        return self._is_complex(self.args) or self._is_complex(self.kwargs)

    def can_fuse(self, other):
        """
        Returns
        -------
            True if

                1. self.annotations == {} or
                2. other.annotations == {} or
                3. self.annotations == other.annotations

            False otherwise.
        """
        return type(other) is Task and (
            self.annotations is EMPTY_DICT
            or other.annotations is EMPTY_DICT
            or self.annotations == other.annotations
        )

    def __eq__(self, other):
        """ Equality """
        return (
            type(other) is Task
            and self.function == other.function
            and self.args == other.args
            and self.kwargs == other.kwargs
            and self.annotations == other.annotations
        )

    def __reduce__(self):
        """ Pickling """
        return (Task, (self.function, self.args, self.kwargs, self.annotations))

    def _format_components(self, fn):
        """ Format task components into a (arg0, kw0=kwv0, annotations=...) string """
        if isinstance(self.args, (tuple, list)):
            arg_str = ", ".join(fn(a) for a in self.args)
        else:
            arg_str = "*%s" % fn(self.args)

        if isinstance(self.kwargs, dict):
            kwargs_str = ", ".join("%s=%s" % (k, fn(v)) for k, v in self.kwargs.items())
        else:
            kwargs_str = "**%s" % fn(self.kwargs)

        annot_str = "annotations=%s" % fn(self.annotations) if self.annotations else ""

        bits = (bit for bit in (arg_str, kwargs_str, annot_str) if bit)
        return ", ".join(bits)

    def __str__(self):
        from .utils import funcname

        return "%s(%s)" % (funcname(self.function), self._format_components(str))

    def __repr__(self):
        from .utils import funcname

        return "Task(%s, %s)" % (funcname(self.function), self._format_components(repr))
