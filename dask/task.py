import functools
import types


class EmptyDict(dict):
    def __init__(self):
        pass

    def __setitem__(self, key, value):
        raise NotImplementedError("Modifying an EmptyDict")

    def __reduce__(self):
        return (EmptyDict, ())


EMPTY_DICT = EmptyDict()


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
        from .utils import apply
        from .highlevelgraph import HighLevelGraph

        fs = Task.from_spec
        typ = type(dsk)

        if typ is tuple:
            # Tuples are either tasks, or keys
            if len(dsk) == 0:
                return dsk
            # task of the form (apply, function [, args [, kwargs]])
            elif dsk[0] is apply:
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
            elif callable(dsk[0]):
                return Task(
                    dsk[0],
                    list(fs(a) for a in dsk[1:]),
                    annotations=getattr(dsk[0], "_dask_annotations", None),
                )
            # key is implied by a standard tuple
            else:
                return dsk
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
            # Key or literal
            return dsk

    @classmethod
    def to_spec(cls, dsk):
        # TODO(sjperkins)
        # Figure out how to make these imports work globally
        from .utils import apply
        from .highlevelgraph import HighLevelGraph

        typ = type(dsk)
        ts = Task.to_spec

        if typ is Task:
            # Get an annotated funtion
            fn = annotate(dsk.function, dsk.annotations)
            kw = ts(dsk.kwargs)

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
        if type(task) is tuple and task and callable(task[0]):
            return Task.from_spec(task)

        raise TypeError("task is not a (fn, *args) task tuple")

    def to_tuple(self):
        return Task.to_spec(self)

    def dependencies(self):
        return (self.args if isinstance(self.args, list) else [self.args]) + (
            list(self.kwargs.values())
            if isinstance(self.kwargs, dict)
            else [self.kwargs]
        )

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

        annot_str = "annotions=%s" % fn(self.annotations) if self.annotations else ""

        bits = (bit for bit in (arg_str, kwargs_str, annot_str) if bit)
        return ", ".join(bits)

    def __str__(self):
        from .utils import funcname

        return "%s(%s)" % (funcname(self.function), self._format_components(str))

    def __repr__(self):
        from .utils import funcname

        return "Task(%s, %s)" % (funcname(self.function), self._format_components(repr))
