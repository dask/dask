class EmptyDict(dict):
    def __init__(self):
        pass

    def __setitem__(self, key, value):
        raise NotImplementedError("Modifying an EmptyDict")

    def __reduce__(self):
        return (EmptyDict, ())


EMPTY_DICT = EmptyDict()


class Task:
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
    def from_spec(cls, obj):
        """
        Transform objects from the the `dask graph specification
        <https://docs.dask.org/en/latest/spec.html>_`_
        by converting task tuples into Task objects

        """
        ft = Task.from_spec

        # TODO(sjperkins)
        # Figure out how to make these imports work globally
        from .utils import apply
        from .highlevelgraph import HighLevelGraph

        if isinstance(obj, tuple):
            # Tuples are either tasks, or keys
            if obj == ():
                return obj
            # task of the form (apply, function [, args [, kwargs]])
            elif obj[0] is apply:
                if len(obj) == 2:
                    # (apply, function)
                    return Task(obj[1])
                elif len(obj) == 3:
                    # (apply, function, args)
                    return Task(obj[1], ft(obj[2]))
                elif len(obj) == 4:
                    # (apply, function, args, kwargs)
                    return Task(obj[1], ft(obj[2]), ft(obj[3]))

                raise ValueError("Invalid apply obj %s" % obj)

            # task of the form (function, arg1, arg2, ..., argn)
            elif callable(obj[0]):
                return Task(obj[0], list(ft(a) for a in obj[1:]))
            # namedtuple
            elif getattr(obj, "_fields", None) is not None:
                return obj._make((ft(a) for a in obj))
            # key is implied by a standard tuple
            else:
                return obj
        elif isinstance(obj, list):
            return [ft(t) for t in obj]
        elif isinstance(obj, dict):
            return {k: ft(v) for k, v in obj.items()}
        elif isinstance(obj, HighLevelGraph):
            # TODO(sjperkins)
            # Properly handle HLG complexity
            return {k: ft(v) for k, v in obj.items()}
        else:
            # Key or literal
            return obj

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
