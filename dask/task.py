class EmptyDict(dict):
    def __init__(self):
        pass

    def __setitem__(self, key, value):
        raise NotImplementedError("Modifying an EmptyDict")

    def __reduce__(self):
        return (EmptyDict,)

EMPTY_DICT = EmptyDict()

class Task:
    __slots__ = ("function", "args", "kwargs", "annotations")

    def __init__(self, function, args=None, kwargs=None, annotations=None):
        if not callable(function):
            raise TypeError("function (%s) must be callable" % function)

        kwargs = kwargs or EMPTY_DICT
        annotations = annotations or EMPTY_DICT
        args = args or ()

        if not isinstance(args, tuple):
            raise TypeError("args (%s) must be a tuple" % args)

        if not isinstance(kwargs, dict):
            raise TypeError("kwargs (%s) must be a dict" % kwargs)

        if not isinstance(annotations, dict):
            raise TypeError("annotations (%s) must be a dict" % annotations)

        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.annotations = annotations

    def __eq__(self, other):
        """ Equality """
        return (type(other) == Task and
                self.function == other.function and
                self.args == other.args and
                self.kwargs == other.kwargs and
                self.annotations == other.annotations)

    def __reduce__(self):
        """ Pickling """
        return (Task, (self.function, self.args,
                       self.kwargs, self.annotations))


    def _format_components(self, fn):
        """ Format task components into a (arg0, kw0=kwv0, annotations=...) string """
        arg_str = ", ".join(fn(a) for a in self.args)
        kwargs_str = ", ".join("%s=%s"% (k, fn(v))
                                         for k, v
                                         in self.kwargs.items())
        annot_str = ("annotions=%s" % fn(self.annotations)
                     if self.annotations else "")

        bits = (bit for bit in (arg_str, kwargs_str, annot_str) if bit)
        return ", ".join(bits)

    def __str__(self):
        return "%s(%s)" % (self.function.__name__,
                           self._format_components(str))

    def __repr__(self):
        return "Task(%s, %s)" % (self.function.__name__,
                                 self._format_components(repr))

    @classmethod
    def from_call(cls, function, *args, annotations=None, **kwargs):
        """ Create Task from function call """
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

        return Task(function, args, kwargs, annotations)

    @classmethod
    def from_tuple(cls, obj):
        """ Recreate objects containing legacy task tuples """
        ft = Task.from_tuple

        # TODO(sjperkins)
        # Figure out how to make this import work globally
        from .utils import apply

        if isinstance(obj, tuple):
            if obj is ():
                return obj
            # (apply, function [, args [, kwargs]])
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
            # (function, arg1, arg2, ..., argn) form
            elif callable(obj[0]):
                return Task(obj[0], ft(obj[1:]))
            # namedtuple
            elif getattr(obj, "_fields", None) is not None:
                return obj._make((ft(a) for a in obj))
            # ordinary tuple
            else:
                return tuple(ft(a) for a in obj)
        if isinstance(obj, list):
            return [ft(t) for t in obj]
        elif isinstance(obj, dict):
            return {k: ft(v) for k, v in obj.items()}
        else:
            return obj

    def can_fuse(self, other):
        return (type(other) == Task and
                (self.annotations == EMPTY_DICT or
                other.annotations == EMPTY_DICT or
                self.annotations == other.annotations))