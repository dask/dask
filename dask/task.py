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

        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or EMPTY_DICT

        if annotations is not None:
            if not isinstance(annotations, dict):
                raise TypeError("annotations (%s) must be a dict"
                                % function)
            self.annotations = annotations
        else:
            self.annotations = EMPTY_DICT

    def __eq__(self, other):
        """ Equality """
        return (self.function == other.function and
                self.args == other.args and
                self.kwargs == other.kwargs and
                self.annotations == other.annotations)

    def __reduce__(self):
        """ Pickling """
        return (Task, (self.function, self.args,
                       self.kwargs, self.annotations))


    def _format_task_bits(self, fn):
        """ Format task components into a (arg0, kw0=kwv0, annotations=...) string """
        arg_str = ", ".join("%s" % fn(a) for a in self.args)
        kwargs_str = ", ".join("%s=%s"% (k, fn(v))
                                         for k, v
                                         in self.kwargs.items())
        annot_str = ("annotions=%s" % fn(self.annotations)
                     if self.annotations else "")

        bits = (bit for bit in (arg_str, kwargs_str, annot_str) if bit)
        return ", ".join(bits)

    def __str__(self):
        return "%s(%s)" % (self.function.__name__, self._format_task_bits(str))

    def __repr__(self):
        return "Task(%s, %s)" % (self.function.__name__, self._format_task_bits(repr))

    @classmethod
    def from_call(cls, function, *args, annotations=None, **kwargs):
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
            else:
                raise ValueError("Invalid apply call")
        else:
            return Task(function, args, kwargs, annotations)

    @classmethod
    def from_tuple(cls, task):
        if type(task) is not tuple:
            raise TypeError("task must be a tuple")

        # TODO(sjperkins)
        # Figure out how to make this import work globally
        from .utils import apply

        # (apply, function, args, kwargs)
        if task[0] is apply:
            if len(task) == 3:
                return Task(task[1], task[2])
            elif len(task) == 4:
                return Task(task[1], task[2], task[3])
            else:
                raise ValueError("Invalid apply task")

        # (function, arg1, arg2, ..., argn) form
        return Task(task[0], task[1:])