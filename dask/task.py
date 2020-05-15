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
            raise TypeError("%s is not callable" % function)

        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or EMPTY_DICT

        if annotations is not None:
            if not isinstance(annotations, dict):
                raise TypeError("annotations must be a dict")
            self.annotations = annotations
        else:
            self.annotations = EMPTY_DICT

    def __equal__(self, other):
        return (self.function == other.function and
                self.args == other.args and
                self.kwargs == other.kwargs and
                self.annotations == other.annotations)

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