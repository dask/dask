try:
    from dataclasses import is_dataclass, fields as dataclass_fields

except ImportError:

    def is_dataclass(x):
        return False

    def dataclass_fields(x):
        return []


def apply(func, args, kwargs=None):
    if kwargs:
        return func(*args, **kwargs)
    else:
        return func(*args)
