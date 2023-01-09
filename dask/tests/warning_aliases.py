try:
    from sqlalchemy.exc import RemovedIn20Warning
except ModuleNotFoundError:

    class _RemovedIn20Warning(Warning):
        pass

    RemovedIn20Warning = _RemovedIn20Warning
