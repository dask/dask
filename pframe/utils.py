from contextlib import contextmanager

def raises(exc, lamda):
    try:
        lamda()
        return False
    except exc:
        return True


@contextmanager
def ignoring(*exc):
    try:
        yield
    except exc:
        pass
