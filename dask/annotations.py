from contextlib import contextmanager
import threading


class SafeStack:
    """ Maintains thread safe stack of current annotations """

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._stack = []

    def push(self, annotation):
        with self._lock:
            self._stack.append(annotation)

    def pop(self):
        with self._lock:
            self._stack.pop()

    def copy(self):
        with self._lock:
            return self._stack.copy()


annotation_stack = SafeStack()


def current_annotations():
    return annotation_stack.copy()


@contextmanager
def annotate(annotation):
    annotation_stack.push(annotation)

    try:
        yield
    finally:
        annotation_stack.pop()
