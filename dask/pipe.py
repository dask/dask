"""
Dynamically trace code decorated to build an execution graph.
"""

import uuid

EXECUTION_GRAPH = {}

class Variable(object):

    def __init__(self):
        self._uid = uuid.uuid4().hex

    def __getitem__(self, *args):
        # How does dask deal with multiple return arguments?
        pass

    def get():
        # Here call the dask computation retrieval mechanism
        pass


def add_var(obj):
    variable = Variable()
    EXECUTION_GRAPH[variable._uid] = obj
    return variable


class do(object):
    # Could also be called 'run'

    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):
        traceable_args = [arg if isinstance(arg, Variable)
                          else add_var(arg)
                          for arg in args]
        traceable_args = tuple([arg._uid for arg in traceable_args])

        # XXX: I don't know how dask deals so far with kwargs
        #traceable_kwargs = dict((name,
        #                          arg._uid if isinstance(arg, Variable)
        #                          else arg)
        #                        for name, arg in kwargs.items())
        result = Variable()
        EXECUTION_GRAPH[result._uid] = (self.function, ) + traceable_args

        return result


if __name__ == '__main__':
    # Simple demo code, doing a bit of pseudo computation, and displaying
    # the built execution graph
    import numpy as np

    x = do(np.add)(1, 2)
    y = do(np.multiply)(x, 4)
    z = do(np.sum)(x, y, do(np.power)(x, 4))

    import pprint
    pprint.pprint(EXECUTION_GRAPH)


