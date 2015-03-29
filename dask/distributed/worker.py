from zmqompute import ComputeNode
import zmq
import dask
from time import time


DEBUG = True

context = zmq.Context()

class Worker(object):
    """ A worker in a distributed dask computation pool

    A worker consists of the following:

    1.  data: A MutableMappiing to collect and store results.
        This may be distributed, like a ``pallet.Warehouse``
    2.  server: A local ``zmqompute.ComputeNode`` server to respond to queries
        to execute dask tasks

    The compute server hosts one function, ``compute``, which takes a key/task
    pair like the following:

        key: 'a',  task: (add, (inc, 'x'), 'y')

    We grab the necessary data (e.g. x, y) from the data store (if a Warehouse
    this may trigger communication), evaluate the task, and store the result
    back in the data store under the given key.  We then return a message
    holding metadata about the computation

        key: key-name
        duration: time in seconds
        status: 'OK' or the text from an Exception

    See Also
    --------

    pallet.Warehouse
    zmqompute.ComputeNode
    """
    def __init__(self, host, port, data):
        self.server = ComputeNode(host=host, port=port,
                                  functions={'compute': self.compute})
        self.data = data

    def get(self, task):
        if isinstance(task, list):
            return [self.get(item) for item in task]

        if ishashable(task) and task in self.data:
            return self.data[task]

        if not dask.istask(task):
            raise ValueError("Received a non-task " + str(task))

        func, args = task[0], task[1:]
        args2 = [self.get(arg) for arg in args]
        result = func(*args2)

        return result

    def compute(self, key, task):
        start = time()

        status = "OK"
        if DEBUG:
            print("Start Worker: %s\tTime: %s\nKey: %s\tTask: %s" %
                    (self.server.url, start, str(key), str(task)))
        try:
            result = self.get(task)
            end = time()
        except Exception as e:
            status = str(e)
            end = time()
        else:
            self.data[key] = result
        if DEBUG:
            print("End Worker: %s\tTime: %s\nKey: %s\tTask: %s" %
                    (self.server.url, end, str(key), str(task)))
            print("Status: %s" % status)

        message = {'key': key,
                   'duration': end - start,
                   'status': status,
                   'worker': self.server.url}
        return message

    def __del__(self):
        self.server.stop()


def ishashable(x):
    try:
        hash(x)
        return True
    except TypeError:
        return False
