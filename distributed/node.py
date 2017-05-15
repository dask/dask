from __future__ import print_function, division, absolute_import

from tornado.ioloop import IOLoop

from .core import Server, ConnectionPool


class Node(object):
    """
    Base class for nodes in a distributed cluster.
    """

    def __init__(self, connection_limit=512, deserialize=True,
                 connection_args=None, io_loop=None):
        self.io_loop = io_loop or IOLoop.current()
        self.rpc = ConnectionPool(limit=connection_limit,
                                  deserialize=deserialize,
                                  connection_args=connection_args)


class ServerNode(Node, Server):
    """
    Base class for server nodes in a distributed cluster.
    """
    # TODO factor out security, listening, services, etc. here

    # XXX avoid inheriting from Server? there is some large potential for confusion
    # between base and derived attribute namespaces...

    def __init__(self, handlers, connection_limit=512, deserialize=True,
                 connection_args=None, io_loop=None):
        Node.__init__(self, deserialize=deserialize,
                      connection_limit=connection_limit,
                      connection_args=connection_args,
                      io_loop=io_loop)
        Server.__init__(self, handlers, connection_limit=connection_limit,
                        deserialize=deserialize, io_loop=self.io_loop)
