from __future__ import absolute_import

from operator import itemgetter

from pydot import Graph, Node, Edge
from ..core import istask, get_dependencies, ishashable


def label(func):
    try:
        while hasattr(func, 'func'):
            func = func.func
        return func.__name__
    except AttributeError:
        return 'func'


def name(x):
    try:
        return str(hash(x))
    except TypeError:
        return str(hash(str(x)))


def to_pydot(d):
    g = Graph(graph_type='digraph')

    node_lk = {}

    for k, v in sorted(d.items(), key=itemgetter(0)):
        k_name = name(k)
        if k_name not in node_lk:
            node_lk[k_name] = node = Node(k_name, shape='box', label=str(k))
            g.add_node(node)

        if istask(v):
            func = v[0]
            func_name = name((k, 'function'))
            if func_name not in node_lk:
                node_lk[func_name] = node = Node(func_name, shape='circle',
                                                 label=label(func))
                g.add_node(node)
            g.add_edge(Edge(func_name, k_name, arrowhead='none'))

            for dep in sorted(get_dependencies(d, k)):
                dep_name = name(dep)
                if dep_name not in node_lk:
                    node_lk[dep_name] = node = Node(dep_name, label=str(dep),
                                                    shape='box')
                    g.add_node(node)
                g.add_edge(Edge(dep_name, func_name, arrowhead='none'))
        elif ishashable(v) and v in d:
            g.add_edge(Edge(name(v), k_name, arrowhead='none'))
        g.set_rankdir('BT')
    return g
