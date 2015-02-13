from __future__ import absolute_import, division, print_function

import networkx as nx
from dask.core import istask, get_dependencies
from toolz import first


def make_hashable(x):
    try:
        hash(x)
        return x
    except TypeError:
        return hash(str(x))


def lower(func):
    while hasattr(func, 'func'):
        func = func.func
    return func


def to_networkx(d, data_attributes=None, function_attributes=None):
    if data_attributes is None:
        data_attributes = dict()
    if function_attributes is None:
        function_attributes = dict()

    g = nx.DiGraph()

    for k, v in sorted(d.items(), key=first):
        g.add_node(k, shape='box', **data_attributes.get(k, dict()))
        if istask(v):
            func, args = v[0], v[1:]
            func_node = make_hashable((v, 'function'))
            g.add_node(func_node,
                       shape='circle',
                       label=lower(func).__name__,
                       **function_attributes.get(k, dict()))
            g.add_edge(k, func_node)
            for dep in sorted(get_dependencies(d, k)):
                arg2 = make_hashable(dep)
                g.add_node(arg2,
                           label=str(dep),
                           shape='box',
                           **data_attributes.get(dep, dict()))
                g.add_edge(func_node, arg2)
        else:
            g.add_node(k, label='%s=%s' % (k, v), **data_attributes.get(k, dict()))

    return g


def write_networkx_to_dot(dg, filename='mydask'):
    import os
    p = nx.to_pydot(dg)
    with open(filename + '.dot', 'w') as f:
        f.write(p.to_string())

    os.system('dot -Tpdf %s.dot -o %s.pdf' % (filename, filename))
    os.system('dot -Tpng %s.dot -o %s.png' % (filename, filename))
    print("Writing graph to %s.pdf" % filename)


def dot_graph(d, filename='mydask', **kwargs):
    dg = to_networkx(d, **kwargs)
    write_networkx_to_dot(dg, filename=filename)


if __name__ == '__main__':
    def add(x, y):
        return x + y
    def inc(x):
        return x + 1

    dsk = {'x': 1, 'y': (inc, 'x'),
           'a': 2, 'b': (inc, 'a'),
           'z': (add, 'y', 'b')}

    dot_graph(dsk)
