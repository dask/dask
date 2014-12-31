import networkx as nx
from dask.core import istask

def to_networkx(d):
    g = nx.DiGraph()

    for k, v in d.items():
        g.add_node(k, shape='box')
        if istask(v):
            func, args = v[0], v[1:]
            func_node = (v, 'function')
            g.add_node(func_node, shape='circle', label=func.__name__)
            g.add_edge(k, func_node)
            for arg in args:
                g.add_node(arg, shape='box')
                g.add_edge(func_node, arg)
        else:
            g.add_node(k, label='%s=%s' % (k, v))

    return g


def dot_graph(d, filename='mydask'):
    import os
    g = to_networkx(d)
    p = nx.to_pydot(g)

    with open(filename + '.dot', 'w') as f:
        f.write(p.to_string())

    os.system('dot -Tpdf %s.dot -o %s.pdf' % (filename, filename))
    os.system('dot -Tpng %s.dot -o %s.png' % (filename, filename))
    print("Writing graph to %s.pdf" % filename)


if __name__ == '__main__':
    def add(x, y):
        return x + y
    def inc(x):
        return x + 1

    dsk = {'x': 1, 'y': (inc, 'x'),
           'a': 2, 'b': (inc, 'a'),
           'z': (add, 'y', 'b')}

    dot_graph(dsk)
