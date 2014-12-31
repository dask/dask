import networkx as nx
from .core import istask

def to_networkx(d):
    g = nx.DiGraph()

    for k, v in d.items():
        g.add_node(k, shape='box')
        if istask(v):
            func, args = v[0], v[1:]
            g.add_node(func, shape='circle', label=func.__name__)
            g.add_edge(k, func)
            for arg in args:
                g.add_node(arg, shape='box')
                g.add_edge(func, arg)
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
