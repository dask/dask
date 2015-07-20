from __future__ import absolute_import, division, print_function

from subprocess import check_call

from graphviz import Digraph

from .core import istask, get_dependencies, ishashable


def task_label(task):
    """Label for a task on a dot graph.

    Examples
    --------
    >>> from operator import add
    >>> task_label((add, 1, 2))
    'add'
    >>> task_label((add, (add, 1, 2), 3))
    'add(...)'
    """
    func = task[0]
    if hasattr(func, 'funcs'):
        if len(func.funcs) > 1:
            return '{0}(...)'.format(funcname(func.funcs[0]))
        else:
            head = funcname(func.funcs[0])
    else:
        head = funcname(task[0])
    if any(has_sub_tasks(i) for i in task[1:]):
        return '{0}(...)'.format(head)
    else:
        return head


def has_sub_tasks(task):
    """Returns True if the task has sub tasks"""
    if istask(task):
        return True
    elif isinstance(task, list):
        return any(has_sub_tasks(i) for i in task)
    else:
        return False


def funcname(func):
    """Get the name of a function."""
    while hasattr(func, 'func'):
        func = func.func
    return func.__name__


def name(x):
    try:
        return str(hash(x))
    except TypeError:
        return str(hash(str(x)))


def to_graphviz(dsk, data_attributes=None, function_attributes=None):
    if data_attributes is None:
        data_attributes = {}
    if function_attributes is None:
        function_attributes = {}

    g = Digraph(graph_attr={'rankdir': 'BT'})

    seen = set()

    for k, v in dsk.items():
        k_name = name(k)
        if k_name not in seen:
            seen.add(k_name)
            g.node(k_name, label=str(k), shape='box',
                   **data_attributes.get(k, {}))

        if istask(v):
            func_name = name((k, 'function'))
            if func_name not in seen:
                seen.add(func_name)
                g.node(func_name, label=task_label(v), shape='circle',
                       **function_attributes.get(k, {}))
            g.edge(func_name, k_name)

            for dep in get_dependencies(dsk, k):
                dep_name = name(dep)
                if dep_name not in seen:
                    seen.add(dep_name)
                    g.node(dep_name, label=str(dep), shape='box',
                           **data_attributes.get(dep, {}))
                g.edge(dep_name, func_name)
        elif ishashable(v) and v in dsk:
            g.edge(name(v), k_name)
    return g


def dot_graph(dsk, filename='mydask', **kwargs):
    g = to_graphviz(dsk, **kwargs)
    g.save(filename + '.dot')

    check_call('dot -Tpdf {0}.dot -o {0}.pdf'.format(filename), shell=True)
    check_call('dot -Tpng {0}.dot -o {0}.png'.format(filename), shell=True)
    try:
        from IPython.display import Image
        return Image(filename + '.png')
    except ImportError:
        pass
