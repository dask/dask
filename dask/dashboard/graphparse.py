from __future__ import division, absolute_import

from collections import namedtuple
import subprocess
import shlex

import toolz

from .to_pydot import to_pydot


fields = ('scale', 'width', 'height', 'nodes', 'edges')
class Graph(namedtuple('Graph', fields)):
    pass


fields = ('name', 'x', 'y', 'width', 'height', 'label', 'style', 'shape',
          'color', 'fillcolor')
class Node(namedtuple('Node', fields)):
    @classmethod
    def _from_parse(cls, data):
        if len(data) != 10:
            raise ValueError("Bad Node Line")
        name, x, y, width, height, label, style, shape, color, fillcolor = data
        x, y = float(x), float(y)
        width, height = float(width), float(height)
        return cls(name, x, y, width, height, label, style, shape,
                   color, fillcolor)

fields = ('tail', 'head', 'cpoints', 'label', 'style', 'color')
class Edge(namedtuple('Edge', fields)):
    @classmethod
    def _from_parse(cls, data):
        tail, head, n = data[:3]
        n = int(n)
        cps = data[3:3 + 2*n]
        cpoints = list(zip(*_form_cpoints(cps)))
        remainder = data[3 + 2*n:]
        if len(remainder) == 2:
            style, color = remainder
            labeldata = None
        elif len(remainder) == 5:
            label, xl, yl, style, color = remainder
            labeldata = (label, float(xl), float(yl))
        else:
            raise ValueError("Bad Edge Line")

        return cls(tail, head, cpoints, labeldata, style, color)


def _form_cpoints(cpoints):
    x0 = cpoints[0]
    y0 = cpoints[1]
    for cx0, cy0, cx1, cy1, x1, y1 in toolz.partition(6, cpoints[2:]):
        yield x0, y0, cx0, cy0, cx1, cy1, x1, y1
        x0 = x1
        y0 = y1


def _merge_lines(lines):
    temp = []
    for line in lines:
        if line.endswith('\\'):
            temp.append(line.strip('\\'))
        elif temp:
            temp.append(line)
            yield ''.join(temp)
            temp = []
        else:
            yield line


def parse_plain_file(content):
    lines = list(_merge_lines(content.splitlines()))
    header = shlex.split(lines.pop(0))

    scale, width, height = (float(i) for i in header[1:])
    nodes = []
    edges = []

    for line in lines:
        line = shlex.split(line)
        key, data = line[0], line[1:]
        if key == 'node':
            nodes.append(Node._from_parse(data))
        elif line[0] == 'edge':
            edges.append(Edge._from_parse(data))
        elif line[0] == 'stop':
            break
    return Graph(scale, width, height, nodes, edges)


def dask_to_graph(dsk):
    p = to_pydot(dsk)

    out, ret = subprocess.Popen(['dot', '-Tplain'], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE).communicate(p.to_string())
    if ret:
        raise ValueError("dot command failure.\n\n" + out)

    return parse_plain_file(out)
