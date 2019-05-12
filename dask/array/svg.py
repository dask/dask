import numpy as np
import math

def svg(chunks, **kwargs):
    if len(chunks) == 1:
        return svg_1d(chunks, **kwargs)
    elif len(chunks) == 2:
        return svg_2d(chunks, **kwargs)
    else:
        # TODO
        return ''

def svg_2d(chunks, offset=(0, 0), skew=(0, 0)):
    y, x = grid_points(chunks)
    shape = tuple(map(sum, chunks))

    lines, (max_x, max_y) = svg_grid(x, y, shape, offset=offset, skew=skew)

    header = '<svg width="%d" height="%d" style="stroke:rgb(0,0,0);stroke-width:1" >\n' % (max_x + 50, max_y + 50)

    footer = '\n</svg>'

    return header + '\n'.join(lines) + footer


def svg_lines(x1, y1, x2, y2):
    n = len(x1)
    lines = ['  <line x1="%d" y1="%d" x2="%d" y2="%d" />' % (x1[i], y1[i], x2[i], y2[i])
               for i in range(n)]

    lines[0] = lines[0].replace(' /', ' style="stroke-width:2" /')
    lines[-1] = lines[-1].replace(' /', ' style="stroke-width:2" /')
    return lines


def svg_grid(x, y, shape, offset=(0, 0), skew=(0, 0)):
    """ Create lines of SVG text that show a grid

    Parameters
    ----------
    x: numpy.ndarray
    y: numpy.ndarray
    shape: tuple
    offset: tuple
        translational displacement of the grid in SVG coordinates
    skew: tuple
    """
    x1 = np.zeros_like(y) + skew[1] * y + offset[0]
    y1 = y + skew[0] * x + offset[1]
    x2 = np.full_like(y, x[-1]) + skew[1] * y + offset[0]
    y2 = y + skew[0] * x + offset[1]

    max_x = max(x1.max(), x2.max())
    max_y = max(y1.max(), y2.max())

    h_lines = ["<!-- Horizontal lines -->"] + svg_lines(x1, y1, x2, y2)

    x1 = x + offset[0]
    y1 = np.zeros_like(x) + offset[1]
    x2 = x + offset[0]
    y2 = np.full_like(x, y[-1]) + offset[1]

    y1 += x[-1] * skew[0]
    x2 += y[-1] * skew[1]

    max_x = max(max_x, x1.max(), x2.max())
    max_y = max(max_y, y1.max(), y2.max())

    v_lines = ["<!-- Vertical lines -->"] + svg_lines(x1, y1, x2, y2)

    rect = [
            '  <polygon points="%f,%f %f,%f %f,%f %f,%f" style="fill:#ECB172A0;stroke-width:0"/>' % (
            x1[0], y1[0], x1[0], y1[-1], x2[-1], y2[-1], x2[0], y1[0])
    ]

    text = [
        '',
        '<!-- Text -->',
        '<text x="%f" y="%f" font-size="1.4rem" text-anchor="middle">%d</text>' % (x[-1] / 2, y[-1] + 20, shape[1]),
        '<text x="%f" y="%f" font-size="1.4rem" text-anchor="middle" transform="rotate(-90 %f,%f)">%d</text>' % (
            x[-1] + 20, y[-1] / 2, x[-1] + 20, y[-1] / 2, shape[0]),
    ]

    return h_lines + v_lines + rect + text, (max_x, max_y)



def svg_1d(chunks, **kwargs):
    return svg_2d(((1,),) + chunks, **kwargs)


def grid_points(chunks):
    cumchunks = [np.cumsum((0,) + c) for c in chunks]
    sizes = draw_sizes(tuple(map(sum, chunks)))
    points = [x * size / x[-1] for x, size in zip(cumchunks, sizes)]
    return points


def draw_sizes(shape, max_size=400):
    """

    Examples
    --------
    >>> draw_sizes((10, 10), max_size=100)
    (100, 100)
    >>> draw_sizes((1000,), max_size=100)
    (100,)
    >>> draw_sizes((1000, 1), max_size=100)
    (100,, 10)
    >>> draw_sizes((1000000, 1000, 1), max_size=100)
    (100, 2, 1)
    """
    mx = max(shape)
    size = math.log(mx) * 40
    ratios = [d / mx for d in shape]
    ratios = [max(1/10, r) for r in ratios]
    return tuple(size * r for r in ratios)


class HTML:
    def __init__(self, text):
        self.text = text

    def _repr_html_(self):
        return self.text
