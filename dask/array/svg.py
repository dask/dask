import numpy as np
import math

def svg(chunks, **kwargs):
    if len(chunks) == 1:
        return svg_1d(chunks, **kwargs)
    elif len(chunks) == 2:
        return svg_2d(chunks, **kwargs)
    elif len(chunks) == 3:
        return svg_3d(chunks, **kwargs)
    else:
        return ''

def svg_2d(chunks, offset=(0, 0), skew=(0, 0)):
    shape = tuple(map(sum, chunks))
    sizes = draw_sizes(shape)
    y, x = grid_points(chunks, sizes)

    lines, (min_x, max_x, min_y, max_y) = svg_grid(x, y, shape, offset=offset, skew=skew)
    header = '<svg width="%d" height="%d" style="stroke:rgb(0,0,0);stroke-width:1" >\n' % (
        max_x + 50, max_y + 50)
    footer = '\n</svg>'

    if shape[0] >= 100:
        rotate = -90
    else:
        rotate = 0

    text = [
        '',
        '  <!-- Text -->',
        '  <text x="%f" y="%f" font-size="1.4rem" text-anchor="middle">%d</text>' % (max_x / 2, max_y + 20, shape[1]),
        '  <text x="%f" y="%f" font-size="1.4rem" text-anchor="middle" transform="rotate(%d,%f,%f)">%d</text>' % (
            max_x + 20, max_y / 2, rotate, max_x + 20, max_y / 2, shape[0]),
    ]


    return header + '\n'.join(lines + text) + footer


def svg_3d(chunks):
    shape = tuple(map(sum, chunks))
    sizes = draw_sizes(shape)
    x, y, z = grid_points(chunks, sizes)

    xy, (mnx, mxx, mny, mxy) = svg_grid(x / 1.7, y, (shape[0], shape[1]), offset=(10, 0), skew=(1, 0))
    zx, (_, _, _, max_x) = svg_grid(z, x / 1.7, (shape[2], shape[0]), offset=(10, 0), skew=(0, 1))
    zy, (min_z, max_z, min_y, max_y) = svg_grid(z, y, (shape[2], shape[1]), offset=(max_x + 10, max_x), skew=(0, 0))
    header = '<svg width="%d" height="%d" style="stroke:rgb(0,0,0);stroke-width:1" >\n' % (
        max_z + 50, max_y + 50)
    footer = '\n</svg>'

    if shape[1] >= 100:
        rotate = -90
    else:
        rotate = 0

    text = [
        '',
        '  <!-- Text -->',
        '  <text x="%f" y="%f" font-size="1.4rem" text-anchor="middle">%d</text>' % ((min_z + max_z) / 2, max_y + 20, shape[2]),
        '  <text x="%f" y="%f" font-size="1.4rem" text-anchor="middle" transform="rotate(%d,%f,%f)">%d</text>' % (
            max_z + 20, (min_y + max_y) / 2, rotate, max_z + 20, (min_y + max_y) / 2, shape[1]),
        '  <text x="%f" y="%f" font-size="1.4rem" text-anchor="middle" transform="rotate(45,%f,%f)">%d</text>' % (
            (mnx + mxx) / 2 - 10, mxy - (mxx - mnx) / 2 + 20, (mnx + mxx) / 2 - 10, mxy - (mxx - mnx) / 2 + 20, shape[0]),
    ]

    return header + '\n'.join(xy + zx + zy + text) + footer


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
    # Horizontal lines
    x1 = np.zeros_like(y) + offset[0]
    y1 = y + offset[1]
    x2 = np.full_like(y, x[-1]) + offset[0]
    y2 = y + offset[1]

    if skew[0]:
        y1_old = y1.copy()
        y2 += x.max() * skew[0]
    if skew[1]:
        x1 += skew[1] * y
        x2 += skew[1] * y

    min_x = min(x1.min(), x2.min())
    min_y = min(y1.min(), y2.min())
    max_x = max(x1.max(), x2.max())
    max_y = max(y1.max(), y2.max())

    h_lines = ["", "  <!-- Horizontal lines -->"] + svg_lines(x1, y1, x2, y2)

    # Vertical lines
    x1 = x + offset[0]
    y1 = np.zeros_like(x) + offset[1]
    x2 = x + offset[0]
    y2 = np.full_like(x, y[-1]) + offset[1]

    if skew[0]:
        y1 += skew[0] * x
        y2 += skew[0] * x
    if skew[1]:
        x2 += skew[1] * y.max()

    v_lines = ["", "  <!-- Vertical lines -->"] + svg_lines(x1, y1, x2, y2)

    rect = [
            '  <polygon points="%f,%f %f,%f %f,%f %f,%f" style="fill:#ECB172A0;stroke-width:0"/>' % (
            x1[0], y1[0], x1[-1], y1[-1], x2[-1], y2[-1], x2[0], y2[0])
    ]

    return h_lines + v_lines + rect , (min_x, max_x, min_y, max_y)


def svg_1d(chunks, **kwargs):
    return svg_2d(((1,),) + chunks, **kwargs)


def grid_points(chunks, sizes):
    cumchunks = [np.cumsum((0,) + c) for c in chunks]
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
    size = 200
    ratios = [mx / d for d in shape]
    ratios = [ratio_response(r) for r in ratios]
    return tuple(size / r for r in ratios)


def ratio_response(x):
    """ How we display actual size ratios

    Common ratios in sizes span several orders of magnitude,
    which is hard for us to perceive.

    We keep ratios in the 1-3 range accurate, and then apply a logarithm to
    values up until about 100 or so, at which point we stop scaling.
    """
    if x < math.e:
        return x
    elif x <= 100:
        return math.log(x + 12.4)  # f(e) == e
    else:
        return math.log(100 + 12.4)


class HTML:
    def __init__(self, text):
        self.text = text

    def _repr_html_(self):
        return self.text
