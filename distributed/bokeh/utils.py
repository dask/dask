from __future__ import print_function, division, absolute_import

from distutils.version import LooseVersion

import bokeh
from toolz import partition

from ..compatibility import PY2

BOKEH_VERSION = LooseVersion(bokeh.__version__)


if BOKEH_VERSION >= "1.0.0" and not PY2:
    # This decorator is only available in bokeh >= 1.0.0, and doesn't work for
    # callbacks in Python 2, since the signature introspection won't line up.
    from bokeh.core.properties import without_property_validation
else:

    def without_property_validation(f):
        return f


def parse_args(args):
    options = dict(partition(2, args))
    for k, v in options.items():
        if v.isdigit():
            options[k] = int(v)

    return options


def transpose(lod):
    keys = list(lod[0].keys())
    return {k: [d[k] for d in lod] for k in keys}
