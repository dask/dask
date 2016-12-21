from __future__ import print_function, division, absolute_import

from toolz import partition

def parse_args(args):
    options = dict(partition(2, args))
    for k, v in options.items():
        if v.isdigit():
            options[k] = int(v)

    return options


def transpose(lod):
    keys = list(lod[0].keys())
    return {k: [d[k] for d in lod] for k in keys}
