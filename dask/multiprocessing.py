from __future__ import absolute_import, division, print_function

import multiprocessing
import psutil
from .async import get_async # TODO: get better get


def get(dsk, keys):
    """ Multiprocessed get function appropriate for Bags """
    pool = multiprocessing.Pool(psutil.cpu_count())
    manager = multiprocessing.Manager()
    queue = manager.Queue()
    result = get_async(pool.apply_async, psutil.cpu_count(), dsk, keys,
                       queue=queue)
    return result
