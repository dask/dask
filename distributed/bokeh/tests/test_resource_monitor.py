from __future__ import print_function, division, absolute_import

import pytest

bokeh = pytest.importorskip('bokeh')
from bokeh.plotting import Figure

from tornado import gen
from distributed import Executor
from distributed.bokeh.resource_monitor import Occupancy
from distributed.utils_test import cluster, loop
from time import time


def test_occupancy(loop):
    with cluster(nanny=True) as (s, [a, b]):
        rm = Occupancy(('127.0.0.1', s['port']), interval=0.01)
        for k in ['host', 'processing', 'waiting']:
            assert k in rm.cds.data

        start = time()
        while not rm.cds.data['host']:
            loop.run_sync(lambda: gen.sleep(0.05))
            assert time() < start + 2

        assert (len(rm.cds.data['host']) ==
                len(rm.cds.data['processing']) ==
                len(rm.cds.data['waiting']) == 2)

        assert isinstance(rm.figure, Figure)
        rm.stream.close()
