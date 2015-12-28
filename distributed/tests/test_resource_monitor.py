import pytest

bokeh = pytest.importorskip('bokeh')
from bokeh.plotting import Figure

from tornado import gen
from distributed.resource_monitor import ResourceMonitor
from distributed.utils_test import (cluster, scheduler, slow, _test_cluster, loop, inc,
        div, dec)
from time import time


def test_resource_monitor(loop):
    with cluster(nanny=True) as (c, [a, b]):
        with scheduler(c['port']) as sport:
            rm = ResourceMonitor(('127.0.0.1', sport), notebook=False,
                    interval=0.01)
            for k in ['cpu', 'memory', 'host']:
                assert k in rm.cds.data

            start = time()
            while not rm.cds.data['cpu']:
                loop.run_sync(lambda: gen.sleep(0.05))
                assert time() < start + 2

            assert (len(rm.cds.data['cpu']) ==
                    len(rm.cds.data['host']) ==
                    len(rm.cds.data['memory']) == 2)

            assert isinstance(rm.figure, Figure)
            rm.stream.close()
