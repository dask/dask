import pytest

bokeh = pytest.importorskip('bokeh')
from bokeh.plotting import Figure

from tornado import gen
from distributed import Executor
from distributed.diagnostics.resource_monitor import ResourceMonitor, Occupancy
from distributed.utils_test import (cluster, scheduler, slow, _test_cluster, loop, inc,
        div, dec, cluster_center)
from time import time


def test_resource_monitor(loop):
    with cluster_center(nanny=True) as (c, [a, b]):
        with scheduler(c['port']) as sport:
            with Executor(('127.0.0.1', sport)) as e:
                rm1 = ResourceMonitor(interval=0.01)
                with Executor(('127.0.0.1', c['port'])) as e:
                    rm2 = ResourceMonitor(interval=0.01)
                    rm3 = ResourceMonitor(('127.0.0.1', sport), interval=0.01)
                    for rm in [rm1, rm2, rm3]:
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
