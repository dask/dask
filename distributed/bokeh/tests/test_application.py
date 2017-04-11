import requests
from time import sleep
import sys

import pytest
pytest.importorskip('bokeh')

from distributed.bokeh.application import BokehWebInterface
from distributed import LocalCluster
from distributed.http import HTTPScheduler
from distributed.metrics import time
from distributed.utils_test import cluster, loop
from distributed.utils import ignoring

def test_BokehWebInterface(loop):
    with LocalCluster(2, loop=loop, scheduler_port=0,
                      services={('http', 0): HTTPScheduler},
                      diagnostics_port=None) as c:
        with pytest.raises(Exception):
            response = requests.get('http://127.0.0.1:8787/status/')
        with BokehWebInterface(
                scheduler_address=c.scheduler.address,
                http_port=c.scheduler.services['http'].port,
                bokeh_port=8787) as w:
            start = time()
            while True:
                with ignoring(Exception):
                    response = requests.get('http://127.0.0.1:8787/status/')
                    if response.ok:
                        break
                assert time() < start + 5
                sleep(0.01)
    with pytest.raises(Exception):
        response = requests.get('http://127.0.0.1:8787/status/')
