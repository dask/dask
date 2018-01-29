import os
import shutil
import sys
import tempfile

from distributed import Client
from distributed.utils_test import cluster
from distributed.utils_test import loop  # noqa F401


PRELOAD_TEXT = """
_worker_info = {}

def dask_setup(worker):
    _worker_info['address'] = worker.address

def get_worker_address():
    return _worker_info['address']
"""


def test_worker_preload_file(loop):

    def check_worker():
        import worker_info
        return worker_info.get_worker_address()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, 'worker_info.py')
        with open(path, 'w') as f:
            f.write(PRELOAD_TEXT)
        with cluster(worker_kwargs={'preload': [path]}) as (s, workers), \
                Client(s['address'], loop=loop) as c:

            assert c.run(check_worker) == {
                worker['address']: worker['address']
                for worker in workers
            }
    finally:
        shutil.rmtree(tmpdir)


def test_worker_preload_module(loop):

    def check_worker():
        import worker_info
        return worker_info.get_worker_address()

    tmpdir = tempfile.mkdtemp()
    sys.path.insert(0, tmpdir)
    try:
        path = os.path.join(tmpdir, 'worker_info.py')
        with open(path, 'w') as f:
            f.write(PRELOAD_TEXT)

        with cluster(worker_kwargs={'preload': ['worker_info']}) \
                as (s, workers), Client(s['address'], loop=loop) as c:

            assert c.run(check_worker) == {
                worker['address']: worker['address']
                for worker in workers
            }
    finally:
        sys.path.remove(tmpdir)
        shutil.rmtree(tmpdir)
