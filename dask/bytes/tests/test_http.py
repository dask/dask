import os
import pytest
import requests
import subprocess
import time

from dask.bytes.core import open_files
from dask.compatibility import PY2


@pytest.fixture
def server():
    if PY2:
        cmd = ['python', '-m', 'SimpleHTTPServer', '8999']
    else:
        cmd = ['python', '-m', 'http.server', '8999']
    p = subprocess.Popen(cmd)
    timeout = 10
    while True:
        try:
            requests.get('http://localhost:8999')
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
            timeout -= 0.1
            if timeout < 0:
                raise RuntimeError('Server did not appear')
    yield
    p.terminate()


def test_simple(server):
    root = 'http://localhost:8999/'
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    fn = files[0]
    f = open_files(root + fn)[0]
    with f as f:
        data = f.read()
    assert data == open(fn, 'rb').read()
