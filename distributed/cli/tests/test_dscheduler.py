from __future__ import print_function, division, absolute_import

from subprocess import Popen, PIPE
import requests

from distributed import Scheduler, Executor

def test_defaults():
    try:
        proc = Popen(['dscheduler'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)

        response = requests.get('http://127.0.0.1:9786/info.json')
        assert response.ok
        assert response.json()['status'] == 'running'
    finally:
        e.shutdown()
        proc.kill()
