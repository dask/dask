from __future__ import print_function, division, absolute_import

from distributed.sync import connect_sync, read_sync, write_sync
from distributed.utils_test import cluster, loop


def test_core_sync(loop):
    with cluster(nworkers=0) as (s, []):
        sock = connect_sync('127.0.0.1', s['port'])
        write_sync(sock, {'op': 'identity'})
        result = read_sync(sock)
        assert result['type'] == 'Scheduler'
