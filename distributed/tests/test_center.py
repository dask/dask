from __future__ import print_function, division, absolute_import

from time import sleep

from toolz import valmap
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.ioloop import IOLoop

from distributed.core import read, write, rpc
from distributed.center import Center
from distributed.utils_test import loop, gen_test


alice = 'alice:8888'
bob = 'bob:8888'
charlie = 'charlie:8888'


@gen_test()
def test_metadata():
    c = Center('127.0.0.1')
    c.listen(8006)

    cc = rpc(ip='127.0.0.1', port=8006)
    response = yield cc.register(address=alice, ncores=4)
    assert alice in c.has_what
    assert c.ncores[alice] == 4

    response = yield cc.add_keys(address=alice, keys=['x', 'y'])
    assert response == 'OK'

    response = yield cc.register(address=bob, ncores=4)
    response = yield cc.add_keys(address=bob, keys=['y', 'z'])
    assert response == 'OK'

    response = yield cc.who_has(keys=['x', 'y'])
    assert valmap(set, response) == {
                        'x': {alice},
                        'y': {alice, bob}}

    response = yield cc.remove_keys(address=bob, keys=['y'])
    assert response == 'OK'

    response = yield cc.has_what(keys=[alice, bob])
    assert valmap(set, response) == {alice: {'x', 'y'}, bob: {'z'}}

    response = yield cc.ncores()
    assert response == {alice: 4, bob: 4}
    response = yield cc.ncores(addresses=[alice, charlie])
    assert response == {alice: 4, charlie: None}

    response = yield cc.unregister(address=alice, close=True)
    assert response == 'OK'
    assert alice not in c.has_what
    assert alice not in c.ncores

    yield c.terminate()

"""
def test_delete_data():
    from distributed import Worker
    c = Center('127.0.0.1', 8037, loop=loop)
    a = Worker('127.0.0.1', 8038, c.ip, c.port, loop=loop)
    @asyncio.coroutine
    def f():
        while len(c.ncores) < 1:
            yield from asyncio.sleep(0.01, loop=loop)
        yield from rpc(a.ip, a.port).update_data(data={'x': 1, 'y': 2})
        assert a.data == {'x': 1, 'y': 2}
        yield from rpc(c.ip, c.port).add_keys(address=(a.ip, a.port),
                                              keys=['x', 'y'])
        yield from rpc(c.ip, c.port).delete_data(keys=['x'])

        assert a.data == {'y': 2}
        assert not c.who_has['x']
        assert list(c.has_what[(a.ip, a.port)]) == ['y']

        yield from a._close()
        yield from c._close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), f()))
"""
