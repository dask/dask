from __future__ import print_function, division, absolute_import

import mock

import pytest
from toolz import first

from distributed import Client
from distributed.utils_test import zmq_ctx, mock_ipython, cluster, loop


@pytest.mark.ipython
def test_start_ipython_workers(loop, zmq_ctx):
    from jupyter_client import BlockingKernelClient

    with cluster(1) as (s, [a]):
        with Client(s['address'], loop=loop) as e:
            info_dict = e.start_ipython_workers()
            info = first(info_dict.values())
            key = info.pop('key')
            kc = BlockingKernelClient(**info)
            kc.session.key = key
            kc.start_channels()
            kc.wait_for_ready(timeout=10)
            msg_id = kc.execute("worker")
            reply = kc.get_shell_msg(timeout=10)
            assert reply['parent_header']['msg_id'] == msg_id
            assert reply['content']['status'] == 'ok'
            kc.stop_channels()


@pytest.mark.ipython
def test_start_ipython_scheduler(loop, zmq_ctx):
    from jupyter_client import BlockingKernelClient

    with cluster(1) as (s, [a]):
        with Client(s['address'], loop=loop) as e:
            info = e.start_ipython_scheduler()
            key = info.pop('key')
            kc = BlockingKernelClient(**info)
            kc.session.key = key
            kc.start_channels()
            msg_id = kc.execute("scheduler")
            reply = kc.get_shell_msg(timeout=10)
            kc.stop_channels()


@pytest.mark.ipython
def test_start_ipython_scheduler_magic(loop, zmq_ctx):
    with cluster(1) as (s, [a]):
        with Client(s['address'], loop=loop) as e, mock_ipython() as ip:
            info = e.start_ipython_scheduler()

        expected = [
            {'magic_kind': 'line', 'magic_name': 'scheduler'},
            {'magic_kind': 'cell', 'magic_name': 'scheduler'},
        ]

        call_kwargs_list = [ kwargs for (args, kwargs) in ip.register_magic_function.call_args_list ]
        assert call_kwargs_list == expected
        magic = ip.register_magic_function.call_args_list[0][0][0]
        magic(line="", cell="scheduler")


@pytest.mark.ipython
def test_start_ipython_workers_magic(loop, zmq_ctx):
    with cluster(2) as (s, [a, b]):

        with Client(s['address'], loop=loop) as e, mock_ipython() as ip:
            workers = list(e.ncores())[:2]
            names = [ 'magic%i' % i for i in range(len(workers)) ]
            info_dict = e.start_ipython_workers(workers, magic_names=names)

        expected = [
            {'magic_kind': 'line', 'magic_name': 'remote'},
            {'magic_kind': 'cell', 'magic_name': 'remote'},
            {'magic_kind': 'line', 'magic_name': 'magic0'},
            {'magic_kind': 'cell', 'magic_name': 'magic0'},
            {'magic_kind': 'line', 'magic_name': 'magic1'},
            {'magic_kind': 'cell', 'magic_name': 'magic1'},
        ]
        call_kwargs_list = [ kwargs for (args, kwargs) in ip.register_magic_function.call_args_list ]
        assert call_kwargs_list == expected
        assert ip.register_magic_function.call_count == 6
        magics = [ args[0][0] for args in ip.register_magic_function.call_args_list[2:] ]
        magics[-1](line="", cell="worker")
        [ m.client.stop_channels() for m in magics ]


@pytest.mark.ipython
def test_start_ipython_workers_magic_asterix(loop, zmq_ctx):
    with cluster(2) as (s, [a, b]):

        with Client(s['address'], loop=loop) as e, mock_ipython() as ip:
            workers = list(e.ncores())[:2]
            info_dict = e.start_ipython_workers(workers, magic_names='magic_*')

        expected = [
            {'magic_kind': 'line', 'magic_name': 'remote'},
            {'magic_kind': 'cell', 'magic_name': 'remote'},
            {'magic_kind': 'line', 'magic_name': 'magic_0'},
            {'magic_kind': 'cell', 'magic_name': 'magic_0'},
            {'magic_kind': 'line', 'magic_name': 'magic_1'},
            {'magic_kind': 'cell', 'magic_name': 'magic_1'},
        ]
        call_kwargs_list = [ kwargs for (args, kwargs) in ip.register_magic_function.call_args_list ]
        assert call_kwargs_list == expected
        assert ip.register_magic_function.call_count == 6
        magics = [ args[0][0] for args in ip.register_magic_function.call_args_list[2:] ]
        magics[-1](line="", cell="worker")
        [ m.client.stop_channels() for m in magics ]


@pytest.mark.ipython
def test_start_ipython_remote(loop, zmq_ctx):
    from distributed._ipython_utils import remote_magic
    with cluster(1) as (s, [a]):
        with Client(s['address'], loop=loop) as e, mock_ipython() as ip:
            worker = first(e.ncores())
            ip.user_ns['info'] = e.start_ipython_workers(worker)[worker]
            remote_magic('info 1') # line magic
            remote_magic('info', 'worker') # cell magic

        expected = [
            ((remote_magic,), {'magic_kind': 'line', 'magic_name': 'remote'}),
            ((remote_magic,), {'magic_kind': 'cell', 'magic_name': 'remote'}),
        ]
        assert ip.register_magic_function.call_args_list == expected
        assert ip.register_magic_function.call_count == 2


@pytest.mark.ipython
def test_start_ipython_qtconsole(loop):
    Popen = mock.Mock()
    with cluster() as (s, [a, b]):
        with mock.patch('distributed._ipython_utils.Popen', Popen), Client(s['address'], loop=loop) as e:
            worker = first(e.ncores())
            e.start_ipython_workers(worker, qtconsole=True)
            e.start_ipython_workers(worker, qtconsole=True, qtconsole_args=['--debug'])
    assert Popen.call_count == 2
    (cmd,), kwargs = Popen.call_args_list[0]
    assert cmd[:3] == [ 'jupyter', 'qtconsole', '--existing' ]
    (cmd,), kwargs = Popen.call_args_list[1]
    assert cmd[-1:] == [ '--debug' ]

