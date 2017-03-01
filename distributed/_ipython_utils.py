"""Utilities for integrating with IPython

These functions should probably reside in Jupyter and IPython repositories,
after which we can import them instead of having our own definitions.
"""

from __future__ import print_function

import atexit
import os
try:
    import queue
except ImportError:
    # Python 2
    import Queue as queue
from subprocess import Popen
import sys
from threading import Thread
from uuid import uuid4

from tornado.gen import TimeoutError
from tornado.ioloop import IOLoop
from threading import Event

from IPython import get_ipython
from jupyter_client import BlockingKernelClient, write_connection_file
from jupyter_core.paths import jupyter_runtime_dir


OUTPUT_TIMEOUT = 10


def run_cell_remote(ip, kc, cell):
    """Run a cell on a KernelClient

    Any output from the cell will be redisplayed in the local session.
    """
    msg_id = kc.execute(cell)

    in_kernel = getattr(ip, 'kernel', False)
    if in_kernel:
        socket = ip.display_pub.pub_socket
        session = ip.display_pub.session
        parent_header = ip.display_pub.parent_header

    while True:
        try:
            msg = kc.get_iopub_msg(timeout=OUTPUT_TIMEOUT)
        except queue.Empty:
            raise TimeoutError("Timeout waiting for IPython output")

        if msg['parent_header'].get('msg_id') != msg_id:
            continue
        msg_type = msg['header']['msg_type']
        content = msg['content']
        if msg_type == 'status':
            if content['execution_state'] == 'idle':
                # idle means output is done
                break
        elif msg_type == 'stream':
            stream = getattr(sys, content['name'])
            stream.write(content['text'])
        elif msg_type in ('display_data', 'execute_result', 'error'):
            if in_kernel:
                session.send(socket, msg_type, content, parent=parent_header)
            else:
                if msg_type == 'error':
                    print('\n'.join(content['traceback']), file=sys.stderr)
                else:
                    sys.stdout.write(content['data'].get('text/plain', ''))
        else:
            pass


def register_worker_magic(connection_info, magic_name='worker'):
    """Register a %worker magic, given connection_info.

    Both a line and cell magic are registered,
    which run the given cell in a remote kernel.
    """
    ip = get_ipython()
    info = dict(connection_info) # copy
    key = info.pop('key')
    kc = BlockingKernelClient(**connection_info)
    kc.session.key = key
    kc.start_channels()

    def remote(line, cell=None):
        """Run the current cell on a remote IPython kernel"""
        if cell is None:
            # both line and cell magic
            cell = line
        run_cell_remote(ip, kc, cell)

    remote.client = kc # preserve reference on kc, largely for mocking
    ip.register_magic_function(remote, magic_kind='line', magic_name=magic_name)
    ip.register_magic_function(remote, magic_kind='cell', magic_name=magic_name)


def remote_magic(line, cell=None):
    """A magic for running code on a specified remote worker

    The connection_info dict of the worker will be looked up
    as the first positional arg to the magic.
    The rest of the line (or the entire cell for a %%cell magic)
    will be passed to the remote kernel.

    Usage:

        info = e.start_ipython(worker)[worker]
        %remote info print(worker.data)
    """
    # get connection info from IPython's user namespace
    ip = get_ipython()
    split_line = line.split(None, 1)
    info_name = split_line[0]
    if info_name not in ip.user_ns:
        raise NameError(info_name)
    connection_info = dict(ip.user_ns[info_name])

    if not cell: # line magic, use the rest of the line
        if len(split_line) == 1:
            raise ValueError("I need some code to run!")
        cell = split_line[1]

    # turn info dict to hashable str for use as lookup key in _clients cache
    key = ','.join(map(str, sorted(connection_info.items())))
    session_key = connection_info.pop('key')

    if key in remote_magic._clients:
        kc = remote_magic._clients[key]
    else:
        kc = BlockingKernelClient(**connection_info)
        kc.session.key = session_key
        kc.start_channels()
        kc.wait_for_ready(timeout=10)
        remote_magic._clients[key] = kc

    # actually run the code
    run_cell_remote(ip, kc, cell)


# cache clients for re-use in remote magic
remote_magic._clients = {}


def register_remote_magic(magic_name='remote'):
    """Define the parameterized %remote magic

    See remote_magic above for details.
    """
    ip = get_ipython()
    if ip is None:
        return # do nothing if IPython's not running
    ip.register_magic_function(remote_magic, magic_kind='line', magic_name=magic_name)
    ip.register_magic_function(remote_magic, magic_kind='cell', magic_name=magic_name)


def connect_qtconsole(connection_info, name=None, extra_args=None):
    """Open a QtConsole connected to a worker who has the given future

    - identify worker with who_has
    - start IPython kernel on the worker
    - start qtconsole connected to the kernel
    """
    runtime_dir = jupyter_runtime_dir()
    if name is None:
        name = uuid4().hex

    path = os.path.join(runtime_dir, name + '.json')
    write_connection_file(path, **connection_info)
    cmd = ['jupyter', 'qtconsole', '--existing', path]
    if extra_args:
        cmd.extend(extra_args)
    Popen(cmd)

    @atexit.register
    def _cleanup_connection_file():
        """Cleanup our connection file when we exit."""
        try:
            os.remove(path)
        except OSError:
            pass


def start_ipython(ip=None, ns=None, log=None):
    """Start an IPython kernel in a thread

    Parameters
    ----------

    ip: str
        The IP address to listen on (likely the parent object's ip).
    ns: dict
        Any names that should be injected into the IPython namespace.
    log: logger instance
        Hook up IPython's logging to an existing logger instead of the default.
    """
    from IPython import get_ipython
    if get_ipython() is not None:
        raise RuntimeError("Cannot start IPython, it's already running.")

    from zmq.eventloop.ioloop import ZMQIOLoop
    from ipykernel.kernelapp import IPKernelApp
    # save the global IOLoop instance
    # since IPython relies on it, but we are going to put it in a thread.
    save_inst = IOLoop.instance()
    IOLoop.clear_instance()
    zmq_loop = ZMQIOLoop()
    zmq_loop.install()

    # start IPython, disabling its signal handlers that won't work due to running in a thread:
    app = IPKernelApp.instance(log=log)
    # Don't connect to the history database
    app.config.HistoryManager.hist_file = ':memory:'
    # listen on all interfaces, so remote clients can connect:
    if ip:
        app.ip = ip
    # disable some signal handling, logging
    noop = lambda : None
    app.init_signal = noop
    app.log_connection_info = noop

    # start IPython in a thread
    # initialization happens in the thread to avoid threading problems
    # with the sqlite history
    evt = Event()

    def _start():
        app.initialize([])
        app.kernel.pre_handler_hook = noop
        app.kernel.post_handler_hook = noop
        app.kernel.start()
        app.kernel.loop = IOLoop.instance()
        # save self in the IPython namespace as 'worker'
        # inject things into the IPython namespace
        if ns:
            app.kernel.shell.user_ns.update(ns)
        evt.set()
        zmq_loop.start()

    zmq_loop_thread = Thread(target=_start)
    zmq_loop_thread.daemon = True
    zmq_loop_thread.start()
    assert evt.wait(timeout=5), "IPython didn't start in a reasonable amount of time."

    # put the global IOLoop instance back:
    IOLoop.clear_instance()
    save_inst.install()
    return app
