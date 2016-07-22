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
from uuid import uuid4

from tornado.gen import TimeoutError

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
    kc = BlockingKernelClient(**connection_info)
    kc.session.key = connection_info['key']
    kc.start_channels()
    def remote(line, cell=None):
        """Run the current cell on a remote IPython kernel"""
        if cell is None:
            # both line and cell magic
            cell = line
        run_cell_remote(ip, kc, cell)
    ip.register_magic_function(remote, magic_kind='line', magic_name=magic_name)
    ip.register_magic_function(remote, magic_kind='cell', magic_name=magic_name)


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
    def _cleanup_connection_file():
        """Cleanup our connection file when we exit."""
        try:
            os.remove(path)
        except OSError:
            pass
    atexit.register(_cleanup_connection_file)

