
import json
import logging
import multiprocessing
import os
import socket
import sys

import bokeh
import distributed.bokeh
from ..utils import ignoring

dirname = os.path.dirname(distributed.__file__)
paths = [os.path.join(dirname, 'bokeh', name)
         for name in ['status', 'tasks']]

binname = 'bokeh.exe' if sys.platform.startswith('win') else 'bokeh'
binname = os.path.join(os.path.dirname(sys.argv[0]), binname)

logger = logging.getLogger(__file__)

dask_dir = os.path.join(os.path.expanduser('~'), '.dask')
if not os.path.exists(dask_dir):
    os.mkdir(dask_dir)


class BokehWebInterface(object):
    def __init__(self, host='127.0.0.1', http_port=9786, tcp_port=8786,
                 bokeh_port=8787, bokeh_whitelist=[], log_level='critical',
                 show=False, prefix=None, use_xheaders=False, quiet=True):
        self.port = bokeh_port
        ip = socket.gethostbyname(host)

        hosts = ['localhost',
                 '127.0.0.1',
                 ip,
                 host]

        with ignoring(Exception):
            hosts.append(socket.gethostbyname(ip))
        with ignoring(Exception):
            hosts.append(socket.gethostbyname(socket.gethostname()))

        hosts = ['%s:%d' % (h, bokeh_port) for h in hosts]

        hosts.append("*")

        hosts.extend(map(str, bokeh_whitelist))

        args = ([binname, 'serve'] + paths +
                ['--log-level', 'warning',
                 '--check-unused-sessions=50',
                 '--unused-session-lifetime=1',
                 '--port', str(bokeh_port)] +
                 sum([['--host', h] for h in hosts], []))

        if prefix:
            args.extend(['--prefix', prefix])

        if show:
            args.append('--show')

        if use_xheaders:
            args.append('--use-xheaders')

        if log_level in ('debug', 'info', 'warning', 'error', 'critical'):
            args.extend(['--log-level', log_level])

        bokeh_options = {'host': host,
                         'http-port': http_port,
                         'tcp-port': tcp_port,
                         'bokeh-port': bokeh_port}
        with open(os.path.join(dask_dir, '.dask-web-ui.json'), 'w') as f:
            json.dump(bokeh_options, f, indent=2)

        if sys.version_info[0] >= 3:
            ctx = multiprocessing.get_context('spawn')
            self.process = ctx.Process(target=bokeh_main, args=(args,), daemon=True)
            self.process.start()
        else:
            import subprocess
            self.process = subprocess.Popen(args, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)

        if not quiet:
            logger.info(" Bokeh UI at:  http://%s:%d/status/"
                         % (ip, bokeh_port))

    def close(self, join=True, timeout=None):
        if sys.version_info[0] >= 3:
            try:
                if self.process.is_alive():
                    self.process.terminate()
            except AssertionError:
                self.process.terminate()
            if join:
                self.process.join(timeout=timeout)
        else:
            if self.process.returncode is None:
                self.process.terminate()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def bokeh_main(args):
    from bokeh.command.bootstrap import main
    import logging
    logger = logging.getLogger('bokeh').setLevel(logging.CRITICAL)
    main(args)
