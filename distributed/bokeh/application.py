
import json
import logging
import multiprocessing
import os
import socket
import sys

import bokeh
import distributed.bokeh

dirname = os.path.dirname(distributed.__file__)
paths = [os.path.join(dirname, 'bokeh', name)
         for name in ['status', 'tasks']]

binname = 'bokeh.exe' if sys.platform.startswith('win') else 'bokeh'
binname = os.path.join(os.path.dirname(sys.argv[0]), binname)

logger = logging.getLogger(__file__)

class BokehWebInterface(object):
    def __init__(self, host='127.0.0.1', http_port=9786, tcp_port=8786,
                 bokeh_port=8787, bokeh_whitelist=[], log_level='info', show=False):
        self.port = bokeh_port
        ip = socket.gethostbyname(host)

        hosts = ['%s:%d' % (h, bokeh_port) for h in
                 ['localhost',
                  '127.0.0.1',
                  ip, socket.gethostbyname(ip),
                  socket.gethostname(),
                  socket.gethostbyname(socket.gethostname()),
                  host] + list(map(str, bokeh_whitelist))]

        args = ([binname, 'serve'] + paths +
                ['--log-level', 'warning',
                 '--check-unused-sessions=50',
                 '--unused-session-lifetime=1',
                 '--port', str(bokeh_port)] +
                 sum([['--host', h] for h in hosts], []))
        if show:
            args.append('--show')

        if log_level in ('debug', 'info', 'warning', 'error', 'critical'):
            args.extend(['--log-level', log_level])

        bokeh_options = {'host': host,
                         'http-port': http_port,
                         'tcp-port': tcp_port,
                         'bokeh-port': bokeh_port}
        with open('.dask-web-ui.json', 'w') as f:
            json.dump(bokeh_options, f, indent=2)

        if sys.version_info[0] >= 3:
            from bokeh.command.bootstrap import main
            ctx = multiprocessing.get_context('spawn')
            self.process = ctx.Process(target=main, args=(args,))
            self.process.daemon = True
            self.process.start()
        else:
            import subprocess
            self.process = subprocess.Popen(args)

        logger.info(" Bokeh UI at:  http://%s:%d/status/"
                    % (ip, bokeh_port))

    def close(self):
        if sys.version_info[0] >= 3:
            try:
                if self.process.is_alive():
                    self.process.terminate()
            except AssertionError:
                self.process.terminate()
        else:
            if self.process.returncode is None:
                self.process.terminate()

    def __del__(self):
        self.close()
