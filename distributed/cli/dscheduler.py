from __future__ import print_function, division, absolute_import

import json
import logging
import multiprocessing
import os
import socket
import subprocess
import sys
from time import sleep

import click

import distributed
from distributed import Scheduler
from distributed.utils import get_ip
from distributed.http import HTTPScheduler
from distributed.cli.utils import check_python_3
from tornado.ioloop import IOLoop

logger = logging.getLogger('distributed.scheduler')

import signal

def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@click.command()
@click.argument('center', type=str, default='')
@click.option('--port', type=int, default=8786, help="Serving port")
@click.option('--http-port', type=int, default=9786, help="HTTP port")
@click.option('--bokeh-port', type=int, default=8787, help="HTTP port")
@click.option('--bokeh/--no-bokeh', '_bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--host', type=str, default=None,
              help="IP or hostname of this server")
@click.option('--show/--no-show', default=False, help="Show web UI")
@click.option('--bokeh-whitelist', default=None, multiple=True,
              help="IP addresses to whitelist for bokeh.")
def main(center, host, port, http_port, bokeh_port, show, _bokeh, bokeh_whitelist):
    given_host = host
    host = host or get_ip()
    ip = socket.gethostbyname(host)
    loop = IOLoop.current()
    scheduler = Scheduler(center, ip=ip,
                          services={('http', http_port): HTTPScheduler})
    if center:
        loop.run_sync(scheduler.sync_center)
    scheduler.start(port)

    if _bokeh:
        try:
            import bokeh
            import distributed.bokeh
            hosts = ['%s:%d' % (h, bokeh_port) for h in
                     ['localhost', '127.0.0.1', ip, socket.gethostname(),
                      host] + list(bokeh_whitelist)]
            dirname = os.path.dirname(distributed.__file__)
            paths = [os.path.join(dirname, 'bokeh', name)
                     for name in ['status', 'tasks']]
            binname = 'bokeh.bat' if 'win' in sys.platform else 'bokeh'
            binname = os.path.join(os.path.dirname(sys.argv[0]), binname)
            args = ([binname, 'serve'] + paths +
                    ['--log-level', 'warning',
                     '--check-unused-sessions=50',
                     '--unused-session-lifetime=1',
                     '--port', str(bokeh_port)] +
                     sum([['--host', host] for host in hosts], []))
            if show:
                args.append('--show')

            bokeh_options = {'host': host if given_host else '127.0.0.1',
                             'http-port': http_port,
                             'tcp-port': port,
                             'bokeh-port': bokeh_port}
            with open('.dask-web-ui.json', 'w') as f:
                json.dump(bokeh_options, f, indent=2)

            if sys.version_info[0] >= 3:
                from bokeh.command.bootstrap import main
                ctx = multiprocessing.get_context('spawn')
                bokeh_proc = ctx.Process(target=main, args=(args,))
                bokeh_proc.daemon = True
                bokeh_proc.start()
            else:
                bokeh_proc = subprocess.Popen(args)

            logger.info(" Bokeh UI at:  http://%s:%d/status/"
                        % (ip, bokeh_port))
        except ImportError:
            logger.info("Please install Bokeh to get Web UI")
        except Exception as e:
            logger.warn("Could not start Bokeh web UI", exc_info=True)

    loop.start()
    loop.close()
    scheduler.stop()
    bokeh_proc.terminate()

    logger.info("End scheduler at %s:%d", ip, port)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
