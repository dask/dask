import json
import logging

from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.httpclient import AsyncHTTPClient
from tornado import gen

from .scheduler import scheduler_progress_df, worker_status_df

logger = logging.getLogger(__name__)


def scheduler_status_widget(d, widget=None):
    """ IPython widget to display scheduler status

    See also:
        scheduler_status_str
    """
    from ipywidgets import HTML, VBox
    if widget is None:
        widget = VBox([HTML(''), HTML(''), HTML('')])
    header = '<h3>Scheduler: %s</h3>' % d['address']
    sched = scheduler_progress_df(d)
    workers = worker_status_df(d)
    widget.children[0].value = header
    widget.children[1].value = sched.style.set_table_attributes('class="table"').render()
    widget.children[2].value = workers.style.set_table_attributes('class="table"').render()
    logger.debug("Update scheduler status widget")
    return widget


@gen.coroutine
def update_status_widget(widget, ip, port):
    client = AsyncHTTPClient()
    try:
        response = yield client.fetch('http://%s:%d/status.json' % (ip, port))
        d = json.loads(response.body.decode())
        scheduler_status_widget(d, widget)
    except Exception as e:
        logger.exception(e)
        raise


def scheduler_status(e, interval=200, port=9786, loop=None):
    from ipywidgets import HTML, VBox
    loop = loop or IOLoop.current()
    widget = VBox([HTML(''), HTML(''), HTML('')])
    cb = lambda: update_status_widget(widget, e.scheduler.ip, port)
    loop.add_callback(cb)
    pc = PeriodicCallback(cb, interval, io_loop=loop)
    pc.start()

    return widget
