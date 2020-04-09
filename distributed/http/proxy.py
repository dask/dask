import logging

from tornado import web

logger = logging.getLogger(__name__)

try:
    from jupyter_server_proxy.handlers import ProxyHandler

    class GlobalProxyHandler(ProxyHandler):
        """
        A tornado request handler that proxies HTTP and websockets
        from a port to any valid endpoint'.
        """

        def initialize(self, dask_server=None, extra=None):
            self.scheduler = dask_server
            self.extra = extra or {}

        async def http_get(self, port, host, proxied_path):
            # route here first
            # incoming URI /proxy/{port}/{host}/{proxied_path}

            self.host = host

            # rewrite uri for jupyter-server-proxy handling
            uri = "/proxy/%s/%s" % (str(port), proxied_path)
            self.request.uri = uri

            # slash is removed during regex in handler
            proxied_path = "/%s" % proxied_path

            worker = "%s:%s" % (self.host, str(port))
            if not check_worker_dashboard_exits(self.scheduler, worker):
                msg = "Worker <%s> does not exist" % worker
                self.set_status(400)
                self.finish(msg)
                return
            return await self.proxy(port, proxied_path)

        async def open(self, port, host, proxied_path):
            # finally, proxy to other address/port
            return await self.proxy_open(host, port, proxied_path)

        def post(self, port, proxied_path):
            return self.proxy(port, proxied_path)

        def put(self, port, proxied_path):
            return self.proxy(port, proxied_path)

        def delete(self, port, proxied_path):
            return self.proxy(port, proxied_path)

        def head(self, port, proxied_path):
            return self.proxy(port, proxied_path)

        def patch(self, port, proxied_path):
            return self.proxy(port, proxied_path)

        def options(self, port, proxied_path):
            return self.proxy(port, proxied_path)

        def proxy(self, port, proxied_path):
            # router here second
            # returns ProxyHandler coroutine
            return super().proxy(self.host, port, proxied_path)


except ImportError:
    logger.info(
        "To route to workers diagnostics web server "
        "please install jupyter-server-proxy: "
        "python -m pip install jupyter-server-proxy"
    )

    class GlobalProxyHandler(web.RequestHandler):
        """Minimal Proxy handler when jupyter-server-proxy is not installed
        """

        def initialize(self, dask_server=None, extra=None):
            self.server = dask_server
            self.extra = extra or {}

        def get(self, port, host, proxied_path):
            worker_url = "%s:%s/%s" % (host, str(port), proxied_path)
            msg = """
                <p> Try navigating to <a href=http://%s>%s</a> for your worker dashboard </p>

                <p>
                Dask tried to proxy you to that page through your
                Scheduler's dashboard connection, but you don't have
                jupyter-server-proxy installed.  You may want to install it
                with either conda or pip, and then restart your scheduler.
                </p>

                <p><pre> conda install jupyter-server-proxy -c conda-forge </pre></p>
                <p><pre> python -m pip install jupyter-server-proxy</pre></p>

                <p>
                The link above should work though if your workers are on a
                sufficiently open network.  This is common on single machines,
                but less common in production clusters.  Your IT administrators
                will know more
                </p>
            """ % (
                worker_url,
                worker_url,
            )
            self.write(msg)


def check_worker_dashboard_exits(scheduler, worker):
    """Check addr:port exists as a worker in scheduler list

    Parameters
    ----------
    worker : str
        addr:port

    Returns
    -------
    bool
    """
    addr, port = worker.split(":")
    workers = list(scheduler.workers.values())
    for w in workers:
        bokeh_port = w.services.get("dashboard", "")
        if addr == w.host and port == str(bokeh_port):
            return True
    return False


routes = [
    (r"proxy/(\d+)/(.*?)/(.*)", GlobalProxyHandler, {}),
]
