from __future__ import annotations

import logging
import ssl
import warnings
import weakref
from contextlib import suppress

import tlz
from tornado.httpserver import HTTPServer

import dask

from distributed.comm import get_address_host, get_tcp_server_addresses
from distributed.core import Server
from distributed.http.routing import RoutingApplication
from distributed.utils import DequeHandler, clean_dashboard_address
from distributed.versions import get_versions


class ServerNode(Server):
    """
    Base class for server nodes in a distributed cluster.
    """

    # TODO factor out security, listening, services, etc. here

    # XXX avoid inheriting from Server? there is some large potential for confusion
    # between base and derived attribute namespaces...

    def versions(self, packages=None):
        return get_versions(packages=packages)

    def start_services(self, default_listen_ip):
        if default_listen_ip == "0.0.0.0":
            default_listen_ip = ""  # for IPV6

        for k, v in self.service_specs.items():
            listen_ip = None
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            if isinstance(port, str):
                port = port.split(":")

            if isinstance(port, (tuple, list)):
                if len(port) == 2:
                    listen_ip, port = (port[0], int(port[1]))
                elif len(port) == 1:
                    [listen_ip], port = port, 0
                else:
                    raise ValueError(port)

            if isinstance(v, tuple):
                v, kwargs = v
            else:
                kwargs = {}

            try:
                service = v(self, io_loop=self.loop, **kwargs)
                service.listen(
                    (listen_ip if listen_ip is not None else default_listen_ip, port)
                )
                self.services[k] = service
            except Exception as e:
                warnings.warn(
                    f"\nCould not launch service '{k}' on port {port}. "
                    + "Got the following message:\n\n"
                    + str(e),
                    stacklevel=3,
                )

    def stop_services(self):
        if hasattr(self, "http_application"):
            for application in self.http_application.applications:
                if hasattr(application, "stop") and callable(application.stop):
                    application.stop()
        for service in self.services.values():
            service.stop()

    @property
    def service_ports(self):
        return {k: v.port for k, v in self.services.items()}

    def _setup_logging(self, logger: logging.Logger) -> None:
        self._deque_handler = DequeHandler(
            n=dask.config.get("distributed.admin.log-length")
        )
        self._deque_handler.setFormatter(
            logging.Formatter(dask.config.get("distributed.admin.log-format"))
        )
        logger.addHandler(self._deque_handler)
        weakref.finalize(self, logger.removeHandler, self._deque_handler)

    def get_logs(self, start=0, n=None, timestamps=False):
        """
        Fetch log entries for this node

        Parameters
        ----------
        start : float, optional
            A time (in seconds) to begin filtering log entries from
        n : int, optional
            Maximum number of log entries to return from filtered results
        timestamps : bool, default False
            Do we want log entries to include the time they were generated?

        Returns
        -------
        List of tuples containing the log level, message, and (optional) timestamp for each filtered entry, newest first
        """
        deque_handler = self._deque_handler

        L = []
        for count, msg in enumerate(reversed(deque_handler.deque)):
            if n and count >= n or msg.created < start:
                break
            if timestamps:
                L.append((msg.created, msg.levelname, deque_handler.format(msg)))
            else:
                L.append((msg.levelname, deque_handler.format(msg)))
        return L

    def start_http_server(
        self, routes, dashboard_address, default_port=0, ssl_options=None
    ):
        """This creates an HTTP Server running on this node"""

        self.http_application = RoutingApplication(routes)

        # TLS configuration
        tls_key = dask.config.get("distributed.scheduler.dashboard.tls.key")
        tls_cert = dask.config.get("distributed.scheduler.dashboard.tls.cert")
        tls_ca_file = dask.config.get("distributed.scheduler.dashboard.tls.ca-file")
        if tls_cert:
            ssl_options = ssl.create_default_context(
                cafile=tls_ca_file, purpose=ssl.Purpose.CLIENT_AUTH
            )
            ssl_options.load_cert_chain(tls_cert, keyfile=tls_key)

        self.http_server = HTTPServer(self.http_application, ssl_options=ssl_options)

        http_addresses = clean_dashboard_address(dashboard_address or default_port)
        for http_address in http_addresses:
            # Handle default case for dashboard address
            # In case dashboard_address is given, e.g. ":8787"
            # the address is empty and it is intended to listen to all interfaces
            if dashboard_address is not None and http_address["address"] == "":
                http_address["address"] = "0.0.0.0"

            if http_address["address"] is None or http_address["address"] == "":
                address = self._start_address
                if isinstance(address, (list, tuple)):
                    address = address[0]
                if address:
                    with suppress(ValueError):
                        http_address["address"] = get_address_host(address)

            change_port = False
            retries_left = 3
            while True:
                try:
                    if not change_port:
                        self.http_server.listen(**http_address)
                    else:
                        self.http_server.listen(**tlz.merge(http_address, {"port": 0}))
                    break
                except Exception:
                    change_port = True
                    retries_left = retries_left - 1
                    if retries_left < 1:
                        raise

        bound_addresses = get_tcp_server_addresses(self.http_server)

        # If more than one address is configured we just use the first here
        # Socket addresses representation: https://docs.python.org/3/library/socket.html#socket-families
        self.http_server.address, self.http_server.port = bound_addresses[0][:2]
        self.services["dashboard"] = self.http_server

        # Warn on port changes
        for expected, actual in zip(
            [a["port"] for a in http_addresses], [b[1] for b in bound_addresses]
        ):
            if expected != actual and expected > 0:
                warnings.warn(
                    f"Port {expected} is already in use.\n"
                    "Perhaps you already have a cluster running?\n"
                    f"Hosting the HTTP server on port {actual} instead"
                )
