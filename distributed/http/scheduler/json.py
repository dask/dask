from ..utils import RequestHandler
from ...utils import log_errors


class CountsJSON(RequestHandler):
    def get(self):
        scheduler = self.server
        erred = 0
        nbytes = 0
        nthreads = 0
        memory = 0
        processing = 0
        released = 0
        waiting = 0
        waiting_data = 0
        desired_workers = scheduler.adaptive_target()

        for ts in scheduler.tasks.values():
            if ts.exception_blame is not None:
                erred += 1
            elif ts.state == "released":
                released += 1
            if ts.waiting_on:
                waiting += 1
            if ts.waiters:
                waiting_data += 1
        for ws in scheduler.workers.values():
            nthreads += ws.nthreads
            memory += len(ws.has_what)
            nbytes += ws.nbytes
            processing += len(ws.processing)

        response = {
            "bytes": nbytes,
            "clients": len(scheduler.clients),
            "cores": nthreads,
            "erred": erred,
            "hosts": len(scheduler.host_info),
            "idle": len(scheduler.idle),
            "memory": memory,
            "processing": processing,
            "released": released,
            "saturated": len(scheduler.saturated),
            "tasks": len(scheduler.tasks),
            "unrunnable": len(scheduler.unrunnable),
            "waiting": waiting,
            "waiting_data": waiting_data,
            "workers": len(scheduler.workers),
            "desired_workers": desired_workers,
        }
        self.write(response)


class IdentityJSON(RequestHandler):
    def get(self):
        self.write(self.server.identity())


class IndexJSON(RequestHandler):
    def get(self):
        with log_errors():
            r = [url[5:] for url, _, _ in routes if url.endswith(".json")]
            self.render(
                "json-index.html", routes=r, title="Index of JSON routes", **self.extra
            )


routes = [
    (r"json/counts.json", CountsJSON, {}),
    (r"json/identity.json", IdentityJSON, {}),
    (r"json/index.html", IndexJSON, {}),
]
