from .plugin import SchedulerPlugin
from ..utils import key_split
from .task_stream import colors


class WebsocketPlugin(SchedulerPlugin):
    def __init__(self, socket, scheduler):
        self.socket = socket
        self.scheduler = scheduler

    def restart(self, scheduler, **kwargs):
        """ Run when the scheduler restarts itself """
        self.socket.send("restart", {})

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a new worker enters the cluster """
        self.socket.send("add_worker", {"worker": worker})

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a worker leaves the cluster"""
        self.socket.send("remove_worker", {"worker": worker})

    def add_client(self, scheduler=None, client=None, **kwargs):
        """ Run when a new client connects """
        self.socket.send("add_client", {"client": client})

    def remove_client(self, scheduler=None, client=None, **kwargs):
        """ Run when a client disconnects """
        self.socket.send("remove_client", {"client": client})

    def update_graph(self, scheduler, client=None, **kwargs):
        """ Run when a new graph / tasks enter the scheduler """
        self.socket.send("update_graph", {"client": client})

    def transition(self, key, start, finish, *args, **kwargs):
        """ Run whenever a task changes state

        Parameters
        ----------
        key: string
        start: string
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish: string
            Final state of the transition.
        *args, **kwargs: More options passed when transitioning
            This may include worker ID, compute time, etc.
        """
        if key not in self.scheduler.tasks:
            return
        kwargs["key"] = key
        startstops = kwargs.get("startstops", [])
        for startstop in startstops:
            color = colors[startstop["action"]]
            if type(color) is not str:
                color = color(kwargs)
            data = {
                "key": key,
                "name": key_split(key),
                "color": color,
                **kwargs,
                **startstop,
            }
            self.socket.send("transition", data)
