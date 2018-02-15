from __future__ import print_function, division, absolute_import

from .plugin import SchedulerPlugin


class GraphLayout(SchedulerPlugin):
    """ Dynamic graph layout during computation

    This assigns (x, y) locations to all tasks quickly and dynamically as new
    tasks are added.  This scales to a few thousand nodes.

    It is commonly used with distributed/bokeh/scheduler.py::GraphPlot, which
    is rendered at /graph on the diagnostic dashboard.
    """
    def __init__(self, scheduler):
        self.x = {}
        self.y = {}
        self.scheduler = scheduler
        self.index = {}
        self.index_edge = {}
        self.next_y = 0
        self.next_index = 0
        self.next_edge_index = 0
        self.new = []
        self.new_edges = []
        self.state_updates = []
        self.visible_updates = []
        self.visible_edge_updates = []

        scheduler.add_plugin(self)

        if self.scheduler.tasks:
            dependencies = {k: [ds.key for ds in ts.dependencies]
                            for k, ts in scheduler.tasks.items()}
            priority = {k: ts.priority for k, ts in scheduler.tasks.items()}
            self.update_graph(self.scheduler, dependencies=dependencies,
                    priority=priority)

    def update_graph(self, scheduler, dependencies=None, priority=None,
                     **kwargs):
        for key in sorted(dependencies, key=priority.get):
            deps = dependencies[key]
            if key in self.x or key not in scheduler.tasks:
                continue
            if deps:
                total_deps = sum(len(scheduler.tasks[dep].dependents)
                                 for dep in deps)
                y = sum(self.y[dep] * len(scheduler.tasks[dep].dependents)
                                      / total_deps
                        for dep in deps)
                x = max(self.x[dep] for dep in deps) + 1
            else:
                x = 0
                y = self.next_y
                self.next_y += 1

            self.x[key] = x
            self.y[key] = y
            self.index[key] = self.next_index
            self.next_index = self.next_index + 1
            self.new.append(key)
            for dep in deps:
                edge = (dep, key)
                self.index_edge[edge] = self.next_edge_index
                self.next_edge_index += 1
                self.new_edges.append(edge)

    def transition(self, key, start, finish, *args, **kwargs):
        if finish != 'forgotten':
            self.state_updates.append((self.index[key], finish))
        else:
            self.visible_updates.append((self.index[key], 'False'))
            task = self.scheduler.tasks[key]
            for dep in task.dependents:
                edge = (key, dep.key)
                self.visible_edge_updates.append((self.index_edge.pop((key, dep.key)), 'False'))
            for dep in task.dependencies:
                self.visible_edge_updates.append((self.index_edge.pop((dep.key, key)), 'False'))

            for collection in [self.x, self.y, self.index]:
                del collection[key]

    def reset_index(self):
        """ Reset the index and refill new and new_edges

        From time to time GraphPlot wants to remove invisible nodes and reset
        all of its indices.  This helps.
        """
        self.new = []
        self.new_edges = []
        self.visible_updates = []
        self.state_updates = []
        self.visible_edge_updates = []

        self.index = {}
        self.next_index = 0
        self.index_edge = {}
        self.next_edge_index = 0

        for key in self.x:
            self.index[key] = self.next_index
            self.next_index += 1
            self.new.append(key)
            for dep in self.scheduler.tasks[key].dependencies:
                edge = (dep.key, key)
                self.index_edge[edge] = self.next_edge_index
                self.next_edge_index += 1
                self.new_edges.append(edge)
