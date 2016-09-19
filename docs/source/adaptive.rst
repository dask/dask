Adaptive Deployments
====================

It is possible to grow and shrink Dask clusters based on current use.  This
allows you to run Dask permanently on your cluster and have it only take up
resources when necessary.  Dask contains the logic about when to grow and
shrink but relies on external cluster managers to launch and kill
``dask-worker`` jobs.  This page describes the policies about adaptively
resizing Dask clusters based on load, how to connect these policies to a
particular job scheduler, and an example implementation.

Dynamically scaling a Dask cluster up and down requires tight integration with
an external cluster management system that can deploy ``dask-worker`` jobs
throughout the cluster.  Several systems are in wide use today, including
common examples like SGE, SLURM, Torque, Condor, LSF, Yarn, Mesos, Marathon,
Kubernetes, etc..  These systems can be quite different from each other, but
all are used to manage distributed services throughout different kinds of
clusters.

The large number of relevant systems, the challenges of rigorously testing
each, and finite development time precludes the systematic inclusion of all
solutions within the dask/distributed repository.  Instead, we include a
generic interface that can be extended by someone with basic understanding of
their cluster management tool.  We encourage these as third party modules.


Policies
--------

We control the number of workers based on current load and memory use.  The
scheduler checks itself periodically to determine if more or fewer workers are
needed.

If there are excess unclaimed tasks, or if the memory of the current workers is
more nearing full then the scheduler tries to increase the number of workers by
a fixed factor, defaulting to 2.  This causes exponential growth while growth
is useful.

If there are idle workers and if the memory of the current workers is nearing
empty then we gracefully retire the idle workers with the least amount of data
in memory.  We first move these results to the surviving workers and then
remove the idle workers from the cluster.  This shrinks the cluster while
gracefully preserving intermediate results, shrinking the cluster when excess
size is not useful.


Adaptive class interface
------------------------

The ``distributed.deploy.Adaptive`` class contains the logic about when to ask
for new workers, and when to close idle ones.  This class is incomplete and
needs to have two methods filled in by subclassing to start and stop workers.

.. currentmodule:: distributed.deploy.adaptive

.. autoclass:: Adaptive
   :members: scale_up, scale_down

Implementing these ``scale_up`` and ``scale_down`` functions depends strongly
on the cluster management system.


Marathon: an example
--------------------

We now present an example project that implements this ``Adaptive`` class to
create an adaptive Dask cluster backed by the Marathon cluster management
tool backed by Mesos.  Full source code and testing apparatus is available
here: http://github.com/mrocklin/dask-marathon

The implementation is small.  It uses the Marathon HTTP API through the
`marathon Python client library <https://github.com/thefactory/marathon-python>`_.
We reproduce the full body of the implementation below as an example:

.. code-block:: python

   from distributed.deploy import Adaptive

   from marathon import MarathonClient, MarathonApp
   from marathon.models.container import MarathonContainer


   class AdaptiveCluster(Adaptive):
       def __init__(self, scheduler,
                    executable='dask-worker',
                    docker_image='mrocklin/dask-distributed',
                    marathon_address='http://localhost:8080', **kwargs):
           # Create Marathon App to run dask-worker
           args = [executable, scheduler.address,
                   '--name', '$MESOS_TASK_ID']  # use Mesos task ID as worker name
           if 'mem' in kwargs:
               args.extend(['--memory-limit',
                            str(int(kwargs['mem'] * 0.6 * 1e6))])
           kwargs['cmd'] = ' '.join(args)
           container = MarathonContainer({'image': docker_image})
           app = MarathonApp(instances=0, container=container, **kwargs)

           # Connect and register app
           self.marathon_client = MarathonClient(marathon_address)
           self.app = self.marathon_client.create_app('dask-%s' % uuid.uuid4(), app)

           self.scheduler = scheduler
           super(AdaptiveCluster, self).__init__()

       def scale_up(self, instances):
           self.marathon_client.scale_app(self.app.id, instances=instances)

       def scale_down(self, workers):
           for w in workers:
               self.marathon_client.kill_task(self.app.id,
                                              self.scheduler.worker_info[w]['name'],
                                              scale=True)
