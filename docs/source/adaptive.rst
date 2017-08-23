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
Kubernetes, etc... These systems can be quite different from each other, but
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
for new workers, and when to close idle ones.  This class requires both a
scheduler and a cluster object.

The cluster object must support two methods, ``scale_up(n, **kwargs)``, which
takes in a target number of total workers for the cluster and
``scale_down(workers)``, which takes in a list of addresses to remove from the
cluster.  The Adaptive class will call these methods with the correct values at
the correct times.

.. code-block:: python

   class MyCluster(object):
       @gen.coroutine
       def scale_up(self, n, **kwargs):
           """
           Bring the total count of workers up to ``n``

           This function/coroutine should bring the total number of workers up to
           the number ``n``.

           This can be implemented either as a function or as a Tornado coroutine.
           """
           raise NotImplementedError()

       @gen.coroutine
       def scale_down(self, workers):
           """
           Remove ``workers`` from the cluster

           Given a list of worker addresses this function should remove those
           workers from the cluster.  This may require tracking which jobs are
           associated to which worker address.

           This can be implemented either as a function or as a Tornado coroutine.
           """

   from distributed.deploy import Adaptive

   scheduler = Scheduler()
   cluster = MyCluster()
   adapative_cluster = Adaptive(scheduler, cluster)
   scheduler.start()

Implementing these ``scale_up`` and ``scale_down`` functions depends strongly
on the cluster management system.  See :doc:`LocalCluster <local-cluster>` for
an example.


Marathon: an example
--------------------

We now present an example project that implements this cluster interface backed
by the Marathon cluster management tool on Mesos.  Full source code and testing
apparatus is available here: http://github.com/mrocklin/dask-marathon

The implementation is small.  It uses the Marathon HTTP API through the
`marathon Python client library <https://github.com/thefactory/marathon-python>`_.
We reproduce the full body of the implementation below as an example:

.. code-block:: python

   from marathon import MarathonClient, MarathonApp
   from marathon.models.container import MarathonContainer

   class MarathonCluster(object):
       def __init__(self, scheduler,
                    executable='dask-worker',
                    docker_image='mrocklin/dask-distributed',
                    marathon_address='http://localhost:8080',
                    name=None, **kwargs):
           self.scheduler = scheduler

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
           self.client = MarathonClient(marathon_address)
           self.app = self.client.create_app(name or 'dask-%s' % uuid.uuid4(), app)

       def scale_up(self, instances):
           self.marathon_client.scale_app(self.app.id, instances=instances)

       def scale_down(self, workers):
           for w in workers:
               self.marathon_client.kill_task(self.app.id,
                                              self.scheduler.worker_info[w]['name'],
                                              scale=True)

Subclassing Adaptive
--------------------

The default behaviors of ``Adaptive`` controlling when to scale up or down, and
by how much, may not be appropriate for your cluster manager or workload. For
example, you may have tasks that require a worker with more memory than usual.
This means we need to pass through some additional keyword arguments to
``cluster.scale_up`` call.

.. code-block:: python

   from distributed.deploy import Adaptive

   class MyAdaptive(Adaptive):
       def get_scale_up_kwargs(self):
           kwargs = super(Adaptive, self).get_scale_up_kwargs()
           # resource_restrictions maps task keys to a dict of restrictions
           restrictions = self.scheduler.resource_restrictions.values()
           memory_restrictions = [x.get('memory') for x in restrictions
                                  if 'memory' in x]

           if memory_restrictions:
               kwargs['memory'] = max(memory_restrictions)

           return kwargs


So if there are any tasks that are waiting to be run on a worker with enough
memory, the ``kwargs`` dictionary passed to ``cluster.scale_up`` will include
a key and value for ``'memory'`` (your ``Cluster.scale_up`` method needs to be
able to support this).
