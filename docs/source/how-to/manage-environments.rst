Manage environments
===================

It is critical that each of your dask workers uses the same set of
python packages and modules when executing your code, so that Dask
can function. Upon connecting with a distributed ``Client``, Dask
will automatically check the versions of some critical packages
(including Dask itself) and warn you of any mismatch.

Most functions you will run on Dask will require imports. This
is even true for passing any object which is not a python builtin -
the pickle serialisation method will save references to imported modules
rather than trying to send all of your source code.

You therefore must ensure that workers have access to all of the modules
you will need, and ideally with exactly the same versions.

Single-machine schedulers
`````````````````````````

If you are using the threaded scheduler, then you do not need to do
anything, since the workers are in the same process, and objects are
simply shared rather than serialised and deserialised.

Similarly, if you use the multiprocessing scheduler, new processes
will be copied from, or launched in the same way as the original process,
so you only need make sure that you have not changed environment variables
related to starting up python and importing
code (such as PATH, PYTHONPATH, ``sys.path``).

If you are using the distributed scheduler on a single machine, this is roughly
equivalent to using the multiprocessing scheduler, above, if you are launching
with ``Client(...)`` or ``LocalCluster(...)``.

However, if you are launching your workers from the command line, then you must
ensure that you are running in the same environment (virtualenv, pipenv or conda).

The rest of this page concerns only distributed clusters.

Maintain consistent environments
````````````````````````````````

If you manage your environments yourself, then setting up module consistency
can be as simple as creating environments from the same pip or conda specification
on each machine. You should consult the documentation for ``pip``, ``pipenv``
and ``conda``, whichever you normally use. You will normally want to be as specific
about package versions as possible, and distribute the same environment file to
workers before installation.

However, other common ways to distribute an environment directly, rather than build it
in-place, include:

- docker images, where the environment has been built into the image; this is the
  normal route when you are running on infrastructure enabled by docker, such as
  kubernetes
- `conda-pack`_ is a tool for bundling existing conda environments, so they can be
  relocated to other machines. This tool was specifically created for dask on YARN/hadoop
  clusters, but could be used elsewhere
- shared filesystem, e.g., NFS, that can be seen by all machines. Note that importing
  python modules is fairly IO intensive, so your server needs to be able to handle
  many requests
- cluster install method (e.g., `parcels`_): depending on your infrastructure, there may be
  ways to install specific binaries to all workers in a cluster.

.. _conda-pack: https://conda.github.io/conda-pack/
.. _parcels: https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_ig_parcels.html

Temporary installations
```````````````````````
The worker plugin ``distributed.diagnostics.plugin.PipInstall`` allows you to
run pip installation commands on your workers, and optionally have them restart
upon success. Please read the plugin documentation to see how to use this.

Objects in ``__main__``
```````````````````````

Objects that you create without any reference to modules, such as classes that
are defined right in the repl or notebook cells, are not pickled with reference to
any imported module. You can redefine such objects, and Dask will serialise them
completely, including the source code. This can be a good way to try new things
while working with a distributed setup.

Send Source
```````````

Particularly during development, you may want to send files directly to workers
that are already running.

You should use ``client.upload_file`` in these cases.
For more detail, see the `API docs`_ and a
StackOverflow question
`"Can I use functions imported from .py files in Dask/Distributed?"`__
This function supports both standalone file and setuptools's ``.egg`` files
for larger modules.

__ http://stackoverflow.com/questions/39295200/can-i-use-functions-imported-from-py-files-in-dask-distributed
.. _API docs: https://distributed.readthedocs.io/en/latest/api.html#distributed.executor.Executor.upload_file
