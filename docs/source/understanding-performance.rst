Understanding Performance
=========================

The first step to making computations run quickly is to understand the costs involved.
In Python we often rely on tools like
the `CProfile module <https://docs.python.org/3/library/profile.html>`_,
the `%%prun IPython magic <http://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-prun>`_
the `VMProf <https://vmprof.readthedocs.io/en/latest/>`_ module or
the `snakeviz module <https://jiffyclub.github.io/snakeviz/>`_
to understand the costs associated with our code.
However, few of these tools work well on multi-threaded or multi-process code,
and fewer still on computations distributed among many machines.
We also have new costs like data transfer, serialization, task scheduling overhead, and more
that we may not be accustomed to tracking.

Fortunately the Dask schedulers come with diagnostics
to help you understand the performance characteristics of your computations.
By using these diagnostics and a little thought
we can often identify the slow parts of troublesome computations.

The :doc:`single-machine and distributed schedulers <scheduling>` come with *different* diagnostic tools.
These tools are deeply integrated into each scheduler
so a tool designed for one will not transfer over to the other.

These pages provide four options for profiling parallel code:

1.  :doc:`Visualize task graphs <graphviz>`
2.  :ref:`Single threaded scheduler and a normal Python profiler <single-threaded-scheduler>`
3.  :doc:`Diagnostics for the single-machine scheduler <diagnostics-local>`
4.  :doc:`Dask.distributed dashboard <diagnostics-distributed>`
