Scheduling State
================

.. currentmodule:: distributed.scheduler

Overview
--------

The life of a computation with Dask can be described in the following stages:

1.  The user authors a graph using some library, perhaps dask.delayed or
    dask.dataframe or the ``submit/map`` functions on the client.  They submit
    these tasks to the scheduler.
2.  The schedulers assimilates these tasks into its graph of all tasks to
    track, and as their dependencies become available it asks workers to run
    each of these tasks in turn.
3.  The worker receives information about how to run the task, communicates
    with its peer workers to collect data dependencies, and then runs the
    relevant function on the appropriate data.  It reports back to the
    scheduler that it has finished, keeping the result stored in the worker
    where it was computed.
4.  The scheduler reports back to the user that the task has completed.  If the
    user desires, it then fetches the data from the worker through the
    scheduler.

Most relevant logic is in tracking tasks as they evolve from newly submitted,
to waiting for dependencies, to actively running on some worker, to finished in
memory, to garbage collected.  Tracking this process, and tracking all effects
that this task has on other tasks that might depend on it, is the majority of
the complexity of the dynamic task scheduler.  This section describes the
system used to perform this tracking.

For more abstract information about the policies used by the scheduler, see
:doc:`Scheduling Policies<scheduling-policies>`.

The scheduler keeps internal state about several kinds of entities:

* Individual tasks known to the scheduler
* Workers connected to the scheduler
* Clients connected to the scheduler


.. note::
   Everything listed in this page is an internal detail of how Dask operates.
   It may change between versions and you should probably avoid relying on it
   in user code (including on any APIs explained here).


Task State
----------

Internally, the scheduler moves tasks between a fixed set of states,
notably ``released``, ``waiting``, ``no-worker``, ``processing``,
``memory``, ``error``.

Tasks flow along the following states with the following allowed transitions:

.. image:: images/task-state.svg
    :alt: Dask scheduler task states

*  *Released*: Known but not actively computing or in memory
*  *Waiting*: On track to be computed, waiting on dependencies to arrive in
   memory
*  *No-worker*: Ready to be computed, but no appropriate worker exists
   (for example because of resource restrictions, or because no worker is
   connected at all).
*  *Processing*: Actively being computed by one or more workers
*  *Memory*: In memory on one or more workers
*  *Erred*: Task computation, or one of its dependencies, has encountered an error
*  *Forgotten* (not actually a state): Task is no longer needed by any client
   or dependent task

In addition to the literal state, though, other information needs to be
kept and updated about each task.  Individual task state is stored in an
object named :class:`TaskState` and consists of the following information:

.. autoclass:: TaskState


The scheduler keeps track of all the :class:`TaskState` objects (those
not in the "forgotten" state) using several containers:

.. attribute:: tasks: {str: TaskState}

   A dictionary mapping task keys (usually strings) to :class:`TaskState`
   objects.  Task keys are how information about tasks is communicated
   between the scheduler and clients, or the scheduler and workers; this
   dictionary is then used to find the corresponding :class:`TaskState`
   object.

.. attribute:: unrunnable: {TaskState}

   A set of :class:`TaskState` objects in the "no-worker" state.  These
   tasks already have all their :attr:`~TaskState.dependencies` satisfied
   (their :attr:`~TaskState.waiting_on` set is empty), and are waiting
   for an appropriate worker to join the network before computing.


Worker State
------------

Each worker's current state is stored in a :class:`WorkerState` object.
This information is involved in deciding
:ref:`which worker to run a task on <decide-worker>`.

.. autoclass:: WorkerState


In addition to individual worker state, the scheduler maintains two
containers to help with scheduling tasks:

.. attribute:: Scheduler.saturated: {WorkerState}

   A set of workers whose computing power (as
   measured by :attr:`WorkerState.ncores`) is fully exploited by processing
   tasks, and whose current :attr:`~WorkerState.occupancy` is a lot greater
   than the average.

.. attribute:: Scheduler.idle: {WorkerState}

   A set of workers whose computing power is not fully exploited.  These
   workers are assumed to be able to start computing new tasks immediately.

These two sets are disjoint.  Also, some workers may be *neither* "idle"
nor "saturated".  "Idle" workers will be preferred when
:ref:`deciding a suitable worker <decide-worker>` to run a new task on.
Conversely, "saturated" workers may see their workload lightened through
:doc:`work-stealing`.


Client State
------------

Information about each individual client is kept in a :class:`ClientState`
object:

.. autoclass:: ClientState


.. XXX list invariants somewhere?


Understanding a Task's Flow
---------------------------

As seen above, there are numerous pieces of information pertaining to
task and worker state, and some of them can be computed, updated or
removed during a task's transitions.

The table below shows which state variable a task is in, depending on the
task's state.  Cells with a check mark (`✓`) indicate the task key *must*
be present in the given state variable; cells with an question mark (`?`)
indicate the task key *may* be present in the given state variable.


================================================================ ======== ======= ========= ========== ====== =====
State variable                                                   Released Waiting No-worker Processing Memory Erred
================================================================ ======== ======= ========= ========== ====== =====
:attr:`TaskState.dependencies`                                   ✓        ✓       ✓         ✓          ✓      ✓
:attr:`TaskState.dependents`                                     ✓        ✓       ✓         ✓          ✓      ✓
---------------------------------------------------------------- -------- ------- --------- ---------- ------ -----
:attr:`TaskState.host_restrictions`                              ?        ?       ?         ?          ?      ?
:attr:`TaskState.worker_restrictions`                            ?        ?       ?         ?          ?      ?
:attr:`TaskState.resource_restrictions`                          ?        ?       ?         ?          ?      ?
:attr:`TaskState.loose_restrictions`                             ?        ?       ?         ?          ?      ?
---------------------------------------------------------------- -------- ------- --------- ---------- ------ -----
:attr:`TaskState.waiting_on`                                              ✓                 ✓
:attr:`TaskState.waiters`                                                 ✓                 ✓
:attr:`TaskState.processing_on`                                                             ✓
:attr:`WorkerState.processing`                                                              ✓
:attr:`TaskState.who_has`                                                                              ✓
:attr:`WorkerState.has_what`                                                                           ✓
:attr:`TaskState.nbytes` *(1)*                                   ?        ?       ?         ?          ✓      ?
:attr:`TaskState.exception` *(2)*                                                                             ?
:attr:`TaskState.traceback` *(2)*                                                                             ?
:attr:`TaskState.exception_blame`                                                                             ✓
:attr:`TaskState.retries`                                        ?        ?       ?         ?          ?      ?
:attr:`TaskState.suspicious_tasks`                               ?        ?       ?         ?          ?      ?
================================================================ ======== ======= ========= ========== ====== =====

Notes:

1. :attr:`TaskState.nbytes`: this attribute can be known as long as a
   task has already been computed, even if it has been later released.

2. :attr:`TaskState.exception` and :attr:`TaskState.traceback` should
   be looked up on the :attr:`TaskState.exception_blame` task.


The table below shows which worker state variables are updated on each
task state transition.

==================================== ==========================================================
Transition                           Affected worker state
==================================== ==========================================================
released → waiting                   occupancy, idle, saturated
waiting → processing                 occupancy, idle, saturated, used_resources
waiting → memory                     idle, saturated, nbytes
processing → memory                  occupancy, idle, saturated, used_resources, nbytes
processing → erred                   occupancy, idle, saturated, used_resources
processing → released                occupancy, idle, saturated, used_resources
memory → released                    nbytes
memory → forgotten                   nbytes
==================================== ==========================================================

.. note::
   Another way of understanding this table is to observe that entering or
   exiting a specific task state updates a well-defined set of worker state
   variables.  For example, entering and exiting the "memory" state updates
   :attr:`WorkerState.nbytes`.


Implementation
--------------

Every transition between states is a separate method in the scheduler.  These
task transition functions are prefixed with ``transition`` and then have the
name of the start and finish task state like the following.

.. code-block:: python

   def transition_released_waiting(self, key):

   def transition_processing_memory(self, key):

   def transition_processing_erred(self, key):

These functions each have three effects.

1.  They perform the necessary transformations on the scheduler state (the 20
    dicts/lists/sets) to move one key between states.
2.  They return a dictionary of recommended ``{key: state}`` transitions to
    enact directly afterwards on other keys.  For example after we transition a
    key into memory we may find that many waiting keys are now ready to
    transition from waiting to a ready state.
3.  Optionally they include a set of validation checks that can be turned on
    for testing.

Rather than call these functions directly we call the central function
``transition``:

.. code-block:: python

   def transition(self, key, final_state):
       """ Transition key to the suggested state """

This transition function finds the appropriate path from the current to the
final state.  It also serves as a central point for logging and diagnostics.

Often we want to enact several transitions at once or want to continually
respond to new transitions recommended by initial transitions until we reach a
steady state.  For that we use the ``transitions`` function (note the plural ``s``).

.. code-block:: python

   def transitions(self, recommendations):
       recommendations = recommendations.copy()
       while recommendations:
           key, finish = recommendations.popitem()
           new = self.transition(key, finish)
           recommendations.update(new)

This function runs ``transition``, takes the recommendations and runs them as
well, repeating until no further task-transitions are recommended.


Stimuli
-------

Transitions occur from stimuli, which are state-changing messages to the
scheduler from workers or clients.  The scheduler responds to the following
stimuli:

* **Workers**
    * Task finished: A task has completed on a worker and is now in memory
    * Task erred: A task ran and erred on a worker
    * Task missing data: A task tried to run but was unable to find necessary
      data on other workers
    * Worker added: A new worker was added to the network
    * Worker removed: An existing worker left the network

* **Clients**
    * Update graph: The client sends more tasks to the scheduler
    * Release keys: The client no longer desires the result of certain keys

Stimuli functions are prepended with the text ``stimulus``, and take a variety
of keyword arguments from the message as in the following examples:

.. code-block:: python

   def stimulus_task_finished(self, key=None, worker=None, nbytes=None,
                              type=None, compute_start=None, compute_stop=None,
                              transfer_start=None, transfer_stop=None):

   def stimulus_task_erred(self, key=None, worker=None,
                           exception=None, traceback=None)

These functions change some non-essential administrative state and then call
transition functions.

Note that there are several other non-state-changing messages that we receive
from the workers and clients, such as messages requesting information about the
current state of the scheduler.  These are not considered stimuli.


API
---

.. autoclass:: Scheduler
   :members:

.. autofunction:: decide_worker
