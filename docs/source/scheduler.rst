Scheduler
=========

The scheduler orchestrates all work across the cluster.  It tracks all active
workers and clients and manages the workers to perform the tasks requested by
the clients.  It tracks the current state of the entire cluster, determining
which tasks execute on which workers in what order.  It is a Tornado TCPServer
consisting of several concurrent coroutines running in a single event loop.

Design
------

The scheduler tracks the state of many tasks among many workers.  It updates
the state of tasks in response to stimuli from workers and from clients.  After
the update of any new information it ensures that the entire system is on track
to complete the desired tasks.

For performance, all updates happen in constant time, regardless of the number
of known tasks.  To achieve constant-time performance the scheduler state is
heavily indexed, with several data structures (around 20) indexing each other
to achieve fast lookup.  All state is a mixture of interwoven dictionaries,
lists, and sets.

This interwoven collection of dictionaries, sets, and lists is difficult to
update consistently without error.  The introduction of small errors and race
conditions leads to infrequent but deadlocking errors that erode confidence in
the scheduler.  To mitigate this complexity we introduce coarser scale task
transitions that are easier to reason about and are themselves heavily tested.
Most code that responds to external stimuli/events is then written as a
sequence of task transitions.

Transitions
-----------

A task is a single Python function call on data intended to be run within a
single worker.  Tasks fall into the following states with the following allowed transitions

.. image:: images/task-state.svg
    :alt: Dask scheduler task states

*  Released: known but not actively computing or in memory
*  Waiting: On track to be computed, waiting on dependencies to arrive in
   memory
*  Queue (ready): Ready to be computed by any worker
*  Stacks (ready): Ready to be computed by a particular preferred worker
*  No-worker (ready, rare): Ready to be computed, but no appropriate worker
   exists
*  Processing: Actively being computed by one or more workers
*  Memory: In memory on one or more workers
*  Erred: Task has computed and erred
*  Removed (not actually a state): Task is no longer needed by any client and
   so it removed from state

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
    enact directly afterwards.  For example after we transition a key into
    memory we may find that many waiting keys are now ready to transition from
    waiting to a ready state.
3.  Optionally they include a set of validation checks that can be turned on
    for testing.

Rather than call these functions directly we call the central function
``transition``:

.. code-block:: python

   def transition(self, key, final_state):
       """ Transition key to the suggested state """

This transition function finds the appropriate path from the current to the
final state.  Italso serves as a central point for logging and diagnostics.

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

.. currentmodule:: distributed.scheduler

.. autoclass:: Scheduler
   :members:
.. autofunction:: decide_worker
