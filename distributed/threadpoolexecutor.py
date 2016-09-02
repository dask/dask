"""
Modified ThreadPoolExecutor to support threads leaving the thread pool

This includes a global `secede` method that a submitted function can call to
have its thread leave the ThreadPoolExecutor's thread pool.  This allows the
thread pool to allocate another thread if necessary and so is useful when a
function realises that it is going to be a long-running job that doesn't want
to take up space.  When the function finishes its thread will terminate
gracefully.

This code copies and modifies two functions from the
`concurrent.futures.thread` module, notably `_worker` and
ThreadPoolExecutor._adjust_thread_count` to allow for checking against a global
`threading.local` state.  These functions are subject to the following license,
which is included as a comment at the end of this file:

    https://docs.python.org/3/license.html

... and are under copyright by the Python Software Foundation

   Copyright 2001-2016 Python Software Foundation; All Rights Reserved
"""
from __future__ import print_function, division, absolute_import

from concurrent.futures import thread

from threading import local, Thread
from .compatibility import get_thread_identity

thread_state = local()

def _worker(executor, work_queue):
    thread_state.proceed = True
    thread_state.executor = executor

    try:
        while thread_state.proceed:
            task = work_queue.get()
            if task is not None:  # sentinel
                task.run()
                del task
            elif thread._shutdown or executor is None or executor._shutdown:
                work_queue.put(None)
                return
        del executor
    except BaseException:
        _base.LOGGER.critical('Exception in worker', exc_info=True)
    finally:
        del thread_state.proceed
        del thread_state.executor


class ThreadPoolExecutor(thread.ThreadPoolExecutor):
    def _adjust_thread_count(self):
        if len(self._threads) < self._max_workers:
            t = Thread(target=_worker, args=(self, self._work_queue))
            t.daemon = True
            self._threads.add(t)
            t.start()


def secede():
    """ Have this thread secede from the ThreadPoolExecutor """
    thread_state.proceed = False
    ident = get_thread_identity()
    for t in thread_state.executor._threads:
        if t.ident == ident:
            thread_state.executor._threads.remove(t)
            break

"""
PSF LICENSE AGREEMENT FOR PYTHON 3.5.2
======================================

1. This LICENSE AGREEMENT is between the Python Software Foundation ("PSF"), and
   the Individual or Organization ("Licensee") accessing and otherwise using Python
   3.5.2 software in source or binary form and its associated documentation.

2. Subject to the terms and conditions of this License Agreement, PSF hereby
   grants Licensee a nonexclusive, royalty-free, world-wide license to reproduce,
   analyze, test, perform and/or display publicly, prepare derivative works,
   distribute, and otherwise use Python 3.5.2 alone or in any derivative
   version, provided, however, that PSF's License Agreement and PSF's notice of
   copyright, i.e., "Copyright c 2001-2016 Python Software Foundation; All Rights
   Reserved" are retained in Python 3.5.2 alone or in any derivative version
   prepared by Licensee.

3. In the event Licensee prepares a derivative work that is based on or
   incorporates Python 3.5.2 or any part thereof, and wants to make the
   derivative work available to others as provided herein, then Licensee hereby
   agrees to include in any such work a brief summary of the changes made to Python
   3.5.2.

4. PSF is making Python 3.5.2 available to Licensee on an "AS IS" basis.
   PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED.  BY WAY OF
   EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND DISCLAIMS ANY REPRESENTATION OR
   WARRANTY OF MERCHANTABILITY OR FITNESS FOR ANY PARTICULAR PURPOSE OR THAT THE
   USE OF PYTHON 3.5.2 WILL NOT INFRINGE ANY THIRD PARTY RIGHTS.

5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON 3.5.2
   FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS A RESULT OF
   MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON 3.5.2, OR ANY DERIVATIVE
   THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.

6. This License Agreement will automatically terminate upon a material breach of
   its terms and conditions.

7. Nothing in this License Agreement shall be deemed to create any relationship
   of agency, partnership, or joint venture between PSF and Licensee.  This License
   Agreement does not grant permission to use PSF trademarks or trade name in a
   trademark sense to endorse or promote products or services of Licensee, or any
   third party.

8. By copying, installing or otherwise using Python 3.5.2, Licensee agrees
   to be bound by the terms and conditions of this License Agreement.
"""
