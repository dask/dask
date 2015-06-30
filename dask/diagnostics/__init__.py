from .profile import Profiler

from dask import threaded, multiprocessing

thread_prof = Profiler(threaded.get)
process_prof = Profiler(multiprocessing.get)
