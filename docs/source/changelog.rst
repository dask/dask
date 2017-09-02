Changelog
=========

1.18.2 - September 2nd, 2017
----------------------------
-  Silently pass on cancelled futures in as_completed (:pr:`1366`)
-  Fix unicode keys error in Python 2 (:pr:`1370`)
-  Support numeric worker names
-  Add dask-mpi executable (:pr:`1367`)

1.18.1 - August 25th, 2017
--------------------------
-  Clean up forgotten keys in fire-and-forget workloads (:pr:`1250`)
-  Handle missing extensions (:pr:`1263`)
-  Allow recreate_exception on persisted collections (:pr:`1253`)
-  Add asynchronous= keyword to blocking client methods (:pr:`1272`)
-  Restrict to horizontal panning in bokeh plots (:pr:`1274`)
-  Rename client.shutdown to client.close (:pr:`1275`)
-  Avoid blocking on event loop (:pr:`1270`)
-  Avoid cloudpickle errors for Client.get_versions (:pr:`1279`)
-  Yield on Tornado IOStream.write futures (:pr:`1289`)
-  Assume async behavior if inside a sync statement (:pr:`1284`)
-  Avoid error messages on closing (:pr:`1297`), (:pr:`1296`) (:pr:`1318`)
  (:pr:`1319`)
-  Add timeout= keyword to get_client (:pr:`1290`)
-  Respect timeouts when restarting (:pr:`1304`)
-  Clean file descriptor and memory leaks in tests (:pr:`1317`)
-  Deprecate Executor (:pr:`1302`)
-  Add timeout to ThreadPoolExecutor.shutdown (:pr:`1330`)
-  Clean up AsyncProcess handling (:pr:`1324`)
-  Allow unicode keys in Python 2 scheduler (:pr:`1328`)
-  Avoid leaking stolen data (:pr:`1326`)
-  Improve error handling on failed nanny starts (:pr:`1337`), (:pr:`1331`)
-  Make Adaptive more flexible
-  Support ``--contact-address`` and ``--listen-address`` in worker (:pr:`1278`)
-  Remove old dworker, dscheduler executables (:pr:`1355`)
-  Exit workers if nanny process fails (:pr:`1345`)
-  Auto pep8 and flake (:pr:`1353`)

1.18.0 - July 8th, 2017
-----------------------
-  Multi-threading safety (:pr:`1191`), (:pr:`1228`), (:pr:`1229`)
-  Improve handling of byte counting (:pr:`1198`) (:pr:`1224`)
-  Add get_client, secede functions, refactor worker-client relationship
  (:pr:`1201`)
-  Allow logging configuraiton using logging.dictConfig() (:pr:`1206`) (:pr:`1211`)
-  Offload serialization and deserialization to separate thread (:pr:`1218`)
-  Support fire-and-forget tasks (:pr:`1221`)
-  Support bytestrings as keys (for Julia) (:pr:`1234`)
-  Resolve testing corner-cases (:pr:`1236`), (:pr:`1237`), (:pr:`1240`), (:pr:`1241`), (:pr:`1242`), (:pr:`1244`)
-  Automatic use of scatter/gather(direct=True) in more cases (:pr:`1239`)

1.17.1 - June 14th, 2017
------------------------

-  Remove Python 3.4 testing from travis-ci (:pr:`1157`)
-  Remove ZMQ Support (:pr:`1160`)
-  Fix memoryview nbytes issue in Python 2.7 (:pr:`1165`)
-  Re-enable counters (:pr:`1168`)
-  Improve scheduler.restart (:pr:`1175`)


1.17.0 - June 9th, 2017
-----------------------

-  Reevaluate worker occupancy periodically during scheduler downtime
   (:pr:`1038`) (:pr:`1101`)
-  Add ``AioClient`` asyncio-compatible client API (:pr:`1029`) (:pr:`1092`)
   (:pr:`1099`)
-  Update Keras serializer (:pr:`1067`)
-  Support TLS/SSL connections for security (:pr:`866`) (:pr:`1034`)
-  Always create new worker directory when passed ``--local-directory``
   (:pr:`1079`)
-  Support pre-scattering data when using joblib frontent (:pr:`1022`)
-  Make workers more robust to failure of ``sizeof`` function (:pr:`1108`) and
   writing to disk (:pr:`1096`)
-  Add ``is_empty`` and ``update`` methods to ``as_completed`` (:pr:`1113`)
-  Remove ``_get`` coroutine and replace with ``get(..., sync=False)``
   (:pr:`1109`)
-  Improve API compatibility with async/await syntax (:pr:`1115`) (:pr:`1124`)
-  Add distributed Queues (:pr:`1117`) and shared Variables (:pr:`1128`) to
   enable inter-client coordination
-  Support direct client-to-worker scattering and gathering (:pr:`1130`) as
   well as performance enhancements when scattering data
-  Style improvements for bokeh web dashboards (:pr:`1126`) (:pr:`1141`) as
   well as a removal of the external bokeh process
-  HTML reprs for Future and Client objects (:pr:`1136`)
-  Support nested collections in client.compute (:pr:`1144`)
-  Use normal client API in asynchronous mode (:pr:`1152`)
-  Remove old distributed.collections submodule (:pr:`1153`)


1.16.3 - May 5th, 2017
----------------------

-  Add bokeh template files to MANIFEST (:pr:`1063`)
-  Don't set worker_client.get as default get (:pr:`1061`)
-  Clean up logging on Client().shutdown() (:pr:`1055`)

1.16.2 - May 3rd, 2017
----------------------

-  Support ``async with Client`` syntax (:pr:`1053`)
-  Use internal bokeh server for default diagnostics server (:pr:`1047`)
-  Improve styling of bokeh plots when empty (:pr:`1046`) (:pr:`1037`)
-  Support efficient serialization for sparse arrays (:pr:`1040`)
-  Prioritize newly arrived work in worker (:pr:`1035`)
-  Prescatter data with joblib backend (:pr:`1022`)
-  Make client.restart more robust to worker failure (:pr:`1018`)
-  Support preloading a module or script in dask-worker or dask-scheduler processes (:pr:`1016`)
-  Specify network interface in command line interface (:pr:`1007`)
-  Client.scatter supports a single element (:pr:`1003`)
-  Use blosc compression on all memoryviews passing through comms (:pr:`998`)
-  Add concurrent.futures-compatible Executor (:pr:`997`)
-  Add as_completed.batches method and return results (:pr:`994`) (:pr:`971`)
-  Allow worker_clients to optionally stay within the thread pool (:pr:`993`)
-  Add bytes-stored and tasks-processing diagnostic histograms (:pr:`990`)
-  Run supports non-msgpack-serializable results (:pr:`965`)


1.16.1 - March 22nd, 2017
-------------------------

-  Use inproc transport in LocalCluster (:pr:`919`)
-  Add structured and queryable cluster event logs (:pr:`922`)
-  Use connection pool for inter-worker communication (:pr:`935`)
-  Robustly shut down spawned worker processes at shutdown (:pr:`928`)
-  Worker death timeout (:pr:`940`)
-  More visual reporting of exceptions in progressbar (:pr:`941`)
-  Render disk and serialization events to task stream visual (:pr:`943`)
-  Support async for / await protocol (:pr:`952`)
-  Ensure random generators are re-seeded in worker processes (:pr:`953`)
-  Upload sourcecode as zip module (:pr:`886`)
-  Replay remote exceptions in local process (:pr:`894`)

1.16.0 - February 24th, 2017
----------------------------

- First come first served priorities on client submissions (:pr:`840`)
- Can specify Bokeh internal ports (:pr:`850`)
- Allow stolen tasks to return from either worker (:pr:`853`), (:pr:`875`)
- Add worker resource constraints during execution (:pr:`857`)
- Send small data through Channels (:pr:`858`)
- Better estimates for SciPy sparse matrix memory costs (:pr:`863`)
- Avoid stealing long running tasks (:pr:`873`)
- Maintain fortran ordering of NumPy arrays (:pr:`876`)
- Add ``--scheduler-file`` keyword to dask-scheduler (:pr:`877`)
- Add serializer for Keras models (:pr:`878`)
- Support uploading modules from zip files (:pr:`886`)
- Improve titles of Bokeh dashboards (:pr:`895`)

1.15.2 - January 27th, 2017
---------------------------

*  Fix a bug where arrays with large dtypes or shapes were being improperly compressed (:pr:`830` :pr:`832` :pr:`833`)
*  Extend ``as_completed`` to accept new futures during iteration (:pr:`829`)
*  Add ``--nohost`` keyword to ``dask-ssh`` startup utility (:pr:`827`)
*  Support scheduler shutdown of remote workers, useful for adaptive clusters (:pr: `811` :pr:`816` :pr:`821`)
*  Add ``Client.run_on_scheduler`` method for running debug functions on the scheduler (:pr:`808`)

1.15.1 - January 11th, 2017
---------------------------

*  Make compatibile with Bokeh 0.12.4 (:pr:`803`)
*  Avoid compressing arrays if not helpful  (:pr:`777`)
*  Optimize inter-worker data transfer (:pr:`770`) (:pr:`790`)
*  Add --local-directory keyword to worker (:pr:`788`)
*  Enable workers to arrive to the cluster with their own data.
   Useful if a worker leaves and comes back (:pr:`785`)
*  Resolve thread safety bug when using local_client (:pr:`802`)
*  Resolve scheduling issues in worker (:pr:`804`)


1.15.0 - January 2nd, 2017
--------------------------

*  Major Worker refactor (:pr:`704`)
*  Major Scheduler refactor (:pr:`717`) (:pr:`722`) (:pr:`724`) (:pr:`742`) (:pr:`743`
*  Add ``check`` (default is ``False``) option to ``Client.get_versions``
   to raise if the versions don't match on client, scheduler & workers (:pr:`664`)
*  ``Future.add_done_callback`` executes in separate thread (:pr:`656`)
*  Clean up numpy serialization (:pr:`670`)
*  Support serialization of Tornado v4.5 coroutines (:pr:`673`)
*  Use CPickle instead of Pickle in Python 2 (:pr:`684`)
*  Use Forkserver rather than Fork on Unix in Python 3 (:pr:`687`)
*  Support abstract resources for per-task constraints (:pr:`694`) (:pr:`720`) (:pr:`737`)
*  Add TCP timeouts (:pr:`697`)
*  Add embedded Bokeh server to workers (:pr:`709`) (:pr:`713`) (:pr:`738`)
*  Add embedded Bokeh server to scheduler (:pr:`724`) (:pr:`736`) (:pr:`738`)
*  Add more precise timers for Windows (:pr:`713`)
*  Add Versioneer (:pr:`715`)
*  Support inter-client channels  (:pr:`729`) (:pr:`749`)
*  Scheduler Performance improvements (:pr:`740`) (:pr:`760`)
*  Improve load balancing and work stealing (:pr:`747`) (:pr:`754`) (:pr:`757`)
*  Run Tornado coroutines on workers
*  Avoid slow sizeof call on Pandas dataframes (:pr:`758`)


1.14.3 - November 13th, 2016
----------------------------

*  Remove custom Bokeh export tool that implicitly relied on nodejs (:pr:`655`)
*  Clean up scheduler logging (:pr:`657`)


1.14.2 - November 11th, 2016
----------------------------

*  Support more numpy dtypes in custom serialization, (:pr:`627`), (:pr:`630`), (:pr:`636`)
*  Update Bokeh plots (:pr:`628`)
*  Improve spill to disk heuristics (:pr:`633`)
*  Add Export tool to Task Stream plot
*  Reverse frame order in loads for very many frames (:pr:`651`)
*  Add timeout when waiting on write (:pr:`653`)


1.14.0 - November 3rd, 2016
---------------------------

*   Add ``Client.get_versions()`` function to return software and package
    information from the scheduler, workers, and client (:pr:`595`)
*   Improved windows support (:pr:`577`) (:pr:`590`) (:pr:`583`) (:pr:`597`)
*   Clean up rpc objects explicitly (:pr:`584`)
*   Normalize collections against known futures (:pr:`587`)
*   Add key= keyword to map to specify keynames (:pr:`589`)
*   Custom data serialization (:pr:`606`)
*   Refactor the web interface (:pr:`608`) (:pr:`615`) (:pr:`621`)
*   Allow user-supplied Executor in Worker (:pr:`609`)
*   Pass Worker kwargs through LocalCluster


1.13.3 - October 15th, 2016
---------------------------

*   Schedulers can retire workers cleanly
*   Add ``Future.add_done_callback`` for ``concurrent.futures`` compatibility
*   Update web interface to be consistent with Bokeh 0.12.3
*   Close streams explicitly, avoiding race conditions and supporting
    more robust restarts on Windows.
*   Improved shuffled performance for dask.dataframe
*   Add adaptive allocation cluster manager
*   Reduce administrative overhead when dealing with many workers
*   ``dask-ssh --log-directory .`` no longer errors
*   Microperformance tuning for the scheduler

1.13.2
------

*   Revert dask_worker to use fork rather than subprocess by default
*   Scatter retains type information
*   Bokeh always uses subprocess rather than spawn

1.13.1
------

*   Fix critical Windows error with dask_worker executable

1.13.0
------

*   Rename Executor to Client (:pr:`492`)
*   Add ``--memory-limit`` option to ``dask-worker``, enabling spill-to-disk
    behavior when running out of memory (:pr:`485`)
*   Add ``--pid-file`` option to dask-worker and ``--dask-scheduler`` (:pr:`496`)
*   Add ``upload_environment`` function to distribute conda environments.
    This is experimental, undocumented, and may change without notice.  (:pr:`494`)
*   Add ``workers=`` keyword argument to ``Client.compute`` and ``Client.persist``,
    supporting location-restricted workloads with Dask collections (:pr:`484`)
*   Add ``upload_environment`` function to distribute conda environments.
    This is experimental, undocumented, and may change without notice.  (:pr:`494`)

    *   Add optional ``dask_worker=`` keyword to ``client.run`` functions that gets
        provided the worker or nanny object
    *   Add ``nanny=False`` keyword to ``Client.run``, allowing for the execution
        of arbitrary functions on the nannies as well as normal workers


1.12.2
------

This release adds some new features and removes dead code

*   Publish and share datasets on the scheduler between many clients (:pr:`453`).
    See :doc:`publish`.
*   Launch tasks from other tasks (experimental) (:pr:`471`). See :doc:`task-launch`.
*   Remove unused code, notably the ``Center`` object and older client functions (:pr:`478`)
*   ``Executor()`` and ``LocalCluster()`` is now robust to Bokeh's absence (:pr:`481`)
*   Removed s3fs and boto3 from requirements.  These have moved to Dask.

1.12.1
------

This release is largely a bugfix release, recovering from the previous large
refactor.

*  Fixes from previous refactor
    *  Ensure idempotence across clients
    *  Stress test losing scattered data permanently
*  IPython fixes
    *  Add ``start_ipython_scheduler`` method to Executor
    *  Add ``%remote`` magic for workers
    *  Clean up code and tests
*  Pool connects to maintain reuse and reduce number of open file handles
*  Re-implement work stealing algorithm
*  Support cancellation of tuple keys, such as occur in dask.arrays
*  Start synchronizing against worker data that may be superfluous
*  Improve bokeh plots styling
    *  Add memory plot tracking number of bytes
    *  Make the progress bars more compact and align colors
    *  Add workers/ page with workers table, stacks/processing plot, and memory
*  Add this release notes document


1.12.0
------

This release was largely a refactoring release.  Internals were changed
significantly without many new features.

*  Major refactor of the scheduler to use transitions system
*  Tweak protocol to traverse down complex messages in search of large
   bytestrings
*  Add dask-submit and dask-remote
*  Refactor HDFS writing to align with changes in the dask library
*  Executor reconnects to scheduler on broken connection or failed scheduler
*  Support sklearn.external.joblib as well as normal joblib
