Changelog
=========

1.28.1 - 2019-05-13
-------------------

This is a small bugfix release due to a config change upstream.

-  Use config accessor method for "scheduler-address" (#2676) `James Bourbeau`_


1.28.0 - 2019-05-08
-------------------

- Add Type Attribute to TaskState (:pr:`2657`) `Matthew Rocklin`_
- Add waiting task count to progress title bar (:pr:`2663`) `James Bourbeau`_
- DOC: Clean up reference to cluster object (:pr:`2664`) `K.-Michael Aye`_
- Allow scheduler to politely close workers as part of shutdown (:pr:`2651`) `Matthew Rocklin`_
- Check direct_to_workers before using get_worker in Client (:pr:`2656`) `Matthew Rocklin`_
- Fixed comment regarding keeping existing level if less verbose (:pr:`2655`) `Brett Randall`_
- Add idle timeout to scheduler (:pr:`2652`) `Matthew Rocklin`_
- Avoid deprecation warnings (:pr:`2653`) `Matthew Rocklin`_
- Use an LRU cache for deserialized functions (:pr:`2623`) `Matthew Rocklin`_
- Rename Worker._close to Worker.close (:pr:`2650`) `Matthew Rocklin`_
- Add Comm closed bookkeeping (:pr:`2648`) `Matthew Rocklin`_
- Explain LocalCluster behavior in Client docstring (:pr:`2647`) `Matthew Rocklin`_
- Add last worker into KilledWorker exception to help debug (:pr:`2610`) `@plbertrand`_
- Set working worker class for dask-ssh (:pr:`2646`) `Martin Durant`_
- Add as_completed methods to docs (:pr:`2642`) `Jim Crist`_
- Add timeout to Client._reconnect (:pr:`2639`) `Jim Crist`_
- Limit test_spill_by_default memory, reenable it (:pr:`2633`) `Peter Andreas Entschev`_
- Use proper address in worker -> nanny comms (:pr:`2640`) `Jim Crist`_
- Fix deserialization of bytes chunks larger than 64MB (:pr:`2637`) `Peter Andreas Entschev`_


1.27.1 - 2019-04-29
-------------------

-  Adaptive: recommend close workers when any are idle (:pr:`2330`) `Michael Delgado`_
-  Increase GC thresholds (:pr:`2624`) `Matthew Rocklin`_
-  Add interface= keyword to LocalCluster (:pr:`2629`) `Matthew Rocklin`_
-  Add worker_class argument to LocalCluster (:pr:`2625`) `Matthew Rocklin`_
-  Remove Python 2.7 from testing matrix (:pr:`2631`) `Matthew Rocklin`_
-  Add number of trials to diskutils test (:pr:`2630`) `Matthew Rocklin`_
-  Fix parameter name in LocalCluster docstring (:pr:`2626`) `Loïc Estève`_
-  Integrate stacktrace for low-level profiling (:pr:`2575`) `Peter Andreas Entschev`_
-  Apply Black to standardize code styling (:pr:`2614`) `Matthew Rocklin`_
-  added missing whitespace to start_worker cmd (:pr:`2613`) `condoratberlin`_
-  Updated logging module doc links from docs.python.org/2 to docs.python.org/3. (:pr:`2635`) `Brett Randall`_


1.27.0 - 2019-04-12
-------------------

-  Add basic health endpoints to scheduler and worker bokeh. (:pr:`2607`) `amerkel2`_
-  Improved description accuracy of --memory-limit option. (:pr:`2601`) `Brett Randall`_
-  Check self.dependencies when looking at dependent tasks in memory (:pr:`2606`) `deepthirajagopalan7`_
-  Add RabbitMQ SchedulerPlugin example (:pr:`2604`) `Matt Nicolls`_
-  add resources to scheduler update_graph plugin (:pr:`2603`) `Matt Nicolls`_
-  Use ensure_bytes in serialize_error (:pr:`2588`) `Matthew Rocklin`_
-  Specify data storage explicitly from Worker constructor (:pr:`2600`) `Matthew Rocklin`_
-  Change bokeh port keywords to dashboard_address (:pr:`2589`) `Matthew Rocklin`_
-  .detach_() pytorch tensor to serialize data as numpy array. (:pr:`2586`) `Muammar El Khatib`_
-  Add warning if creating scratch directories takes a long time (:pr:`2561`) `Matthew Rocklin`_
-  Fix typo in pub-sub doc. (:pr:`2599`) `Loïc Estève`_
-  Allow return_when='FIRST_COMPLETED' in wait (:pr:`2598`) `Nikos Tsaousis`_
-  Forward kwargs through Nanny to Worker (:pr:`2596`) `Brian Chu`_
-  Use ensure_dict instead of dict (:pr:`2594`) `James Bourbeau`_
-  Specify protocol in LocalCluster (:pr:`2489`) `Matthew Rocklin`_

1.26.1 - 2019-03-29
-------------------

-  Fix LocalCluster to not overallocate memory when overcommitting threads per worker (:pr:`2541`) `George Sakkis`_
-  Make closing resilient to lacking an address (:pr:`2542`) `Matthew Rocklin`_
-  fix typo in comment (:pr:`2546`) `Brett Jurman`_
-  Fix double init of prometheus metrics (:pr:`2544`) `Marco Neumann`_
-  Skip test_duplicate_clients without bokeh. (:pr:`2553`) `Elliott Sales de Andrade`_
-  Add blocked_handlers to servers (:pr:`2556`) `Chris White`_
-  Always yield Server.handle_comm coroutine (:pr:`2559`) `Tom Augspurger`_
-  Use yaml.safe_load (:pr:`2566`) `Matthew Rocklin`_
-  Fetch executables from build root. (:pr:`2551`) `Elliott Sales de Andrade`_
-  Fix Torando 6 test failures (:pr:`2570`) `Matthew Rocklin`_
-  Fix test_sync_closed_loop (:pr:`2572`) `Matthew Rocklin`_

1.26.0 - 2019-02-25
-------------------

-  Update style to fix recent flake8 update (:pr:`2500`) (:pr:`2509`) `Matthew Rocklin`_
-  Fix typo in gen_cluster log message (:pr:`2503`) `Loïc Estève`_
-  Allow KeyError when closing event loop (:pr:`2498`) `Matthew Rocklin`_
-  Avoid thread testing for TCP ThreadPoolExecutor (:pr:`2510`) `Matthew Rocklin`_
-  Find Futures inside SubgraphCallable (:pr:`2505`) `Jim Crist`_
-  Avoid AttributeError when closing and sending a message (:pr:`2514`) `Matthew Rocklin`_
-  Add deprecation warning to dask_mpi.py (:pr:`2522`) `Julia Kent`_
-  Relax statistical profiling test (:pr:`2527`) `Matthew Rocklin`_
-  Support alternative --remote-dask-worker SSHCluster() and dask-ssh CLI (:pr:`2526`) `Adam Beberg`_
-  Iterate over full list of plugins in transition (:pr:`2518`) `Matthew Rocklin`_
-  Create Prometheus Endpoint (:pr:`2499`) `Adam Beberg`_
-  Use pytest.importorskip for prometheus test (:pr:`2533`) `Matthew Rocklin`_
-  MAINT skip prometheus test when no installed (:pr:`2534`) `Olivier Grisel`_
-  Fix intermittent testing failures (:pr:`2535`) `Matthew Rocklin`_
-  Avoid using nprocs keyword in dask-ssh if set to one (:pr:`2531`)  `Matthew Rocklin`_
-  Bump minimum Tornado version to 5.0


1.25.3 - 2019-01-31
-------------------

-  Fix excess threading on missing connections (:pr:`2403`) `Daniel Farrell`_
-  Fix typo in doc (:pr:`2457`) `Loïc Estève`_
-  Start fewer but larger workers with LocalCluster (:pr:`2452`) `Matthew Rocklin`_
-  Check for non-zero ``length`` first in ``read`` loop (:pr:`2465`) `John Kirkham`_
-  DOC: Use of local cluster in script (:pr:`2462`) `Peter Killick`_
-  DOC/API: Signature for base class write / read (:pr:`2472`) `Tom Augspurger`_
-  Support Pytest 4 in Tests (:pr:`2478`) `Adam Beberg`_
-  Ensure async behavior in event loop with LocalCluster (:pr:`2484`) `Matthew Rocklin`_
-  Fix spurious CancelledError (:pr:`2485`) `Loïc Estève`_
-  Properly reset dask.config scheduler and shuffle when closing the client (:pr:`2475`) `George Sakkis`_
-  Make it more explict that resources are per worker. (:pr:`2470`) `Loïc Estève`_
-  Remove references to center (:pr:`2488`)  `Matthew Rocklin`_
-  Expand client clearing timeout to 10s in testing (:pr:`2493`) `Matthew Rocklin`_
-  Propagate key keyword in progressbar (:pr:`2492`) `Matthew Rocklin`_
-  Use provided cluster's IOLoop if present in Client (:pr:`2494`) `Matthew Rocklin`_


1.25.2 - 2019-01-04
-------------------

-  Clean up LocalCluster logging better in async mode (:pr:`2448`) `Matthew Rocklin`_
-  Add short error message if bokeh cannot be imported (:pr:`2444`) `Dirk Petersen`_
-  Add optional environment variables to Nanny (:pr:`2431`) `Matthew Rocklin`_
-  Make the direct keyword docstring entries uniform (:pr:`2441`) `Matthew Rocklin`_
-  Make LocalCluster.close async friendly (:pr:`2437`) `Matthew Rocklin`_
-  gather_dep: don't request dependencies we already found out we don't want (:pr:`2428`) `tjb900`_
-  Add parameters to Client.run docstring (:pr:`2429`) `Matthew Rocklin`_
-  Support coroutines and async-def functions in run/run_scheduler (:pr:`2427`) `Matthew Rocklin`_
-  Name threads in ThreadPoolExecutors (:pr:`2408`) `Matthew Rocklin`_



1.25.1 - 2018-12-15
-------------------

-  Serialize numpy.ma.masked objects properly (:pr:`2384`) `Jim Crist`_
-  Turn off bokeh property validation in dashboard (:pr:`2387`) `Jim Crist`_
-  Fully initialize WorkerState objects (:pr:`2388`) `Jim Crist`_
-  Fix typo in scheduler docstring (:pr:`2393`) `Russ Bubley`_
-  DOC: fix typo in distributed.worker.Worker docstring (:pr:`2395`) `Loïc Estève`_
-  Remove clients and workers from event log after removal (:pr:`2394`) `tjb900`_
-  Support msgpack 0.6.0 by providing length keywords (:pr:`2399`) `tjb900`_
-  Use async-await on large messages test (:pr:`2404`) `Matthew Rocklin`_
-  Fix race condition in normalize_collection (:pr:`2386`) `Jim Crist`_
-  Fix redict collection after HighLevelGraph fix upstream (:pr:`2413`) `Matthew Rocklin`_
-  Add a blocking argument to Lock.acquire() (:pr:`2412`) `Stephan Hoyer`_
-  Fix long traceback test (:pr:`2417`) `Matthew Rocklin`_
-  Update x509 certificates to current OpenSSL standards. (:pr:`2418`) `Diane Trout`_


1.25.0 - 2018-11-28
-------------------

-  Fixed the 404 error on the Scheduler Dashboard homepage (:pr:`2361`) `Michael Wheeler`_
-  Consolidate two Worker classes into one (:pr:`2363`) `Matthew Rocklin`_
-  Avoid warnings in pyarrow and msgpack (:pr:`2364`) `Matthew Rocklin`_
-  Avoid race condition in Actor's Future (:pr:`2374`) `Matthew Rocklin`_
-  Support missing packages keyword in Client.get_versions (:pr:`2379`) `Matthew Rocklin`_
-  Fixup serializing masked arrays (:pr:`2373`) `Jim Crist`_


1.24.2 - 2018-11-15
-------------------

-  Add support for Bokeh 1.0 (:pr:`2348`) (:pr:`2356`) `Matthew Rocklin`_
-  Fix regression that dropped support for Tornado 4 (:pr:`2353`) `Roy Wedge`_
-  Avoid deprecation warnings (:pr:`2355`) (:pr:`2357`) `Matthew Rocklin`_
-  Fix typo in worker documentation (:pr:`2349`) `Tom Rochette`_


1.24.1 - 2018-11-09
-------------------

-  Use tornado's builtin AnyThreadLoopEventPolicy (:pr:`2326`) `Matthew Rocklin`_
-  Adjust TLS tests for openssl 1.1 (:pr:`2331`) `Marius van Niekerk`_
-  Avoid setting event loop policy if within Jupyter notebook server (:pr:`2343`) `Matthew Rocklin`_
-  Add preload script to conf (:pr:`2325`) `Guillaume Eynard-Bontemps`_
-  Add serializer for Numpy masked arrays (:pr:`2335`) `Peter Killick`_
-  Use psutil.Process.oneshot (:pr:`2339`) `NotSqrt`_
-  Use worker SSL context when getting client from worker. (:pr:`2301`) Anonymous


1.24.0 - 2018-10-26
-------------------

-  Remove Joblib Dask Backend from codebase (:pr:`2298`) `Matthew Rocklin`_
-  Include worker tls protocol in Scheduler.restart (:pr:`2295`) `Matthew Rocklin`_
-  Adapt to new Bokeh selection for 1.0 (:pr:`2292`) `Matthew Rocklin`_
-  Add explicit retry method to Future and Client (:pr:`2299`) `Matthew Rocklin`_
-  Point to main worker page in bokeh links (:pr:`2300`) `Matthew Rocklin`_
-  Limit concurrency when gathering many times (:pr:`2303`) `Matthew Rocklin`_
-  Add tls_cluster pytest fixture (:pr:`2302`) `Matthew Rocklin`_
-  Convert ConnectionPool.open and active to properties (:pr:`2304`) `Matthew Rocklin`_
-  change export_tb to format_tb (:pr:`2306`) `Eric Ma`_
-  Redirect joblib page to dask-ml (:pr:`2307`) `Matthew Rocklin`_
-  Include unserializable object in error message (:pr:`2310`) `Matthew Rocklin`_
-  Import Mapping, Iterator, Set from collections.abc in Python 3 (:pr:`2315`) `Gaurav Sheni`_
-  Extend Client.scatter docstring (:pr:`2320`) `Eric Ma`_
-  Update for new flake8 (:pr:`2321`)  `Matthew Rocklin`_


1.23.3 - 2018-10-05
-------------------

-  Err in dask serialization if not a NotImplementedError (:pr:`2251`) `Matthew Rocklin`_
-  Protect against key missing from priority in GraphLayout (:pr:`2259`) `Matthew Rocklin`_
-  Do not pull data twice in Client.gather (:pr:`2263`) `Adam Klein`_
-  Add pytest fixture for cluster tests (:pr:`2262`) `Matthew Rocklin`_
-  Cleanup bokeh callbacks  (:pr:`2261`) (:pr:`2278`) `Matthew Rocklin`_
-  Fix bokeh error for `memory_limit=None` (:pr:`2255`) `Brett Naul`_
-  Place large keywords into task graph in Client.map (:pr:`2281`) `Matthew Rocklin`_
-  Remove redundant blosc threading code from protocol.numpy (:pr:`2284`) `Mike Gevaert`_
-  Add ncores to workertable (:pr:`2289`) `Matthew Rocklin`_
-  Support upload_file on files with no extension (:pr:`2290`) `Matthew Rocklin`_


1.23.2 - 2018-09-17
-------------------

-  Discard dependent rather than remove (:pr:`2250`) `Matthew Rocklin`_
-  Use dask_sphinx_theme `Matthew Rocklin`_
-  Drop the Bokeh index page (:pr:`2241`) `John Kirkham`_
-  Revert change to keep link relative (:pr:`2242`) `Matthew Rocklin`_
-  docs: Fix broken AWS link in setup.rst file (:pr:`2240`) `Vladyslav Moisieienkov`_
-  Return cancelled futures in as_completed (:pr:`2233`) `Chris White`_


1.23.1 - 2018-09-06
-------------------

-  Raise informative error when mixing futures between clients (:pr:`2227`) `Matthew Rocklin`_
-  add byte_keys to unpack_remotedata call (:pr:`2232`) `Matthew Rocklin`_
-  Add documentation for gist/rawgit for get_task_stream (:pr:`2236`) `Matthew Rocklin`_
-  Quiet Client.close by waiting for scheduler stop signal (:pr:`2237`) `Matthew Rocklin`_
-  Display system graphs nicely on different screen sizes (:pr:`2239`) `Derek Ludwig`_
-  Mutate passed in workers dict in TaskStreamPlugin.rectangles (:pr:`2238`) `Matthew Rocklin`_


1.23.0 - 2018-08-30
-------------------

-  Add direct_to_workers to Client `Matthew Rocklin`_
-  Add Scheduler.proxy to workers `Matthew Rocklin`_
-  Implement Actors `Matthew Rocklin`_
-  Fix tooltip (:pr:`2168`) `Loïc Estève`_
-  Fix scale /  avoid returning coroutines (:pr:`2171`) `Joe Hamman`_
-  Clarify dask-worker --nprocs (:pr:`2173`) `Yu Feng`_
-  Concatenate all bytes of small messages in TCP comms (:pr:`2172`) `Matthew Rocklin`_
-  Add dashboard_link property (:pr:`2176`) `Jacob Tomlinson`_
-  Always offload to_frames (:pr:`2170`) `Matthew Rocklin`_
-  Warn if desired port is already in use (:pr:`2191`) (:pr:`2199`) `Matthew Rocklin`_
-  Add profile page for event loop thread (:pr:`2144`) `Matthew Rocklin`_
-  Use dispatch for dask serialization, also add sklearn, pytorch (:pr:`2175`) `Matthew Rocklin`_
-  Handle corner cases with busy signal (:pr:`2182`) `Matthew Rocklin`_
-  Check self.dependencies when looking at tasks in memory (:pr:`2196`) `Matthew Rocklin`_
-  Add ability to log additional custom metrics from each worker (:pr:`2169`) `Loïc Estève`_
-  Fix formatting when port is a tuple (:pr:`2204`) `Loïc Estève`_
-  Describe what ZeroMQ is (:pr:`2211`) `Mike DePalatis`_
-  Tiny typo fix (:pr:`2214`) `Anderson Banihirwe`_
-  Add Python 3.7 to travis.yml (:pr:`2203`) `Matthew Rocklin`_
-  Add plot= keyword to get_task_stream (:pr:`2198`) `Matthew Rocklin`_
-  Add support for optional versions in Client.get_versions (:pr:`2216`) `Matthew Rocklin`_
-  Add routes for solo bokeh figures in dashboard (:pr:`2185`) `Matthew Rocklin`_
-  Be resilient to missing dep after busy signal (:pr:`2217`) `Matthew Rocklin`_
-  Use CSS Grid to layout status page on the dashboard (:pr:`2213`) `Derek Ludwig`_ and `Luke Canavan`_
-  Fix deserialization of queues on main ioloop thread (:pr:`2221`) `Matthew Rocklin`_
-  Add a worker initialization function (:pr:`2201`) `Guillaume Eynard-Bontemps`_
-  Collapse navbar in dashboard (:pr:`2223`) `Luke Canavan`_


1.22.1 - 2018-08-03
-------------------

-  Add worker_class= keyword to Nanny to support different worker types (:pr:`2147`) `Martin Durant`_
-  Cleanup intermittent worker failures (:pr:`2152`) (:pr:`2146`) `Matthew Rocklin`_
-  Fix msgpack PendingDeprecationWarning for encoding='utf-8' (:pr:`2153`) `Olivier Grisel`_
-  Make bokeh coloring deterministic using hash function (:pr:`2143`) `Matthew Rocklin`_
-  Allow client to query the task stream plot (:pr:`2122`) `Matthew Rocklin`_
-  Use PID and counter in thread names (:pr:`2084`) (:pr:`2128`) `Dror Birkman`_
-  Test that worker restrictions are cleared after cancellation (:pr:`2107`) `Matthew Rocklin`_
-  Expand resources in graph_to_futures (:pr:`2131`) `Matthew Rocklin`_
-  Add custom serialization support for pyarrow  (:pr:`2115`) `Dave Hirschfeld`_
-  Update dask-scheduler cli help text for preload (:pr:`2120`) `Matt Nicolls`_
-  Added another nested parallelism test (:pr:`1710`) `Tom Augspurger`_
-  insert newline by default after TextProgressBar (:pr:`1976`) `Phil Tooley`_
-  Retire workers from scale (:pr:`2104`) `Matthew Rocklin`_
-  Allow worker to refuse data requests with busy signal (:pr:`2092`) `Matthew Rocklin`_
-  Don't forget released keys (:pr:`2098`) `Matthew Rocklin`_
-  Update example for stopping a worker (:pr:`2088`) `John Kirkham`_
-  removed hardcoded value of memory terminate fraction from a log message (:pr:`2096`) `Bartosz Marcinkowski`_
-  Adjust worker doc after change in config file location and treatment (:pr:`2094`) `Aurélien Ponte`_
-  Prefer gathering data from same host (:pr:`2090`) `Matthew Rocklin`_
-  Handle exceptions on deserialized comm with text error (:pr:`2093`) `Matthew Rocklin`_
-  Fix typo in docstring (:pr:`2087`) `Loïc Estève`_
-  Provide communication context to serialization functions (:pr:`2054`) `Matthew Rocklin`_
-  Allow `name` to be explicitly passed in publish_dataset (:pr:`1995`) `Marius van Niekerk`_
-  Avoid accessing Worker.scheduler_delay around yield point (:pr:`2074`) `Matthew Rocklin`_
-  Support TB and PB in format bytes (:pr:`2072`) `Matthew Rocklin`_
-  Add test for as_completed for loops in Python 2 (:pr:`2071`) `Matthew Rocklin`_
-  Allow adaptive to exist without a cluster (:pr:`2064`) `Matthew Rocklin`_
-  Have worker data transfer wait until recipient acknowledges (:pr:`2052`) `Matthew Rocklin`_
-  Support async def functions in Client.sync (:pr:`2070`) `Matthew Rocklin`_
-  Add asynchronous parameter to docstring of LocalCluster `Matthew Rocklin`_
-  Normalize address before comparison (:pr:`2066`) `Tom Augspurger`_
-  Use ConnectionPool for Worker.scheduler `Matthew Rocklin`_
-  Avoid reference cycle in str_graph `Matthew Rocklin`_
-  Pull data outside of while loop in gather (:pr:`2059`) `Matthew Rocklin`_


1.22.0 - 2018-06-14
-------------------

-  Overhaul configuration (:pr:`1948`) `Matthew Rocklin`_
-  Replace get= keyword with scheduler= (:pr:`1959`) `Matthew Rocklin`_
-  Use tuples in msgpack (:pr:`2000`) `Matthew Rocklin`_ and `Marius van Niekerk`_
-  Unify handling of high-volume connections (:pr:`1970`) `Matthew Rocklin`_
-  Automatically scatter large arguments in joblib connector (:pr:`2020`) (:pr:`2030`) `Olivier Grisel`_
-  Turn click Python 3 locales failure into a warning (:pr:`2001`) `Matthew Rocklin`_
-  Rely on dask implementation of sizeof (:pr:`2042`) `Matthew Rocklin`_
-  Replace deprecated workers.iloc with workers.values() (:pr:`2013`) `Grant Jenks`_
-  Introduce serialization families (:pr:`1912`) `Matthew Rocklin`_

-  Add PubSub (:pr:`1999`) `Matthew Rocklin`_
-  Add Dask stylesheet to documentation `Matthew Rocklin`_
-  Avoid recomputation on partially-complete results (:pr:`1840`) `Matthew Rocklin`_
-  Use sys.prefix in popen for testing (:pr:`1954`) `Matthew Rocklin`_
-  Include yaml files in manifest `Matthew Rocklin`_
-  Use self.sync so Client.processing works in asynchronous context (:pr:`1962`) `Henry Doupe`_
-  Fix bug with bad repr on closed client (:pr:`1965`) `Matthew Rocklin`_
-  Parse --death-timeout keyword in dask-worker (:pr:`1967`) `Matthew Rocklin`_
-  Support serializers in BatchedSend (:pr:`1964`) `Matthew Rocklin`_
-  Use normal serialization mechanisms to serialize published datasets (:pr:`1972`) `Matthew Rocklin`_
-  Add security support to LocalCluster. (:pr:`1855`) `Marius van Niekerk`_
-  add ConnectionPool.remove method (:pr:`1977`) `Tony Lorenzo`_
-  Cleanly close workers when scheduler closes (:pr:`1981`) `Matthew Rocklin`_
-  Add .pyz support in upload_file  (:pr:`1781`) `@bmaisson`_
-  add comm to packages (:pr:`1980`) `Matthew Rocklin`_
-  Replace dask.set_options with dask.config.set `Matthew Rocklin`_
-  Exclude versions of sortedcontainers which do not have .iloc. (:pr:`1993`) `Russ Bubley`_
-  Exclude gc statistics under PyPy (:pr:`1997`) `Marius van Niekerk`_
-  Manage recent config and dataframe changes in dask (:pr:`2009`) `Matthew Rocklin`_
-  Cleanup lingering clients in tests (:pr:`2012`) `Matthew Rocklin`_
-  Use timeouts during `Client._ensure_connected` (:pr:`2011`) `Martin Durant`_
-  Avoid reference cycle in joblib backend (:pr:`2014`) `Matthew Rocklin`_, also `Olivier Grisel`_
-  DOC: fixed test example (:pr:`2017`) `Tom Augspurger`_
-  Add worker_key parameter to Adaptive (:pr:`1992`) `Matthew Rocklin`_
-  Prioritize tasks with their true keys, before stringifying (:pr:`2006`) `Matthew Rocklin`_
-  Serialize worker exceptions through normal channels (:pr:`2016`) `Matthew Rocklin`_
-  Include exception in progress bar (:pr:`2028`) `Matthew Rocklin`_
-  Avoid logging orphaned futures in All (:pr:`2008`) `Matthew Rocklin`_
-  Don't use spill-to-disk dictionary if we're not spilling to disk `Matthew Rocklin`_
-  Only avoid recomputation if key exists (:pr:`2036`) `Matthew Rocklin`_
-  Use client connection and serialization arguments in progress (:pr:`2035`) `Matthew Rocklin`_
-  Rejoin worker client on closing context manager (:pr:`2041`) `Matthew Rocklin`_
-  Avoid forgetting erred tasks when losing dependencies (:pr:`2047`) `Matthew Rocklin`_
-  Avoid collisions in graph_layout (:pr:`2050`) `Matthew Rocklin`_
-  Avoid recursively calling bokeh callback in profile plot (:pr:`2048`) `Matthew Rocklin`_


1.21.8 - 2018-05-03
-------------------

-  Remove errant print statement (:pr:`1957`) `Matthew Rocklin`_
-  Only add reevaluate_occupancy callback once (:pr:`1953`) `Tony Lorenzo`_


1.21.7 - 2018-05-02
-------------------

-  Newline needed for doctest rendering (:pr:`1917`) `Loïc Estève`_
-  Support Client._repr_html_ when in async mode (:pr:`1909`) `Matthew Rocklin`_
-  Add parameters to dask-ssh command (:pr:`1910`) `Irene Rodriguez`_
-  Santize get_dataset trace (:pr:`1888`) `John Kirkham`_
-  Fix bug where queues would not clean up cleanly (:pr:`1922`) `Matthew Rocklin`_
-  Delete cached file safely in upload file (:pr:`1921`) `Matthew Rocklin`_
-  Accept KeyError when closing tornado IOLoop in tests (:pr:`1937`) `Matthew Rocklin`_
-  Quiet the client and scheduler when gather(..., errors='skip') (:pr:`1936`) `Matthew Rocklin`_
-  Clarify couldn't gather keys warning (:pr:`1942`) `Kenneth Koski`_
-  Support submit keywords in joblib (:pr:`1947`) `Matthew Rocklin`_
-  Avoid use of external resources in bokeh server (:pr:`1934`) `Matthew Rocklin`_
-  Drop `__contains__` from `Datasets` (:pr:`1889`) `John Kirkham`_
-  Fix bug with queue timeouts (:pr:`1950`) `Matthew Rocklin`_
-  Replace msgpack-python by msgpack (:pr:`1927`) `Loïc Estève`_


1.21.6 - 2018-04-06
-------------------

-  Fix numeric environment variable configuration (:pr:`1885`) `Joseph Atkins-Kurkish`_
-  support bytearrays in older lz4 library (:pr:`1886`) `Matthew Rocklin`_
-  Remove started timeout in nanny (:pr:`1852`) `Matthew Rocklin`_
-  Don't log errors in sync (:pr:`1894`) `Matthew Rocklin`_
-  downgrade stale lock warning to info logging level (:pr:`1890`) `Matthew Rocklin`_
-  Fix ``UnboundLocalError`` for ``key`` (:pr:`1900`) `John Kirkham`_
-  Resolve deployment issues in Python 2 (:pr:`1905`) `Matthew Rocklin`_
-  Support retries and priority in Client.get method (:pr:`1902`) `Matthew Rocklin`_
-  Add additional attributes to task page if applicable (:pr:`1901`) `Matthew Rocklin`_
-  Add count method to as_completed (:pr:`1897`) `Matthew Rocklin`_
-  Extend default timeout to 10s (:pr:`1904`) `Matthew Rocklin`_




1.21.5 - 2018-03-31
-------------------

-  Increase default allowable tick time to 3s (:pr:`1854`) `Matthew Rocklin`_
-  Handle errant workers when another worker has data (:pr:`1853`) `Matthew Rocklin`_
-  Close multiprocessing queue in Nanny to reduce open file descriptors (:pr:`1862`) `Matthew Rocklin`_
-  Extend nanny started timeout to 30s, make configurable (:pr:`1865`) `Matthew Rocklin`_
-  Comment out the default config file (:pr:`1871`) `Matthew Rocklin`_
-  Update to fix bokeh 0.12.15 update errors (:pr:`1872`) `Matthew Rocklin`_
-  Downgrade Event Loop unresponsive warning to INFO level (:pr:`1870`) `Matthew Rocklin`_
-  Add fifo timeout to control priority generation (:pr:`1828`) `Matthew Rocklin`_
-  Add retire_workers API to Client (:pr:`1876`) `Matthew Rocklin`_
-  Catch NoSuchProcess error in Nanny.memory_monitor (:pr:`1877`) `Matthew Rocklin`_
-  Add uid to nanny queue communitcations (:pr:`1880`) `Matthew Rocklin`_


1.21.4 - 2018-03-21
-------------------

-  Avoid passing bytearrays to snappy decompression (:pr:`1831`) `Matthew Rocklin`_
-  Specify IOLoop in Adaptive (:pr:`1841`) `Matthew Rocklin`_
-  Use connect-timeout config value throughout client (:pr:`1839`) `Matthew Rocklin`_
-  Support direct= keyword argument in Client.get (:pr:`1845`) `Matthew Rocklin`_


1.21.3 - 2018-03-08
-------------------

-  Add cluster superclass and improve adaptivity (:pr:`1813`) `Matthew Rocklin`_
-  Fixup tests and support Python 2 for Tornado 5.0 (:pr:`1818`) `Matthew Rocklin`_
-  Fix bug in recreate_error when dependencies are dropped (:pr:`1815`) `Matthew Rocklin`_
-  Add worker time to live in Scheduler (:pr:`1811`) `Matthew Rocklin`_
-  Scale adaptive based on total_occupancy (:pr:`1807`) `Matthew Rocklin`_
-  Support calling compute within worker_client (:pr:`1814`) `Matthew Rocklin`_
-  Add percentage to profile plot (:pr:`1817`) `Brett Naul`_
-  Overwrite option for remote python in dask-ssh (:pr:`1812`) `Sven Kreiss`_


1.21.2 - 2018-03-05
-------------------

-  Fix bug where we didn't check idle/saturated when stealing (:pr:`1801`) `Matthew Rocklin`_
-  Fix bug where client was noisy when scheduler closed unexpectedly (:pr:`1806`) `Matthew Rocklin`_
-  Use string-based timedeltas (like ``'500 ms'``) everywhere (:pr:`1804`) `Matthew Rocklin`_
-  Keep logs in scheduler and worker even if silenced (:pr:`1803`) `Matthew Rocklin`_
-  Support minimum, maximum, wait_count keywords in Adaptive (:pr:`1797`) `Jacob Tomlinson`_ and `Matthew Rocklin`_
-  Support async protocols for LocalCluster, replace start= with asynchronous= (:pr:`1798`) `Matthew Rocklin`_
-  Avoid restarting workers when nanny waits on scheduler (:pr:`1793`) `Matthew Rocklin`_
-  Use ``IOStream.read_into()`` when available (:pr:`1477`) `Antoine Pitrou`_
-  Reduce LocalCluster logging threshold from CRITICAL to WARN (:pr:`1785`) `Andy Jones`_
-  Add `futures_of` to API docs (:pr:`1783`) `John Kirkham`_
-  Make diagnostics link in client configurable (:pr:`1810`) `Matthew Rocklin`_


1.21.1 - 2018-02-22
-------------------

-  Fixed an uncaught exception in ``distributed.joblib`` with a ``LocalCluster`` using only threads (:issue:`1775`) `Tom Augspurger`_
-  Format bytes in info worker page (:pr:`1752`) `Matthew Rocklin`_
-  Add pass-through arguments for scheduler/worker `--preload` modules. (:pr:`1634`) `Alexander Ford`_
-  Use new LZ4 API (:pr:`1757`) `Thrasibule`_
-  Replace dask.optimize with dask.optimization (:pr:`1754`) `Matthew Rocklin`_
-  Add graph layout engine and bokeh plot (:pr:`1756`) `Matthew Rocklin`_
-  Only expand name with --nprocs if name exists (:pr:`1776`) `Matthew Rocklin`_
-  specify IOLoop for stealing PeriodicCallback (:pr:`1777`) `Matthew Rocklin`_
-  Fixed distributed.joblib with no processes `Tom Augspurger`_
-  Use set.discard to avoid KeyErrors in stealing (:pr:`1766`) `Matthew Rocklin`_
-  Avoid KeyError when task has been released during steal (:pr:`1765`) `Matthew Rocklin`_
-  Add versions routes to avoid the use of run in Client.get_versions (:pr:`1773`) `Matthew Rocklin`_
-  Add write_scheduler_file to Client (:pr:`1778`) `Joe Hamman`_
-  Default host to tls:// if tls information provided (:pr:`1780`) `Matthew Rocklin`_


1.21.0 - 2018-02-09
-------------------

-  Refactor scheduler to use TaskState objects rather than dictionaries (:pr:`1594`) `Antoine Pitrou`_
-  Plot CPU fraction of total in workers page (:pr:`1624`) `Matthew Rocklin`_
-  Use thread CPU time in Throttled GC (:pr:`1625`) `Antoine Pitrou`_
-  Fix bug with ``memory_limit=None`` (:pr:`1639`) `Matthew Rocklin`_
-  Add futures_of to top level api (:pr:`1646`) `Matthew Rocklin`_
-  Warn on serializing large data in Client (:pr:`1636`) `Matthew Rocklin`_
-  Fix intermittent windows failure when removing lock file (:pr:`1652`) `Antoine Pitrou`_
-  Add diagnosis and logging of poor GC Behavior (:pr:`1635`) `Antoine Pitrou`_
-  Add client-scheduler heartbeats (:pr:`1657`) `Matthew Rocklin`_
-  Return dictionary of worker info in ``retire_workers`` (:pr:`1659`) `Matthew Rocklin`_
-  Ensure dumps_function works with unhashable functions (:pr:`1662`) `Matthew Rocklin`_
-  Collect client name ids rom client-name config variable (:pr:`1664`) `Matthew Rocklin`_
-  Allow simultaneous use of --name and --nprocs in dask-worker (:pr:`1665`) `Matthew Rocklin`_
-  Add support for grouped adaptive scaling and adaptive behavior overrides (:pr:`1632`) `Alexander Ford`_
-  Share scheduler RPC between worker and client (:pr:`1673`) `Matthew Rocklin`_
-  Allow ``retries=`` in ClientExecutor (:pr:`1672`) `@rqx`_
-  Improve documentation for get_client and dask.compute examples (:pr:`1638`) `Scott Sievert`_
-  Support DASK_SCHEDULER_ADDRESS environment variable in worker (:pr:`1680`) `Matthew Rocklin`_
-  Support tuple-keys in retries (:pr:`1681`) `Matthew Rocklin`_
-  Use relative links in bokeh dashboard (:pr:`1682`) `Matthew Rocklin`_
-  Make message log length configurable, default to zero (:pr:`1691`) `Matthew Rocklin`_
-  Deprecate ``Client.shutdown`` (:pr:`1699`) `Matthew Rocklin`_
-  Add warning in configuration docs to install pyyaml (:pr:`1701`) `Cornelius Riemenschneider`_
-  Handle nested parallelism in distributed.joblib (:pr:`1705`) `Tom Augspurger`_
-  Don't wait for Worker.executor to shutdown cleanly when restarting process (:pr:`1708`) `Matthew Rocklin`_
-  Add support for user defined priorities (:pr:`1651`) `Matthew Rocklin`_
-  Catch and log OSErrors around worker lock files (:pr:`1714`) `Matthew Rocklin`_
-  Remove worker prioritization.  Coincides with changes to dask.order (:pr:`1730`) `Matthew Rocklin`_
-  Use process-measured memory rather than nbytes in Bokeh dashboard (:pr:`1737`) `Matthew Rocklin`_
-  Enable serialization of Locks  (:pr:`1738`) `Matthew Rocklin`_
-  Support Tornado 5 beta (:pr:`1735`) `Matthew Rocklin`_
-  Cleanup remote_magic client cache after tests (:pr:`1743`) `Min RK`_
-  Allow service ports to be specified as (host, port) (:pr:`1744`) `Bruce Merry`_


1.20.2 - 2017-12-07
-------------------

-  Clear deque handlers after each test (:pr:`1586`) `Antoine Pitrou`_
-  Handle deserialization in FutureState.set_error (:pr:`1592`) `Matthew Rocklin`_
-  Add process leak checker to tests (:pr:`1596`) `Antoine Pitrou`_
-  Customize process title for subprocess (:pr:`1590`) `Antoine Pitrou`_
-  Make linting a separate CI job (:pr:`1599`) `Antoine Pitrou`_
-  Fix error from get_client() with no global client (:pr:`1595`) `Daniel Li`_
-  Remove Worker.host_health, correct WorkerTable metrics (:pr:`1600`) `Matthew Rocklin`_
-  Don't mark tasks as suspicious when retire_workers called. Addresses (:pr:`1607`) `Russ Bubley`_
-  Do not include processing workers in workers_to_close (:pr:`1609`) `Russ Bubley`_
-  Disallow simultaneous scale up and down in Adaptive (:pr:`1608`) `Russ Bubley`_
-  Parse bytestrings in --memory-limit (:pr:`1615`) `Matthew Rocklin`_
-  Use environment variable for scheduler address if present (:pr:`1610`) `Matthew Rocklin`_
-  Fix deprecation warning from logger.warn (:pr:`1616`) `Brett Naul`_




1.20.1 - 2017-11-26
-------------------

- Wrap ``import ssl`` statements with try-except block for ssl-crippled environments, (:pr:`1570`) `Xander Johnson`_
- Support zero memory-limit in Nanny (:pr:`1571`) `Matthew Rocklin`_
- Avoid PeriodicCallback double starts (:pr:`1573`) `Matthew Rocklin`_
- Add disposable workspace facility (:pr:`1543`) `Antoine Pitrou`_
- Use format_time in task_stream plots (:pr:`1575`) `Matthew Rocklin`_
- Avoid delayed finalize calls in compute (:pr:`1577`) `Matthew Rocklin`_
- Doc fix about secede (:pr:`1583`) `Scott Sievert`_
- Add tracemalloc option when tracking test leaks (:pr:`1585`) `Antoine Pitrou`_
- Add JSON routes to Bokeh server (:pr:`1584`) `Matthew Rocklin`_
- Handle exceptions cleanly in Variables and Queues (:pr:`1580`) `Matthew Rocklin`_


1.20.0 - 2017-11-17
-------------------

-  Drop use of pandas.msgpack (:pr:`1473`) `Matthew Rocklin`_
-  Add methods to get/set scheduler metadata `Matthew Rocklin`_
-  Add distributed lock `Matthew Rocklin`_
-  Add reschedule exception for worker tasks `Matthew Rocklin`_
-  Fix ``nbytes()`` for ``bytearrays`` `Matthew Rocklin`_
-  Capture scheduler and worker logs `Matthew Rocklin`_
-  Garbage collect after data eviction on high worker memory usage (:pr:`1488`) `Olivier Grisel`_
-  Add scheduler HTML routes to bokeh server (:pr:`1478`) (:pr:`1514`) `Matthew Rocklin`_
-  Add pytest plugin to test for resource leaks (:pr:`1499`) `Antoine Pitrou`_
-  Improve documentation for scheduler states (:pr:`1498`) `Antoine Pitrou`_
-  Correct warn_if_longer timeout in ThrottledGC (:pr:`1496`) `Fabian Keller`_
-  Catch race condition in as_completed on cancelled futures (:pr:`1507`) `Matthew Rocklin`_
-  Transactional work stealing (:pr:`1489`) (:pr:`1528`) `Matthew Rocklin`_
-  Avoid forkserver in PyPy (:pr:`1509`) `Matthew Rocklin`_
-  Add dict access to get/set datasets (:pr:`1508`) `Mike DePalatis`_
-  Support Tornado 5 (:pr:`1509`) (:pr:`1512`) (:pr:`1518`) (:pr:`1534`) `Antoine Pitrou`_
-  Move thread_state in Dask (:pr:`1523`) `Jim Crist`_
-  Use new Dask collections interface (:pr:`1513`) `Matthew Rocklin`_
-  Add nanny flag to dask-mpi `Matthew Rocklin`_
-  Remove JSON-based HTTP servers `Matthew Rocklin`_
-  Avoid doing I/O in repr/str (:pr:`1536`) `Matthew Rocklin`_
-  Fix URL for MPI4Py project (:pr:`1546`) `Ian Hopkinson`_
-  Allow automatic retries of a failed task (:pr:`1524`) `Antoine Pitrou`_
-  Clean and accelerate tests (:pr:`1548`) (:pr:`1549`) (:pr:`1552`)
   (:pr:`1553`) (:pr:`1560`) (:pr:`1564`) `Antoine Pitrou`_
- Move HDFS functionality to the hdfs3 library (:pr:`1561`) `Jim Crist`_
-  Fix bug when using events page with no events (:pr:`1562`) `@rbubley`_
-  Improve diagnostic naming of tasks within tuples (:pr:`1566`) `Kelvyn Yang`_

1.19.3 - 2017-10-16
-------------------

-  Handle None case in profile.identity (:pr:`1456`)
-  Asyncio rewrite (:pr:`1458`)
-  Add rejoin function partner to secede (:pr:`1462`)
-  Nested compute (:pr:`1465`)
-  Use LooseVersion when comparing Bokeh versions (:pr:`1470`)


1.19.2 - 2017-10-06
-------------------

-  as_completed doesn't block on cancelled futures (:pr:`1436`)
-  Notify waiting threads/coroutines on cancellation (:pr:`1438`)
-  Set Future(inform=True) as default (:pr:`1437`)
-  Rename Scheduler.transition_story to story (:pr:`1445`)
-  Future uses default client by default (:pr:`1449`)
-  Add keys= keyword to Client.call_stack (:pr:`1446`)
-  Add get_current_task to worker (:pr:`1444`)
-  Ensure that Client remains asynchornous before ioloop starts (:pr:`1452`)
-  Remove "click for worker page" in bokeh plot (:pr:`1453`)
-  Add Client.current() (:pr:`1450`)
-  Clean handling of restart timeouts (:pr:`1442`)

1.19.1 - September 25th, 2017
-----------------------------

-  Fix tool issues with TaskStream plot (:pr:`1425`)
-  Move profile module to top level (:pr:`1423`)

1.19.0 - September 24th, 2017
-----------------------------

-  Avoid storing messages in message log (:pr:`1361`)
-  fileConfig does not disable existing loggers (:pr:`1380`)
-  Offload upload_file disk I/O to separate thread (:pr:`1383`)
-  Add missing SSLContext (:pr:`1385`)
-  Collect worker thread information from sys._curent_frames (:pr:`1387`)
-  Add nanny timeout (:pr:`1395`)
-  Restart worker if memory use goes above 95% (:pr:`1397`)
-  Track workers memory use with psutil (:pr:`1398`)
-  Track scheduler delay times in workers (:pr:`1400`)
-  Add time slider to profile plot (:pr:`1403`)
-  Change memory-limit keyword to refer to maximum number of bytes (:pr:`1405`)
-  Add ``cancel(force=)`` keyword (:pr:`1408`)

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
-  Avoid error messages on closing (:pr:`1297`), (:pr:`1296`) (:pr:`1318`) (:pr:`1319`)
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
-  Add get_client, secede functions, refactor worker-client relationship (:pr:`1201`)
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

.. _`Antoine Pitrou`: https://github.com/pitrou
.. _`Olivier Grisel`: https://github.com/ogrisel
.. _`Fabian Keller`: https://github.com/bluenote10
.. _`Mike DePalatis`: https://github.com/mivade
.. _`Matthew Rocklin`: https://github.com/mrocklin
.. _`Jim Crist`: https://github.com/jcrist
.. _`Ian Hopkinson`: https://github.com/IanHopkinson
.. _`@rbubley`: https://github.com/rbubley
.. _`Kelvyn Yang`: https://github.com/kelvynyang
.. _`Scott Sievert`: https://github.com/stsievert
.. _`Xander Johnson`: https://github.com/metasyn
.. _`Daniel Li`: https://github.com/li-dan
.. _`Brett Naul`: https://github.com/bnaul
.. _`Cornelius Riemenschneider`: https://github.com/corni
.. _`Alexander Ford`: https://github.com/asford
.. _`@rqx`: https://github.com/rqx
.. _`Min RK`: https://github.comminrk/
.. _`Bruce Merry`: https://github.com/bmerry
.. _`Tom Augspurger`: https://github.com/TomAugspurger
.. _`Joe Hamman`: https://github.com/jhamman
.. _`Thrasibule`: https://github.com/thrasibule
.. _`Jacob Tomlinson`: https://github.com/jacobtomlinson
.. _`Andy Jones`: https://github.com/andyljones
.. _`John Kirkham`: https://github.com/jakirkham
.. _`Sven Kreiss`:  https://github.com/svenkreiss
.. _`Russ Bubley`: https://github.com/rbubley
.. _`Joseph Atkins-Kurkish`: https://github.com/spacerat
.. _`Irene Rodriguez`: https://github.com/irenerodriguez
.. _`Loïc Estève`: https://github.com/lesteve
.. _`Kenneth Koski`: https://github.com/knkski
.. _`Tony Lorenzo`: https://github.com/alorenzo175
.. _`Henry Doupe`: https://github.com/hdoupe
.. _`Marius van Niekerk`: https://github.com/mariusvniekerk
.. _`@bmaisson`: https://github.com/bmaisson
.. _`Martin Durant`: https://github.com/martindurant
.. _`Grant Jenks`: https://github.com/grantjenks
.. _`Dror Birkman`: https://github.com/Dror-LightCyber
.. _`Dave Hirschfeld`: https://github.com/dhirschfeld
.. _`Matt Nicolls`: https://github.com/nicolls1
.. _`Phil Tooley`: https://github.com/ptooley
.. _`Bartosz Marcinkowski`: https://github.com/bm371613
.. _`Aurélien Ponte`: https://github.com/apatlpo
.. _`Luke Canavan`: https://github.com/canavandl
.. _`Derek Ludwig`: https://github.com/dsludwig
.. _`Anderson Banihirwe`: https://github.com/andersy005
.. _`Yu Feng`: https://github.com/rainwoodman
.. _`Guillaume Eynard-Bontemps`: https://github.com/guillaumeeb
.. _`Vladyslav Moisieienkov`: https://github.com/VMois
.. _`Chris White`: https://github.com/cicdw
.. _`Adam Klein`: https://github.com/adamklein
.. _`Mike Gevaert`: https://github.com/mgeplf
.. _`Gaurav Sheni`: https://github.com/gsheni
.. _`Eric Ma`: https://github.com/ericmjl
.. _`Peter Killick`: https://github.com/dkillick
.. _`NotSqrt`: https://github.com/NotSqrt
.. _`Tom Rochette`: https://github.com/tomzx
.. _`Roy Wedge`: https://github.com/rwedge
.. _`Michael Wheeler`: https://github.com/mikewheel
.. _`Diane Trout`: https://github.com/detrout
.. _`tjb900`: https://github.com/tjb900
.. _`Stephan Hoyer`: https://github.com/shoyer
.. _`tjb900`: https://github.com/tjb900
.. _`Dirk Petersen`: https://github.com/dirkpetersen
.. _`Daniel Farrell`: https://github.com/danpf
.. _`George Sakkis`: https://github.com/gsakkis
.. _`Adam Beberg`: https://github.com/beberg
.. _`Marco Neumann`: https://github.com/crepererum
.. _`Elliott Sales de Andrade`: https://github.com/QuLogic
.. _`Brett Jurman`: https://github.com/ibebrett
.. _`Julia Kent`: https://github.com/jukent
.. _`Brett Randall`: https://github.com/javabrett
.. _`deepthirajagopalan7`: https://github.com/deepthirajagopalan7
.. _`Muammar El Khatib`: https://github.com/muammar
.. _`Nikos Tsaousis`: https://github.com/tsanikgr
.. _`Brian Chu`: https://github.com/bchu
.. _`James Bourbeau`: https://github.com/jrbourbeau
.. _`amerkel2`: https://github.com/amerkel2
.. _`Michael Delgado`: https://github.com/delgadom
.. _`Peter Andreas Entschev`: https://github.com/pentschev
.. _`condoratberlin`: https://github.com/condoratberlin
.. _`K.-Michael Aye`: https://github.com/michaelaye
.. _`@plbertrand`: https://github.com/plbertrand
