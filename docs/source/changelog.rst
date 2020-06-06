Changelog
=========

2.18.0 - 2020-06-05
-------------------

- Merge frames in ``deserialize_bytes`` (:pr:`3639`) `John Kirkham`_
- Allow ``SSHCluster`` to take a list of ``connect_options`` (:pr:`3854`) `Jacob Tomlinson`_
- Add favicon to performance report (:pr:`3852`) `Jacob Tomlinson`_
- Add dashboard plots for the amount of time spent per key and for transfer/serialization (:pr:`3792`) `Benjamin Zaitlen`_
- Fix variable name in journey of a task documentation (:pr:`3840`) `Matthias Bussonnier`_
- Fix typo in journey of a task doc (:pr:`3838`) `James Bourbeau`_
- Register ``dask_cudf`` serializers (:pr:`3832`) `John Kirkham`_
- Fix key check in ``rebalance`` missing keys (:pr:`3834`) `Jacob Tomlinson`_
- Allow collection of partial profile information in case of exceptions (:pr:`3773`) `Florian Jetter`_


2.17.0 - 2020-05-26
-------------------

- Record the time since the last run task on the scheduler (:pr:`3830`) `Matthew Rocklin`_
- Set colour of ``nbytes`` pane based on thresholds (:pr:`3805`) `Krishan Bhasin`_
- Include total number of tasks in the performance report (:pr:`3822`) `Abdulelah Bin Mahfoodh`_
- Allow to pass in task key strings in the worker restrictions (:pr:`3826`) `Nils Braun`_
- Control de/ser offload (:pr:`3793`) `Martin Durant`_
- Parse timeout parameters in ``Variable``/``Event``/``Lock`` to support text timeouts (:pr:`3825`) `Nils Braun`_
- Don't send empty dependencies (:pr:`3423`) `Jakub Beránek`_
- Add distributed Dask ``Event`` that mimics ``threading.Event`` (:pr:`3821`) `Nils Braun`_
- Enhance ``VersionMismatchWarning`` messages (:pr:`3786`) `Abdulelah Bin Mahfoodh`_
- Support Pickle's protocol 5 (:pr:`3784`) `jakirkham`_
- Replace ``utils.ignoring`` with ``contextlib.suppress`` (:pr:`3819`) `Nils Braun`_
- Make re-creating conda environments from the CI output easier (:pr:`3816`) `Lucas Rademaker`_
- Add prometheus metrics for semaphore (:pr:`3757`) `Lucas Rademaker`_
- Fix worker plugin called with superseded transition (:pr:`3812`) `Nick Evans`_
- Add retries to server listen (:pr:`3801`) `Jacob Tomlinson`_
- Remove commented out lines from ``scheduler.py`` (:pr:`3803`) `James Bourbeau`_
- Fix ``RuntimeWarning`` for never awaited coroutine when using ``distributed.Semaphore`` (:pr:`3713`) `Florian Jetter`_
- Fix profile thread leakage during test teardown on some platforms (:pr:`3795`) `Florian Jetter`_
- Await self before handling comms (:pr:`3788`) `Matthew Rocklin`_
- Fix typo in ``Cluster`` docstring (:pr:`3787`) `Scott Sanderson`_


2.16.0 - 2020-05-08
-------------------

- ``Client.get_dataset`` to always create ``Futures`` attached to itself (:pr:`3729`) `crusaderky`_
- Remove dev-requirements since it is unused (:pr:`3782`) `Julia Signell`_
- Use bokeh column for ``/system`` instead of custom css (:pr:`3781`) `Julia Signell`_
- Attempt to fix ``test_preload_remote_module`` on windows (:pr:`3775`) `James Bourbeau`_
- Fix broadcast for TLS comms (:pr:`3766`) `Florian Jetter`_
- Don't validate http preloads locally (:pr:`3768`) `Rami Chowdhury`_
- Allow range of ports to be specified for ``Workers`` (:pr:`3704`) `James Bourbeau`_
- Add UCX support for RDMACM (:pr:`3759`) `Peter Andreas Entschev`_
- Support web addresses in preload (:pr:`3755`) `Matthew Rocklin`_


2.15.2 - 2020-05-01
-------------------

- Connect to dashboard when address provided (:pr:`3758`) `Tom Augspurger`_
- Move ``test_gpu_metrics test`` (:pr:`3721`) `Tom Augspurger`_
- Nanny closing worker on ``KeyboardInterrupt`` (:pr:`3747`) `Mads R. B. Kristensen`_
- Replace ``OrderedDict`` with ``dict`` in scheduler (:pr:`3740`) `Matthew Rocklin`_
- Fix exception handling typo (:pr:`3751`) `Jonas Haag`_


2.15.1 - 2020-04-28
-------------------

- Ensure ``BokehTornado`` uses prefix (:pr:`3746`) `James Bourbeau`_
- Warn if cluster closes before starting (:pr:`3735`) `Matthew Rocklin`_
- Memoryview serialisation (:pr:`3743`) `Martin Durant`_
- Allows logging config under distributed key (:pr:`2952`) `Dillon Niederhut`_


2.15.0 - 2020-04-24
-------------------

- Reinstate support for legacy ``@gen_cluster`` functions (:pr:`3738`) `crusaderky`_
- Relax NumPy requirement in UCX (:pr:`3731`) `jakirkham`_
- Add Configuration Schema (:pr:`3696`) `Matthew Rocklin`_
- Reuse CI scripts for local installation process (:pr:`3698`) `crusaderky`_
- Use ``PeriodicCallback`` class from tornado (:pr:`3725`) `James Bourbeau`_
- Add ``remote_python`` option in ssh cmd (:pr:`3709`) `Abdulelah Bin Mahfoodh`_
- Configurable polling interval for cluster widget (:pr:`3723`) `Julia Signell`_
- Fix copy-paste in docs (:pr:`3728`) `Julia Signell`_
- Replace ``gen.coroutine`` with async-await in tests (:pr:`3706`) `crusaderky`_
- Fix flaky ``test_oversubscribing_leases`` (:pr:`3726`) `Florian Jetter`_
- Add ``batch_size`` to ``Client.map`` (:pr:`3650`) `Tom Augspurger`_
- Adjust semaphore test timeouts (:pr:`3720`) `Florian Jetter`_
- Dask-serialize dicts longer than five elements (:pr:`3689`) `Richard J Zamora`_
- Force ``threads_per_worker`` (:pr:`3715`) `crusaderky`_
- Idempotent semaphore acquire with retries (:pr:`3690`) `Florian Jetter`_
- Always use ``readinto`` in TCP (:pr:`3711`) `jakirkham`_
- Avoid ``DeprecationWarning`` from pandas (:pr:`3712`) `Tom Augspurger`_
- Allow modification of ``distributed.comm.retry`` at runtime (:pr:`3705`) `Florian Jetter`_
- Do not log an error on unset variable delete (:pr:`3652`) `Jonathan J. Helmus`_
- Add ``remote_python`` keyword to the new ``SSHCluster`` (:pr:`3701`) `Abdulelah Bin Mahfoodh`_
- Replace Example with Examples in docstrings (:pr:`3697`) `Matthew Rocklin`_
- Add ``Cluster`` ``__enter__`` and ``__exit__`` methods (:pr:`3699`) `Matthew Rocklin`_
- Fix propagating inherit config in ``SSHCluster`` for non-bash shells (:pr:`3688`) `Abdulelah Bin Mahfoodh`_
- Add ``Client.wait_to_workers`` to ``Client`` autosummary table (:pr:`3692`) `James Bourbeau`_
- Replace Bokeh Server with Tornado HTTPServer (:pr:`3658`) `Matthew Rocklin`_
- Fix ``dask-ssh`` after removing ``local-directory`` from ``dask_scheduler`` cli (:pr:`3684`) `Abdulelah Bin Mahfoodh`_
- Support preload modules in ``Nanny`` (:pr:`3678`) `Matthew Rocklin`_
- Refactor semaphore internals: make ``_get_lease`` synchronous (:pr:`3679`) `Lucas Rademaker`_
- Don't make task graphs too big (:pr:`3671`) `Martin Durant`_
- Pass through ``connection``/``listen_args`` as splatted keywords (:pr:`3674`) `Matthew Rocklin`_
- Run preload at import, start, and teardown (:pr:`3673`) `Matthew Rocklin`_
- Use relative URL in scheduler dashboard (:pr:`3676`) `Nicholas Smith`_
- Expose ``Security`` object as public API (:pr:`3675`) `Matthew Rocklin`_
- Add zoom tools to profile plots (:pr:`3672`) `James Bourbeau`_
- Update ``Scheduler.rebalance`` return value when data is missing (:pr:`3670`) `James Bourbeau`_


2.14.0 - 2020-04-03
-------------------

- Enable more UCX tests (:pr:`3667`) `jakirkham`_
- Remove openssl 1.1.1d pin for Travis (:pr:`3668`) `Jonathan J. Helmus`_
- More documentation for ``Semaphore`` (:pr:`3664`) `Florian Jetter`_
- Get CUDA context to finalize Numba ``DeviceNDArray`` (:pr:`3666`) `jakirkham`_
- Add Resouces option to ``get_task_stream`` and call ``output_file`` (:pr:`3653`) `Prasun Anand`_
- Add ``Semaphore`` extension (:pr:`3573`) `Lucas Rademaker`_
- Replace ``ncores`` with ``nthreads`` in work stealing tests (:pr:`3615`) `James Bourbeau`_
- Clean up some test warnings (:pr:`3662`) `Matthew Rocklin`_
- Write "why killed" docs (:pr:`3596`) `Martin Durant`_
- Update Python version checking (:pr:`3660`) `James Bourbeau`_
- Add newlines to ensure code formatting for ``retire_workers`` (:pr:`3661`) `Rami Chowdhury`_
- Clean up performance report test (:pr:`3655`) `Matthew Rocklin`_
- Avoid diagnostics time in performance report (:pr:`3654`) `Matthew Rocklin`_
- Introduce config for default task duration (:pr:`3642`) `Gabriel Sailer`_
- UCX simplify receiving frames in ``comm`` (:pr:`3651`) `jakirkham`_
- Bump checkout GitHub action to v2 (:pr:`3649`) `James Bourbeau`_
- Handle exception in ``faulthandler`` (:pr:`3646`) `Jacob Tomlinson`_
- Add prometheus metric for suspicious tasks (:pr:`3550`) `Gabriel Sailer`_
- Remove ``local-directory`` keyword (:pr:`3620`) `Prasun Anand`_
- Don't create output Futures in Client when there are mixed Client Futures (:pr:`3643`) `James Bourbeau`_
- Add link to ``contributing.md`` (:pr:`3621`) `Prasun Anand`_
- Update bokeh dependency in CI builds (:pr:`3637`) `James Bourbeau`_


2.13.0 - 2020-03-25
-------------------

- UCX synchronize default stream only on CUDA frames (:pr:`3638`) `Peter Andreas Entschev`_
- Add ``as_completed.clear`` method (:pr:`3617`) `Matthew Rocklin`_
- Drop unused line from ``pack_frames_prelude`` (:pr:`3634`) `John Kirkham`_
- Add logging message when closing idle dask scheduler (:pr:`3632`) `Matthew Rocklin`_
- Include frame lengths of CUDA objects in ``header["lengths"]`` (:pr:`3631`) `John Kirkham`_
- Ensure ``Client`` connection pool semaphore attaches to the ``Client`` event loop (:pr:`3546`) `James Bourbeau`_
- Remove dead stealing code (:pr:`3619`) `Florian Jetter`_
- Check ``nbytes`` and ``types`` before reading ``data`` (:pr:`3628`) `John Kirkham`_
- Ensure that we don't steal blacklisted fast tasks (:pr:`3591`) `Florian Jetter`_
- Support async ``Listener.stop`` functions (:pr:`3613`) `Matthew Rocklin`_
- Add str/repr methods to ``as_completed`` (:pr:`3618`) `Matthew Rocklin`_
- Add backoff to comm connect attempts. (:pr:`3496`) `Matthias Urlichs`_
- Make ``Listeners`` awaitable (:pr:`3611`) `Matthew Rocklin`_
- Increase number of visible mantissas in dashboard plots (:pr:`3585`) `Scott Sievert`_
- Pin openssl to 1.1.1d for Travis (:pr:`3602`) `Jacob Tomlinson`_
- Replace ``tornado.queues`` with ``asyncio.queues`` (:pr:`3607`) `James Bourbeau`_
- Remove ``dill`` from CI environments (:pr:`3608`) `Loïc Estève`_
- Fix linting errors (:pr:`3604`) `James Bourbeau`_
- Synchronize default CUDA stream before UCX send/recv (:pr:`3598`) `Peter Andreas Entschev`_
- Add configuration for ``Adaptive`` arguments (:pr:`3509`) `Gabriel Sailer`_
- Change ``Adaptive`` docs to reference ``adaptive_target`` (:pr:`3597`) `Julia Signell`_
- Optionally compress on a frame-by-frame basis (:pr:`3586`) `Matthew Rocklin`_
- Add Python version to version check (:pr:`3567`) `James Bourbeau`_
- Import ``tlz`` (:pr:`3579`) `John Kirkham`_
- Pin ``numpydoc`` to avoid double escaped ``*`` (:pr:`3530`) `Gil Forsyth`_
- Avoid ``performance_report`` crashing when a worker dies mid-compute (:pr:`3575`) `Krishan Bhasin`_
- Pin ``bokeh`` in CI builds (:pr:`3570`) `James Bourbeau`_
- Disable fast fail on GitHub Actions Windows CI (:pr:`3569`) `James Bourbeau`_
- Fix typo in ``Client.shutdown`` docstring (:pr:`3562`) `John Kirkham`_
- Add ``local_directory`` option to ``dask-ssh`` (:pr:`3554`) `Abdulelah Bin Mahfoodh`_


2.12.0 - 2020-03-06
-------------------

- Update ``TaskGroup`` remove logic (:pr:`3557`) `James Bourbeau`_
- Fix-up CuPy sparse serialization (:pr:`3556`) `John Kirkham`_
- API docs for ``LocalCluster`` and ``SpecCluster`` (:pr:`3548`) `Tom Augspurger`_
- Serialize sparse arrays (:pr:`3545`) `John Kirkham`_
- Allow tasks with restrictions to be stolen (:pr:`3069`) `Stan Seibert`_
- Use UCX default configuration instead of raising (:pr:`3544`) `Peter Andreas Entschev`_
- Support using other serializers with ``register_generic`` (:pr:`3536`) `John Kirkham`_
- DOC: update to async await (:pr:`3543`) `Tom Augspurger`_
- Use ``pytest.raises`` in ``test_ucx_config.py`` (:pr:`3541`) `John Kirkham`_
- Fix/more ucx config options (:pr:`3539`) `Benjamin Zaitlen`_
- Update heartbeat ``CommClosedError`` error handling (:pr:`3529`) `James Bourbeau`_
- Use ``makedirs`` when constructing ``local_directory`` (:pr:`3538`) `John Kirkham`_
- Mark ``None`` as MessagePack serializable (:pr:`3537`) `John Kirkham`_
- Mark ``bool`` as MessagePack serializable (:pr:`3535`) `John Kirkham`_
- Use 'temporary-directory' from ``dask.config`` for Nanny's directory (:pr:`3531`) `John Kirkham`_
- Add try-except around getting source code in performance report (:pr:`3505`) `Matthew Rocklin`_
- Fix typo in docstring (:pr:`3528`) `Davis Bennett`_
- Make work stealing callback time configurable (:pr:`3523`) `Lucas Rademaker`_
- RMM/UCX Config Flags (:pr:`3515`) `Benjamin Zaitlen`_
- Revise develop-docs: conda env example (:pr:`3406`) `Darren Weber`_
- Remove ``import ucp`` from the top of ``ucx.py`` (:pr:`3510`) `Peter Andreas Entschev`_
- Rename ``logs`` to ``get_logs`` (:pr:`3473`) `Jacob Tomlinson`_
- Stop keep alives when worker reconnecting to the scheduler (:pr:`3493`) `Jacob Tomlinson`_


2.11.0 - 2020-02-19
-------------------

- Add dask serialization of CUDA objects (:pr:`3482`) `John Kirkham`_
- Suppress cuML ``ImportError`` (:pr:`3499`) `John Kirkham`_
- Msgpack 1.0 compatibility (:pr:`3494`) `James Bourbeau`_
- Register cuML serializers (:pr:`3485`) `John Kirkham`_
- Check exact equality for worker state (:pr:`3483`) `Brett Naul`_
- Serialize 1-D, contiguous, ``uint8`` CUDA frames (:pr:`3475`) `John Kirkham`_
- Update NumPy array serialization to handle non-contiguous slices (:pr:`3474`) `James Bourbeau`_
- Propose fix for collection based resources docs (:pr:`3480`) `Chris Roat`_
- Remove ``--verbose`` flag from CI runs (:pr:`3484`) `Matthew Rocklin`_
- Do not duplicate messages in scheduler report (:pr:`3477`) `Jakub Beránek`_
- Register Dask cuDF serializers (:pr:`3478`) `John Kirkham`_
- Add support for Python 3.8 (:pr:`3249`) `James Bourbeau`_
- Add last seen column to worker table and highlight errant workers (:pr:`3468`) `kaelgreco`_
- Change default value of ``local_directory`` from empty string to ``None`` (:pr:`3441`) `condoratberlin`_
- Clear old docs (:pr:`3458`) `Matthew Rocklin`_
- Change default multiprocessing behavior to spawn (:pr:`3461`) `Matthew Rocklin`_
- Split dashboard host on additional slashes to handle inproc (:pr:`3466`) `Jacob Tomlinson`_
- Update ``locality.rst`` (:pr:`3470`) `Dustin Tindall`_
- Minor ``gen.Return`` cleanup (:pr:`3469`) `James Bourbeau`_
- Update comparison logic for worker state (:pr:`3321`) `rockwellw`_
- Update minimum ``tblib`` version to 1.6.0 (:pr:`3451`) `James Bourbeau`_
- Add total row to workers plot in dashboard (:pr:`3464`) `Julia Signell`_
- Workaround ``RecursionError`` on profile data (:pr:`3455`) `Tom Augspurger`_
- Include code and summary in performance report (:pr:`3462`) `Matthew Rocklin`_
- Skip ``test_open_close_many_workers`` on Python 3.6 (:pr:`3459`) `Matthew Rocklin`_
- Support serializing/deserializing ``rmm.DeviceBuffer`` s (:pr:`3442`) `John Kirkham`_
- Always add new ``TaskGroup`` to ``TaskPrefix`` (:pr:`3322`) `James Bourbeau`_
- Rerun ``black`` on the code base (:pr:`3444`) `John Kirkham`_
- Ensure ``__causes__`` s of exceptions raised on workers are serialized (:pr:`3430`) `Alex Adamson`_
- Adjust ``numba.cuda`` import and add check (:pr:`3446`) `John Kirkham`_
- Fix name of Numba serialization test (:pr:`3447`) `John Kirkham`_
- Checks for command parameters in ``ssh2`` (:pr:`3078`) `Peter Andreas Entschev`_
- Update ``worker_kwargs`` description in ``LocalCluster`` constructor (:pr:`3438`) `James Bourbeau`_
- Ensure scheduler updates task and worker states after successful worker data deletion (:pr:`3401`) `James Bourbeau`_
- Avoid ``loop=`` keyword in asyncio coordination primitives (:pr:`3437`) `Matthew Rocklin`_
- Call pip as a module to avoid warnings (:pr:`3436`) `Cyril Shcherbin`_
- Add documentation of parameters in coordination primitives (:pr:`3434`) `Søren Fuglede Jørgensen`_
- Replace ``tornado.locks`` with asyncio for Events/Locks/Conditions/Semaphore (:pr:`3397`) `Matthew Rocklin`_
- Remove object from class hierarchy (:pr:`3432`) `Anderson Banihirwe`_
- Add ``dashboard_link`` property to ``Client`` (:pr:`3429`) `Jacob Tomlinson`_
- Allow memory monitor to evict data more aggressively (:pr:`3424`) `fjetter`_
- Make ``_get_ip`` return an IP address when defaulting (:pr:`3418`) `Pierre Glaser`_
- Support version checking with older versions of Dask (:pr:`3390`) `Igor Gotlibovych`_
- Add Mac OS build to CI (:pr:`3358`) `James Bourbeau`_


2.10.0 - 2020-01-28
-------------------

- Fixed ``ZeroDivisionError`` in dashboard when no workers were present (:pr:`3407`) `James Bourbeau`_
- Respect the ``dashboard-prefix`` when redirecting from the root (:pr:`3387`) `Chrysostomos Nanakos`_
- Allow enabling / disabling work-stealing after the cluster has started (:pr:`3410`) `John Kirkham`_
- Support ``*args`` and ``**kwargs`` in offload (:pr:`3392`) `Matthew Rocklin`_
- Add lifecycle hooks to SchedulerPlugin (:pr:`3391`) `Matthew Rocklin`_


2.9.3 - 2020-01-17
------------------

- Raise ``RuntimeError`` if no running loop (:pr:`3385`) `James Bourbeau`_
- Fix ``get_running_loop`` import (:pr:`3383`) `James Bourbeau`_
- Get JavaScript document location instead of window and handle proxied url (:pr:`3382`) `Jacob Tomlinson`_


2.9.2 - 2020-01-16
------------------

- Move Windows CI to GitHub Actions (:pr:`3373`) `Jacob Tomlinson`_
- Add client join and leave hooks (:pr:`3371`) `Jacob Tomlinson`_
- Add cluster map dashboard (:pr:`3361`) `Jacob Tomlinson`_
- Close connection comm on retry (:pr:`3365`) `James Bourbeau`_
- Fix scheduler state in case of worker name collision (:pr:`3366`) `byjott`_
- Add ``--worker-class`` option to ``dask-worker`` CLI (:pr:`3364`) `James Bourbeau`_
- Remove ``locale`` check that fails on OS X (:pr:`3360`) `Jacob Tomlinson`_
- Rework version checking (:pr:`2627`) `Matthew Rocklin`_
- Add websocket scheduler plugin (:pr:`3335`) `Jacob Tomlinson`_
- Return task in ``dask-worker`` ``on_signal`` function (:pr:`3354`) `James Bourbeau`_
- Fix failures on mixed integer/string worker names (:pr:`3352`) `Benedikt Reinartz`_
- Avoid calling ``nbytes`` multiple times when sending data (:pr:`3349`) `Markus Mohrhard`_
- Avoid setting event loop policy if within IPython kernel and no running event loop (:pr:`3336`) `Mana Borwornpadungkitti`_
- Relax intermittent failing ``test_profile_server`` (:pr:`3346`) `Matthew Rocklin`_


2.9.1 - 2019-12-27
------------------

-  Add lock around dumps_function cache (:pr:`3337`) `Matthew Rocklin`_
-  Add setuptools to dependencies (:pr:`3320`) `James Bourbeau`_
-  Use TaskPrefix.name in Graph layout (:pr:`3328`) `Matthew Rocklin`_
-  Add missing `"` in performance report example (:pr:`3329`) `John Kirkham`_
-  Add performance report docs and color definitions to docs (:pr:`3325`) `Benjamin Zaitlen`_
-  Switch startstops to dicts and add worker name to transfer (:pr:`3319`) `Jacob Tomlinson`_
-  Add plugin entry point for out-of-tree comms library (:pr:`3305`) `Patrick Sodré`_
-  All scheduler task states in prometheus (:pr:`3307`) `fjetter`_
-  Use worker name in logs (:pr:`3309`) `Stephan Erb`_
-  Add TaskGroup and TaskPrefix scheduler state (:pr:`3262`)  `Matthew Rocklin`_
-  Update latencies with heartbeats (:pr:`3310`) `fjetter`_
-  Update inlining Futures in task graph in Client._graph_to_futures (:pr:`3303`) `James Bourbeau`_
-  Use hostname as default IP address rather than localhost (:pr:`3308`) `Matthew Rocklin`_
-  Clean up flaky test_nanny_throttle (:pr:`3295`) `Tom Augspurger`_
-  Add lock to scheduler for sensitive operations (:pr:`3259`) `Matthew Rocklin`_
-  Log address for each of the Scheduler listerners (:pr:`3306`) `Matthew Rocklin`_
-  Make ConnectionPool.close asynchronous (:pr:`3304`) `Matthew Rocklin`_


2.9.0 - 2019-12-06
------------------

- Add ``dask-spec`` CLI tool (:pr:`3090`) `Matthew Rocklin`_
- Connectionpool: don't hand out closed connections (:pr:`3301`) `byjott`_
- Retry operations on network issues (:pr:`3294`) `byjott`_
- Skip ``Security.temporary()`` tests if cryptography not installed (:pr:`3302`) `James Bourbeau`_
- Support multiple listeners in the scheduler (:pr:`3288`) `Matthew Rocklin`_
- Updates RMM comment to the correct release (:pr:`3299`) `John Kirkham`_
- Add title to ``performance_report`` (:pr:`3298`) `Matthew Rocklin`_
- Forgot to fix slow test (:pr:`3297`) `Benjamin Zaitlen`_
- Update ``SSHCluster`` docstring parameters (:pr:`3296`) `James Bourbeau`_
- ``worker.close()`` awaits ``batched_stream.close()`` (:pr:`3291`) `Mads R. B. Kristensen`_
- Fix asynchronous listener in UCX (:pr:`3292`) `Benjamin Zaitlen`_
- Avoid repeatedly adding deps to already in memory stack (:pr:`3293`) `James Bourbeau`_
- xfail ucx empty object typed dataframe (:pr:`3279`) `Benjamin Zaitlen`_
- Fix ``distributed.wait`` documentation (:pr:`3289`) `Tom Rochette`_
- Move Python 3 syntax tests into main tests (:pr:`3281`) `Matthew Rocklin`_
- xfail ``test_workspace_concurrency`` for Python 3.6 (:pr:`3283`) `Matthew Rocklin`_
- Add ``performance_report`` context manager for static report generation (:pr:`3282`) `Matthew Rocklin`_
- Update function serialization caches with custom LRU class (:pr:`3260`) `James Bourbeau`_
- Make ``Listener.start`` asynchronous (:pr:`3278`) `Matthew Rocklin`_
- Remove ``dask-submit`` and ``dask-remote`` (:pr:`3280`) `Matthew Rocklin`_
- Worker profile server (:pr:`3274`) `Matthew Rocklin`_
- Improve bandwidth workers plot (:pr:`3273`) `Matthew Rocklin`_
- Make profile coroutines consistent between ``Scheduler`` and ``Worker`` (:pr:`3277`) `Matthew Rocklin`_
- Enable saving profile information from server threads (:pr:`3271`) `Matthew Rocklin`_
- Remove memory use plot (:pr:`3269`) `Matthew Rocklin`_
- Add offload size to configuration (:pr:`3270`) `Matthew Rocklin`_
- Fix layout scaling on profile plots (:pr:`3268`) `Jacob Tomlinson`_
- Set ``x_range`` in CPU plot based on the number of threads (:pr:`3266`) `Matthew Rocklin`_
- Use base-2 values for byte-valued axes in dashboard (:pr:`3267`) `Matthew Rocklin`_
- Robust gather in case of connection failures (:pr:`3246`) `fjetter`_
- Use ``DeviceBuffer`` from newer RMM releases (:pr:`3261`) `John Kirkham`_
- Fix dev requirements for pytest (:pr:`3264`) `Elliott Sales de Andrade`_
- Add validate options to configuration (:pr:`3258`) `Matthew Rocklin`_


2.8.1 - 2019-11-22
------------------

- Fix hanging worker when the scheduler leaves (:pr:`3250`) `Tom Augspurger`_
- Fix NumPy writeable serialization bug (:pr:`3253`) `James Bourbeau`_
- Skip ``numba.cuda`` tests if CUDA is not available (:pr:`3255`) `Peter Andreas Entschev`_
- Add new dashboard plot for memory use by key (:pr:`3243`) `Matthew Rocklin`_
- Fix ``array.shape()`` -> ``array.shape`` (:pr:`3247`) `Jed Brown`_
- Fixed typos in ``pubsub.py`` (:pr:`3244`) `He Jia`_
- Fixed cupy array going out of scope (:pr:`3240`) `Mads R. B. Kristensen`_
- Remove ``gen.coroutine`` usage in scheduler (:pr:`3242`) `Jim Crist-Harif`_
- Use ``inspect.isawaitable`` where relevant (:pr:`3241`) `Jim Crist-Harif`_


2.8.0 - 2019-11-14
------------------

-  Add UCX config values (:pr:`3135`) `Matthew Rocklin`_
-  Relax test_MultiWorker (:pr:`3210`) `Matthew Rocklin`_
-  Avoid ucp.init at import time (:pr:`3211`) `Matthew Rocklin`_
-  Clean up rpc to avoid intermittent test failure (:pr:`3215`) `Matthew Rocklin`_
-  Respect protocol if given to Scheduler (:pr:`3212`) `Matthew Rocklin`_
-  Use legend_field= keyword in bokeh plots (:pr:`3218`) `Matthew Rocklin`_
-  Cache psutil.Process object in Nanny (:pr:`3207`) `Matthew Rocklin`_
-  Replace gen.sleep with asyncio.sleep (:pr:`3208`) `Matthew Rocklin`_
-  Avoid offloading serialization for small messages (:pr:`3224`) `Matthew Rocklin`_
-  Add desired_workers metric (:pr:`3221`) `Gabriel Sailer`_
-  Fail fast when importing distributed.comm.ucx (:pr:`3228`) `Matthew Rocklin`_
-  Add module name to Future repr (:pr:`3231`) `Matthew Rocklin`_
-  Add name to Pub/Sub repr (:pr:`3235`) `Matthew Rocklin`_
-  Import CPU_COUNT from dask.system (:pr:`3199`) `James Bourbeau`_
-  Efficiently serialize zero strided NumPy arrays (:pr:`3180`) `James Bourbeau`_
-  Cache function deserialization in workers (:pr:`3234`) `Matthew Rocklin`_
-  Respect ordering of futures in futures_of (:pr:`3236`) `Matthew Rocklin`_
-  Bump dask dependency to 2.7.0 (:pr:`3237`) `James Bourbeau`_
-  Avoid setting inf x_range (:pr:`3229`) `rockwellw`_
-  Clear task stream based on recent behavior (:pr:`3200`) `Matthew Rocklin`_
-  Use the percentage field for profile plots (:pr:`3238`) `Matthew Rocklin`_

2.7.0 - 2019-11-08
------------------

This release drops support for Python 3.5

-  Adds badges to README.rst [skip ci] (:pr:`3152`) `James Bourbeau`_
-  Don't overwrite `self.address` if it is present (:pr:`3153`) `Gil Forsyth`_
-  Remove outdated references to debug scheduler and worker bokeh pages. (:pr:`3160`) `darindf`_
-  Update CONTRIBUTING.md (:pr:`3159`) `Jacob Tomlinson`_
-  Add Prometheus metric for a worker's executing tasks count (:pr:`3163`) `darindf`_
-  Update Prometheus documentation (:pr:`3165`) `darindf`_
-  Fix Numba serialization when strides is None (:pr:`3166`) `Peter Andreas Entschev`_
-  Await cluster in Adaptive.recommendations (:pr:`3168`) `Simon Boothroyd`_
-  Support automatic TLS (:pr:`3164`) `Jim Crist`_
-  Avoid swamping high-memory workers with data requests (:pr:`3071`) `Tom Augspurger`_
-  Update UCX variables to use sockcm by default (:pr:`3177`) `Peter Andreas Entschev`_
-  Get protocol in Nanny/Worker from scheduler address (:pr:`3175`) `Peter Andreas Entschev`_
-  Add worker and tasks state for Prometheus data collection (:pr:`3174`) `darindf`_
-  Use async def functions for offload to/from_frames (:pr:`3171`) `Mads R. B. Kristensen`_
-  Subprocesses inherit the global dask config (:pr:`3192`) `Mads R. B. Kristensen`_
-  XFail test_open_close_many_workers (:pr:`3194`) `Matthew Rocklin`_
-  Drop Python 3.5 (:pr:`3179`) `James Bourbeau`_
-  UCX: avoid double init after fork (:pr:`3178`) `Mads R. B. Kristensen`_
-  Silence warning when importing while offline (:pr:`3203`) `James A. Bednar`_
-  Adds docs to Client methods for resources, actors, and traverse (:pr:`2851`) `IPetrik`_
-  Add test for concurrent scatter operations (:pr:`2244`) `Matthew Rocklin`_
-  Expand async docs (:pr:`2293`) `Dave Hirschfeld`_
-  Add PatchedDeviceArray to drop stride attribute for cupy<7.0 (:pr:`3198`) `Richard J Zamora`_

2.6.0 - 2019-10-15
------------------

- Refactor dashboard module (:pr:`3138`) `Jacob Tomlinson`_
- Use ``setuptools.find_packages`` in ``setup.py`` (:pr:`3150`) `Matthew Rocklin`_
- Move death timeout logic up to ``Node.start`` (:pr:`3115`) `Matthew Rocklin`_
- Only include metric in ``WorkerTable`` if it is a scalar (:pr:`3140`) `Matthew Rocklin`_
- Add ``Nanny(config={...})`` keyword (:pr:`3134`) `Matthew Rocklin`_
- Xfail ``test_worksapce_concurrency`` on Python 3.6 (:pr:`3132`) `Matthew Rocklin`_
- Extend Worker plugin API with transition method (:pr:`2994`) `matthieubulte`_
- Raise exception if the user passes in unused keywords to ``Client`` (:pr:`3117`) `Jonathan De Troye`_
- Move new ``SSHCluster`` to top level (:pr:`3128`) `Matthew Rocklin`_
- Bump dask dependency (:pr:`3124`) `Jim Crist`_


2.5.2 - 2019-10-04
------------------

-  Make dask-worker close quietly when given sigint signal (:pr:`3116`) `Matthew Rocklin`_
-  Replace use of tornado.gen with asyncio in dask-worker (:pr:`3114`) `Matthew Rocklin`_
-  UCX: allocate CUDA arrays using RMM and Numba (:pr:`3109`) `Mads R. B. Kristensen`_
-  Support calling `cluster.scale` as async method (:pr:`3110`) `Jim Crist`_
-  Identify lost workers in SpecCluster based on address not name (:pr:`3088`) `James Bourbeau`_
-  Add Client.shutdown method (:pr:`3106`) `Matthew Rocklin`_
-  Collect worker-worker and type bandwidth information (:pr:`3094`) `Matthew Rocklin`_
-  Send noise over the wire to keep dask-ssh connection alive (:pr:`3105`) `Gil Forsyth`_
-  Retry scheduler connect multiple times (:pr:`3104`) `Jacob Tomlinson`_
-  Add favicon of logo to the dashboard (:pr:`3095`) `James Bourbeau`_
-  Remove utils.py functions for their dask/utils.py equivalents (:pr:`3042`) `Matthew Rocklin`_
-  Lower default bokeh log level (:pr:`3087`) `Philipp Rudiger`_
-  Check if self.cluster.scheduler is a local scheduler (:pr:`3099`) `Jacob Tomlinson`_


2.5.1 - 2019-09-27
------------------

-   Support clusters that don't have .security or ._close methods (:pr:`3100`) `Matthew Rocklin`_


2.5.0 - 2019-09-27
------------------

-  Use the new UCX Python bindings (:pr:`3059`) `Mads R. B. Kristensen`_
-  Fix worker preload config (:pr:`3027`) `byjott`_
-  Fix widget with spec that generates multiple workers (:pr:`3067`) `Loïc Estève`_
-  Make Client.get_versions async friendly (:pr:`3064`) `Jacob Tomlinson`_
-  Add configuation option for longer error tracebacks (:pr:`3086`) `Daniel Farrell`_
-  Have Client get Security from passed Cluster (:pr:`3079`) `Matthew Rocklin`_
-  Respect Cluster.dashboard_link in Client._repr_html_ if it exists (:pr:`3077`) `Matthew Rocklin`_
-  Add monitoring with dask cluster docs (:pr:`3072`) `Arpit Solanki`_
-  Protocol of cupy and numba handles serialization exclusively  (:pr:`3047`) `Mads R. B. Kristensen`_
-  Allow specification of worker type in SSHCLuster (:pr:`3061`) `Jacob Tomlinson`_
-  Use Cluster.scheduler_info for workers= value in repr (:pr:`3058`) `Matthew Rocklin`_
-  Allow SpecCluster to scale by memory and cores (:pr:`3057`) `Matthew Rocklin`_
-  Allow full script in preload inputs (:pr:`3052`) `Matthew Rocklin`_
-  Check multiple cgroups dirs, ceil fractional cpus (:pr:`3056`) `Jim Crist`_
-  Add blurb about disabling work stealing (:pr:`3055`) `Chris White`_


2.4.0 - 2019-09-13
------------------

- Remove six (:pr:`3045`) `Matthew Rocklin`_
- Add missing test data to sdist tarball (:pr:`3050`) `Elliott Sales de Andrade`_
- Use mock from unittest standard library (:pr:`3049`) `Elliott Sales de Andrade`_
- Use cgroups resource limits to determine default threads and memory (:pr:`3039`) `Jim Crist`_
- Move task deserialization to immediately before task execution (:pr:`3015`) `James Bourbeau`_
- Drop joblib shim module in distributed (:pr:`3040`) `John Kirkham`_
- Redirect configuration doc page (:pr:`3038`) `Matthew Rocklin`_
- Support ``--name 0`` and ``--nprocs`` keywords in dask-worker cli (:pr:`3037`) `Matthew Rocklin`_
- Remove lost workers from ``SpecCluster.workers`` (:pr:`2990`) `Guillaume Eynard-Bontemps`_
- Clean up ``test_local.py::test_defaults`` (:pr:`3017`) `Matthew Rocklin`_
- Replace print statement in ``Queue.__init__`` with debug message (:pr:`3035`) `Mikhail Akimov`_
- Set the ``x_range`` limit of the Meory utilization plot to memory-limit (:pr:`3034`) `Matthew Rocklin`_
- Rely on cudf codebase for cudf serialization (:pr:`2998`) `Benjamin Zaitlen`_
- Add fallback html repr for Cluster (:pr:`3023`) `Jim Crist`_
- Add support for zstandard compression to comms (:pr:`2970`) `Abael He`_
- Avoid collision when using ``os.environ`` in ``dashboard_link`` (:pr:`3021`) `Matthew Rocklin`_
- Fix ``ConnectionPool`` limit handling (:pr:`3005`) `byjott`_
- Support Spec jobs that generate multiple workers (:pr:`3013`) `Matthew Rocklin`_
- Tweak ``Logs`` styling (:pr:`3012`) `Jim Crist`_
- Better name for cudf deserialization function name (:pr:`3008`) `Benjamin Zaitlen`_
- Make ``spec.ProcessInterface`` a valid no-op worker (:pr:`3004`) `Matthew Rocklin`_
- Return dictionaries from ``new_worker_spec`` rather than name/worker pairs (:pr:`3000`) `Matthew Rocklin`_
- Fix minor typo in documentation (:pr:`3002`) `Mohammad Noor`_
- Permit more keyword options when scaling with cores and memory (:pr:`2997`) `Matthew Rocklin`_
- Add ``cuda_ipc`` to UCX environment for NVLink (:pr:`2996`) `Benjamin Zaitlen`_
- Add ``threads=`` and ``memory=`` to Cluster and Client reprs (:pr:`2995`) `Matthew Rocklin`_
- Fix PyNVML initialization (:pr:`2993`) `Richard J Zamora`_


2.3.2 - 2019-08-23
------------------

-  Skip exceptions in startup information (:pr:`2991`) `Jacob Tomlinson`_


2.3.1 - 2019-08-22
------------------

-  Add support for separate external address for SpecCluster scheduler (:pr:`2963`) `Jacob Tomlinson`_
-  Defer cudf serialization/deserialization to that library (:pr:`2881`) `Benjamin Zaitlen`_
-  Workaround for hanging test now calls ucp.fin() (:pr:`2967`) `Mads R. B. Kristensen`_
-  Remove unnecessary bullet point (:pr:`2972`) `Pav A`_
-  Directly import progress from diagnostics.progressbar (:pr:`2975`) `Matthew Rocklin`_
-  Handle buffer protocol objects in ensure_bytes (:pr:`2969`) `Tom Augspurger`_
-  Fix documentatation syntax and tree (:pr:`2981`) `Pav A`_
-  Improve get_ip_interface error message when interface does not exist (:pr:`2964`) `Loïc Estève`_
-  Add cores= and memory= keywords to scale (:pr:`2974`) `Matthew Rocklin`_
-  Make workers robust to bad custom metrics (:pr:`2984`) `Matthew Rocklin`_


2.3.0 - 2019-08-16
------------------

- Except all exceptions when checking ``pynvml`` (:pr:`2961`) `Matthew Rocklin`_
- Pass serialization down through small base collections (:pr:`2948`) `Peter Andreas Entschev`_
- Use ``pytest.warning(Warning)`` rather than ``Exception`` (:pr:`2958`) `Matthew Rocklin`_
- Allow ``server_kwargs`` to override defaults in dashboard (:pr:`2955`) `Bruce Merry`_
- Update ``utils_perf.py`` (:pr:`2954`) `Shayan Amani`_
- Normalize names with ``str`` in ``retire_workers`` (:pr:`2949`) `Matthew Rocklin`_
- Update ``client.py`` (:pr:`2951`) `Shayan Amani`_
- Add ``GPUCurrentLoad`` dashboard plots (:pr:`2944`) `Matthew Rocklin`_
- Pass GPU diagnostics from worker to scheduler (:pr:`2932`) `Matthew Rocklin`_
- Import from ``collections.abc`` (:pr:`2938`) `Jim Crist`_
- Fixes Worker docstring formatting (:pr:`2939`) `James Bourbeau`_
- Redirect setup docs to docs.dask.org (:pr:`2936`) `Matthew Rocklin`_
- Wrap offload in ``gen.coroutine`` (:pr:`2934`) `Matthew Rocklin`_
- Change ``TCP.close`` to a coroutine to avoid task pending warning (:pr:`2930`) `Matthew Rocklin`_
- Fixup black string normalization (:pr:`2929`) `Jim Crist`_
- Move core functionality from ``SpecCluster`` to ``Cluster`` (:pr:`2913`) `Matthew Rocklin`_
- Add aenter/aexit protocols to ``ProcessInterface`` (:pr:`2927`) `Matthew Rocklin`_
- Add real-time CPU utilization plot to dashboard (:pr:`2922`) `Matthew Rocklin`_
- Always kill processes in clean tests, even if we don't check (:pr:`2924`) `Matthew Rocklin`_
- Add timeouts to processes in SSH tests (:pr:`2925`) `Matthew Rocklin`_
- Add documentation around ``spec.ProcessInterface`` (:pr:`2923`) `Matthew Rocklin`_
- Cleanup async warnings in tests (:pr:`2920`) `Matthew Rocklin`_
- Give 404 when requesting nonexistent tasks or workers (:pr:`2921`) `Martin Durant`_
- Raise informative warning when rescheduling an unknown task (:pr:`2916`) `James Bourbeau`_
- Fix docstring (:pr:`2917`) `Martin Durant`_
- Add keep-alive message between worker and scheduler (:pr:`2907`) `Matthew Rocklin`_
- Rewrite ``Adaptive``/``SpecCluster`` to support slowly arriving workers (:pr:`2904`) `Matthew Rocklin`_
- Call heartbeat rather than reconnect on disconnection (:pr:`2906`) `Matthew Rocklin`_


2.2.0 - 2019-07-31
------------------

-  Respect security configuration in LocalCluster (:pr:`2822`) `Russ Bubley`_
-  Add Nanny to worker docs (:pr:`2826`) `Christian Hudon`_
-  Don't make False add-keys report to scheduler (:pr:`2421`) `tjb900`_
-  Include type name in SpecCluster repr (:pr:`2834`) `Jacob Tomlinson`_
-  Extend prometheus metrics endpoint (:pr:`2833`) `Gabriel Sailer`_
-  Add alternative SSHCluster implementation (:pr:`2827`) `Matthew Rocklin`_
-  Dont reuse closed worker in get_worker (:pr:`2841`) `Pierre Glaser`_
-  SpecCluster: move init logic into start (:pr:`2850`) `Jacob Tomlinson`_
-  Document distributed.Reschedule in API docs (:pr:`2860`) `James Bourbeau`_
-  Add fsspec to installation of test builds (:pr:`2859`) `Martin Durant`_
-  Make await/start more consistent across Scheduler/Worker/Nanny (:pr:`2831`) `Matthew Rocklin`_
-  Add cleanup fixture for asyncio tests (:pr:`2866`) `Matthew Rocklin`_
-  Use only remote connection to scheduler in Adaptive (:pr:`2865`) `Matthew Rocklin`_
-  Add Server.finished async function  (:pr:`2864`) `Matthew Rocklin`_
-  Align text and remove bullets in Client HTML repr (:pr:`2867`) `Matthew Rocklin`_
-  Test dask-scheduler --idle-timeout flag (:pr:`2862`) `Matthew Rocklin`_
-  Remove ``Client.upload_environment`` (:pr:`2877`) `Jim Crist`_
-  Replace gen.coroutine with async/await in core (:pr:`2871`) `Matthew Rocklin`_
-  Forcefully kill all processes before each test (:pr:`2882`) `Matthew Rocklin`_
-  Cleanup Security class and configuration (:pr:`2873`) `Jim Crist`_
-  Remove unused variable in SpecCluster scale down (:pr:`2870`) `Jacob Tomlinson`_
-  Add SpecCluster ProcessInterface (:pr:`2874`) `Jacob Tomlinson`_
-  Add Log(str) and Logs(dict) classes for nice HTML reprs (:pr:`2875`) `Jacob Tomlinson`_
-  Pass Client._asynchronous to Cluster._asynchronous (:pr:`2890`) `Matthew Rocklin`_
-  Add default logs method to Spec Cluster (:pr:`2889`) `Matthew Rocklin`_
-  Add processes keyword back into clean (:pr:`2891`) `Matthew Rocklin`_
-  Update black (:pr:`2901`) `Matthew Rocklin`_
-  Move Worker.local_dir attribute to Worker.local_directory (:pr:`2900`) `Matthew Rocklin`_
-  Link from TapTools to worker info pages in dashboard (:pr:`2894`) `Matthew Rocklin`_
-  Avoid exception in Client._ensure_connected if closed (:pr:`2893`) `Matthew Rocklin`_
-  Convert Pythonic kwargs to CLI Keywords for SSHCluster (:pr:`2898`) `Matthew Rocklin`_
-  Use kwargs in CLI (:pr:`2899`) `Matthew Rocklin`_
-  Name SSHClusters by providing name= keyword to SpecCluster (:pr:`2903`) `Matthew Rocklin`_
-  Request feed of worker information from Scheduler to SpecCluster (:pr:`2902`) `Matthew Rocklin`_
-  Clear out compatibillity file (:pr:`2896`) `Matthew Rocklin`_
-  Remove future imports (:pr:`2897`) `Matthew Rocklin`_
-  Use click's show_default=True in relevant places (:pr:`2838`) `Christian Hudon`_
-  Close workers more gracefully (:pr:`2905`) `Matthew Rocklin`_
-  Close workers gracefully with --lifetime keywords (:pr:`2892`) `Matthew Rocklin`_
-  Add closing <li> tags to Client._repr_html_ (:pr:`2911`) `Matthew Rocklin`_
-  Add endline spacing in Logs._repr_html_ (:pr:`2912`) `Matthew Rocklin`_

2.1.0 - 2019-07-08
------------------

- Fix typo that prevented error message (:pr:`2825`) `Russ Bubley`_
- Remove ``dask-mpi`` (:pr:`2824`) `Matthew Rocklin`_
- Updates to use ``update_graph`` in task journey docs (:pr:`2821`) `James Bourbeau`_
- Fix Client repr with ``memory_info=None`` (:pr:`2816`) `Matthew Rocklin`_
- Fix case where key, rather than ``TaskState``, could end up in ``ts.waiting_on`` (:pr:`2819`) `tjb900`_
- Use Keyword-only arguments (:pr:`2814`) `Matthew Rocklin`_
- Relax check for worker references in cluster context manager (:pr:`2813`) `Matthew Rocklin`_
- Add HTTPS support for the dashboard (:pr:`2812`) `Jim Crist`_
- Use ``dask.utils.format_bytes`` (:pr:`2810`) `Tom Augspurger`_


2.0.1 - 2019-06-26
------------------

We neglected to include ``python_requires=`` in our setup.py file, resulting in
confusion for Python 2 users who erroneously get packages for 2.0.0.
This is fixed in 2.0.1 and we have removed the 2.0.0 files from PyPI.

-  Add python_requires entry to setup.py (:pr:`2807`) `Matthew Rocklin`_
-  Correctly manage tasks beyond deque limit in TaskStream plot (:pr:`2797`) `Matthew Rocklin`_
-  Fix diagnostics page for memory_limit=None (:pr:`2770`) `Brett Naul`_


2.0.0 - 2019-06-25
------------------

-  **Drop support for Python 2**
-  Relax warnings before release (:pr:`2796`) `Matthew Rocklin`_
-  Deprecate --bokeh/--no-bokeh CLI (:pr:`2800`) `Tom Augspurger`_
-  Typo in bokeh service_kwargs for dask-worker (:pr:`2783`) `Tom Augspurger`_
-  Update command line cli options docs (:pr:`2794`) `James Bourbeau`_
-  Remove "experimental" from TLS docs (:pr:`2793`) `James Bourbeau`_
-  Add warnings around ncores= keywords (:pr:`2791`) `Matthew Rocklin`_
-  Add --version option to scheduler and worker CLI (:pr:`2782`) `Tom Augspurger`_
-  Raise when workers initialization times out (:pr:`2784`) `Tom Augspurger`_
-  Replace ncores with nthreads throughout codebase (:pr:`2758`) `Matthew Rocklin`_
-  Add unknown pytest markers (:pr:`2764`) `Tom Augspurger`_
-  Delay lookup of allowed failures. (:pr:`2761`) `Tom Augspurger`_
-  Change address -> worker in ColumnDataSource for nbytes plot (:pr:`2755`) `Matthew Rocklin`_
-  Remove module state in Prometheus Handlers (:pr:`2760`) `Matthew Rocklin`_
-  Add stress test for UCX (:pr:`2759`) `Matthew Rocklin`_
-  Add nanny logs (:pr:`2744`) `Tom Augspurger`_
-  Move some of the adaptive logic into the scheduler (:pr:`2735`) `Matthew Rocklin`_
-  Add SpecCluster.new_worker_spec method (:pr:`2751`) `Matthew Rocklin`_
-  Worker dashboard fixes (:pr:`2747`) `Matthew Rocklin`_
-  Add async context managers to scheduler/worker classes (:pr:`2745`) `Matthew Rocklin`_
-  Fix the resource key representation before sending graphs (:pr:`2733`) `Michael Spiegel`_
-  Allow user to configure whether workers are daemon. (:pr:`2739`) `Caleb`_
-  Pin pytest >=4 with pip in appveyor and python 3.5 (:pr:`2737`) `Matthew Rocklin`_
-  Add Experimental UCX Comm (:pr:`2591`) `Ben Zaitlen`_ `Tom Augspurger`_ `Matthew Rocklin`_
-  Close nannies gracefully (:pr:`2731`) `Matthew Rocklin`_
-  add kwargs to progressbars (:pr:`2638`) `Manuel Garrido`_
-  Add back LocalCluster.__repr__. (:pr:`2732`) `Loïc Estève`_
-  Move bokeh module to dashboard (:pr:`2724`) `Matthew Rocklin`_
-  Close clusters at exit (:pr:`2730`) `Matthew Rocklin`_
-  Add SchedulerPlugin TaskState example (:pr:`2622`) `Matt Nicolls`_
-  Add SpecificationCluster (:pr:`2675`) `Matthew Rocklin`_
-  Replace register_worker_callbacks with worker plugins (:pr:`2453`) `Matthew Rocklin`_
-  Proxy worker dashboards from scheduler dashboard (:pr:`2715`) `Ben Zaitlen`_
-  Add docstring to Scheduler.check_idle_saturated (:pr:`2721`) `Matthew Rocklin`_
-  Refer to LocalCluster in Client docstring (:pr:`2719`) `Matthew Rocklin`_
-  Remove special casing of Scikit-Learn BaseEstimator serialization (:pr:`2713`) `Matthew Rocklin`_
-  Fix two typos in Pub class docstring (:pr:`2714`) `Magnus Nord`_
-  Support uploading files with multiple modules (:pr:`2587`) `Sam Grayson`_
-  Change the main workers bokeh page to /status (:pr:`2689`) `Ben Zaitlen`_
-  Cleanly stop periodic callbacks in Client (:pr:`2705`) `Matthew Rocklin`_
-  Disable pan tool for the Progress, Byte Stored and Tasks Processing plot (:pr:`2703`) `Mathieu Dugré`_
-  Except errors in Nanny's memory monitor if process no longer exists (:pr:`2701`) `Matthew Rocklin`_
-  Handle heartbeat when worker has just left (:pr:`2702`) `Matthew Rocklin`_
-  Modify styling of histograms for many-worker dashboard plots (:pr:`2695`) `Mathieu Dugré`_
-  Add method to wait for n workers before continuing (:pr:`2688`) `Daniel Farrell`_
-  Support computation on delayed(None) (:pr:`2697`)  `Matthew Rocklin`_
-  Cleanup localcluster (:pr:`2693`)  `Matthew Rocklin`_
-  Use 'temporary-directory' from dask.config for Worker's directory (:pr:`2654`) `Matthew Rocklin`_
-  Remove support for Iterators and Queues (:pr:`2671`) `Matthew Rocklin`_


1.28.1 - 2019-05-13
-------------------

This is a small bugfix release due to a config change upstream.

-  Use config accessor method for "scheduler-address" (:pr:`2676`) `James Bourbeau`_


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
.. _`Michael Spiegel`: https://github.com/Spiegel0
.. _`Caleb`: https://github.com/calebho
.. _`Ben Zaitlen`: https://github.com/quasiben
.. _`Benjamin Zaitlen`: https://github.com/quasiben
.. _`Manuel Garrido`: https://github.com/manugarri
.. _`Magnus Nord`: https://github.com/magnunor
.. _`Sam Grayson`: https://github.com/charmoniumQ
.. _`Mathieu Dugré`: https://github.com/mathdugre
.. _`Christian Hudon`: https://github.com/chrish42
.. _`Gabriel Sailer`: https://github.com/sublinus
.. _`Pierre Glaser`: https://github.com/pierreglase
.. _`Shayan Amani`: https://github.com/SHi-ON
.. _`Pav A`: https://github.com/rs2
.. _`Mads R. B. Kristensen`: https://github.com/madsbk
.. _`Mikhail Akimov`: https://github.com/roveo
.. _`Abael He`: https://github.com/abaelhe
.. _`byjott`: https://github.com/byjott
.. _`Mohammad Noor`: https://github.com/MdSalih
.. _`Richard J Zamora`: https://github.com/rjzamora
.. _`Arpit Solanki`: https://github.com/arpit1997
.. _`Gil Forsyth`: https://github.com/gforsyth
.. _`Philipp Rudiger`: https://github.com/philippjfr
.. _`Jonathan De Troye`: https://github.com/detroyejr
.. _`matthieubulte`: https://github.com/matthieubulte
.. _`darindf`: https://github.com/darindf
.. _`James A. Bednar`: https://github.com/jbednar
.. _`IPetrik`: https://github.com/IPetrik
.. _`Simon Boothroyd`: https://github.com/SimonBoothroyd
.. _`rockwellw`: https://github.com/rockwellw
.. _`Jed Brown`: https://github.com/jedbrown
.. _`He Jia`: https://github.com/HerculesJack
.. _`Jim Crist-Harif`: https://github.com/jcrist
.. _`fjetter`: https://github.com/fjetter
.. _`Florian Jetter`: https://github.com/fjetter
.. _`Patrick Sodré`: https://github.com/sodre
.. _`Stephan Erb`: https://github.com/StephanErb
.. _`Benedikt Reinartz`: https://github.com/filmor
.. _`Markus Mohrhard`: https://github.com/mmohrhard
.. _`Mana Borwornpadungkitti`: https://github.com/potpath
.. _`Chrysostomos Nanakos`: https://github.com/cnanakos
.. _`Chris Roat`: https://github.com/chrisroat
.. _`Jakub Beránek`: https://github.com/Kobzol
.. _`kaelgreco`: https://github.com/kaelgreco
.. _`Dustin Tindall`: https://github.com/dustindall
.. _`Julia Signell`: https://github.com/jsignell
.. _`Alex Adamson`: https://github.com/aadamson
.. _`Cyril Shcherbin`: https://github.com/shcherbin
.. _`Søren Fuglede Jørgensen`: https://github.com/fuglede
.. _`Igor Gotlibovych`: https://github.com/ig248
.. _`Stan Seibert`: https://github.com/seibert
.. _`Davis Bennett`: https://github.com/d-v-b
.. _`Lucas Rademaker`: https://github.com/lr4d
.. _`Darren Weber`: https://github.com/dazza-codes
.. _`Matthias Urlichs`: https://github.com/smurfix
.. _`Krishan Bhasin`: https://github.com/KrishanBhasin
.. _`Abdulelah Bin Mahfoodh`: https://github.com/abduhbm
.. _`jakirkham`: https://github.com/jakirkham
.. _`Prasun Anand`: https://github.com/prasunanand
.. _`Jonathan J. Helmus`: https://github.com/jjhelmus
.. _`Rami Chowdhury`: https://github.com/necaris
.. _`crusaderky`: https://github.com/crusaderky
.. _`Nicholas Smith`: https://github.com/nsmith-
.. _`Dillon Niederhut`: https://github.com/deniederhut
.. _`Jonas Haag`: https://github.com/jonashaag
.. _`Nils Braun`: https://github.com/nils-braun
.. _`Nick Evans`: https://github.com/nre
.. _`Scott Sanderson`: https://github.com/ssanderson
.. _`Matthias Bussonnier`: https://github.com/Carreau
