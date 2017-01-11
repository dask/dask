1.15.1 - January 11th, 2017
---------------------------

*  Make compatibile with Bokeh 0.12.4 #803
*  Avoid compressing arrays if not helpful  #777
*  Optimize inter-worker data transfer #770 #790
*  Add --local-directory keyword to worker #788
*  Enable workers to arrive to the cluster with their own data.
   Useful if a worker leaves and comes back #785
*  Resolve thread safety bug when using local_client #802
*  Resolve scheduling issues in worker #804


1.15.0 - January 2nd, 2017
--------------------------

*  Major Worker refactor #704
*  Major Scheduler refactor #717 #722 #724 #742 743

*  Add ``check`` (default is ``False``) option to ``Client.get_versions``
   to raise if the versions don't match on client, scheduler & workers #664
*  ``Future.add_done_callback`` executes in separate thread #656
*  Clean up numpy serialization #670
*  Support serialization of Tornado v4.5 coroutines #673
*  Use CPickle instead of Pickle in Python 2 #684
*  Use Forkserver rather than Fork on Unix in Python 3 #687
*  Support abstract resources for per-task constraints #694 #720 #737
*  Add TCP timeouts #697
*  Add embedded Bokeh server to workers #709 #713 #738
*  Add embedded Bokeh server to scheduler #724 #736 #738
*  Add more precise timers for Windows #713
*  Add Versioneer #715
*  Support inter-client channels  #729 #749
*  Scheduler Performance improvements #740 #760
*  Improve load balancing and work stealing #747 #754 #757
*  Run Tornado coroutines on workers
*  Avoid slow sizeof call on Pandas dataframes #758


1.14.3 - November 13th, 2016
----------------------------

*  Remove custom Bokeh export tool that implicitly relied on nodejs #655
*  Clean up scheduler logging #657


1.14.2 - November 11th, 2016
----------------------------

*  Support more numpy dtypes in custom serialization, #627, #630, #636
*  Update Bokeh plots #628
*  Improve spill to disk heuristics #633
*  Add Export tool to Task Stream plot
*  Reverse frame order in loads for very many frames #651
*  Add timeout when waiting on write #653


1.14.0 - November 3rd, 2016
---------------------------

*   Add ``Client.get_versions()`` function to return software and package
    information from the scheduler, workers, and client #595
*   Improved windows support #577 #590 #583 #597
*   Clean up rpc objects explicitly #584
*   Normalize collections against known futures #587
*   Add key= keyword to map to specify keynames #589
*   Custom data serialization #606
*   Refactor the web interface #608 #615 #621
*   Allow user-supplied Executor in Worker #609
*   Pass Worker kwargs through LocalCluster


1.13.3 - October 15th, 2016
---------------------------

*   Schedulers can retire workers cleanly
*   Add Future.add_done_callback for concurrent.futures compatibility
*   Update web interface to be consistent with Bokeh 0.12.3
*   Close streams explicitly, avoiding race conditions and supporting
    more robust restarts on Windows.
*   Improved shuffled performance for dask.dataframe
*   Add adaptive allocation cluster manager
*   Reduce administrative overhead when dealing with many workers
*   dask-ssh --log-directory . no longer errors
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

*   Rename Executor to Client #492
*   Add `--memory-limit` option to `dask-worker`, enabling spill-to-disk
    behavior when running out of memory #485
*   Add `--pid-file` option to dask-worker and `--dask-scheduler` #496
*   Add ``upload_environment`` function to distribute conda environments.
    This is experimental, undocumented, and may change without notice.  # 494
*   Add `workers=` keyword argument to `Client.compute` and `Client.persist`,
    supporting location-restricted workloads with Dask collections #484
*   Add ``upload_environment`` function to distribute conda environments.
    This is experimental, undocumented, and may change without notice.  # 494
    *   Add optional `dask_worker=` keyword to `client.run` functions that gets
        provided the worker or nanny object
    *   Add `nanny=False` keyword to `Client.run`, allowing for the execution
        of arbitrary functions on the nannies as well as normal workers


1.12.2
------

This release adds some new features and removes dead code

*   Publish and share datasets on the scheduler between many clients
    https://github.com/dask/distributed/pull/453
    https://distributed.readthedocs.io/en/latest/publish.html
*   Launch tasks from other tasks (experimental)
    https://distributed.readthedocs.io/en/latest/task-launch.html
    https://github.com/dask/distributed/pull/471
*   Remove unused code, notably the `Center` object and older client functions
    https://github.com/dask/distributed/pull/478
*   Executor() and LocalCluster() is now robust to Bokeh's absence
    https://github.com/dask/distributed/pull/481
*   Removed s3fs and boto3 from requirements.  These have moved to Dask.

1.12.1
------

This release is largely a bugfix release, recovering from the previous large
refactor.

*  Fixes from previous refactor
    *  Ensure idempotence across clients
    *  Stress test losing scattered data permanently
*  IPython fixes
    *  Add `start_ipython_scheduler` method to Executor
    *  Add `%remote` magic for workers
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
