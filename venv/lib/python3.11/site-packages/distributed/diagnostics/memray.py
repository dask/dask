from __future__ import annotations

import contextlib
import logging
import pathlib
import subprocess
import time
import uuid
from collections.abc import Iterator, Sequence
from typing import Any, Literal
from urllib.parse import quote

from toolz.itertoolz import partition

from distributed import get_client
from distributed.worker import Worker

try:
    import memray
except ImportError:
    raise ImportError("You have to install memray to use this module.")

logger = logging.getLogger(__name__)


def _start_memray(dask_worker: Worker, filename: str, **kwargs: Any) -> bool:
    """Start the memray Tracker on a Server"""
    if hasattr(dask_worker, "_memray"):
        dask_worker._memray.close()

    path = pathlib.Path(dask_worker.local_directory) / (filename + str(dask_worker.id))
    if path.exists():
        path.rmdir()

    dask_worker._memray = contextlib.ExitStack()  # type: ignore[attr-defined]
    dask_worker._memray.enter_context(  # type: ignore[attr-defined]
        memray.Tracker(path, **kwargs)
    )

    return True


def _fetch_memray_profile(
    dask_worker: Worker, filename: str, report_args: Sequence[str] | Literal[False]
) -> bytes:
    """Generate and fetch the memray report"""
    if not hasattr(dask_worker, "_memray"):
        return b""
    path = pathlib.Path(dask_worker.local_directory) / (filename + str(dask_worker.id))
    dask_worker._memray.close()
    del dask_worker._memray

    if not report_args:
        with open(path, "rb") as fd:
            return fd.read()

    report_filename = path.with_suffix(".html")
    if not report_args[0] == "memray":
        report_args = ["memray"] + list(report_args)
    assert "-f" not in report_args, "Cannot provide filename for report generation"
    assert (
        "-o" not in report_args
    ), "Cannot provide output filename for report generation"
    report_args = list(report_args) + ["-f", str(path), "-o", str(report_filename)]
    subprocess.run(report_args)
    with open(report_filename, "rb") as fd:
        return fd.read()


@contextlib.contextmanager
def memray_workers(
    directory: str | pathlib.Path = "memray-profiles",
    workers: int | None | list[str] = None,
    report_args: Sequence[str] | Literal[False] = (
        "flamegraph",
        "--temporal",
        "--leaks",
    ),
    fetch_reports_parallel: bool | int = True,
    **memray_kwargs: Any,
) -> Iterator[None]:
    """Generate a Memray profile on the workers and download the generated report.

    Example::

        with memray_workers():
            client.submit(my_function).result()

        # Or even while the computation is already running

        fut = client.submit(my_function)

        with memray_workers():
            time.sleep(10)

        fut.result()

    Parameters
    ----------
    directory : str
        The directory to save the reports to.
    workers : int | None | list[str]
        The workers to profile. If int, the first n workers will be used.
        If None, all workers will be used.
        If list[str], the workers with the given addresses will be used.
    report_args : tuple[str]
        Particularly for native_traces=True, the reports have to be
        generated on the same host using the same Python interpreter as the
        profile was generated. Otherwise, native traces will yield unusable
        results. Therefore, we're generating the reports on the workers and
        download them afterwards. You can modify the report generation by
        providing additional arguments and we will generate the reports as::

            memray *report_args -f <filename> -o <filename>.html

        If the raw data should be fetched instead of the report, set this to
        False.

    fetch_reports_parallel : bool | int
        Fetching results is sometimes slow and it's sometimes not desired to
        wait for all workers to finish before receiving the first reports.
        This controls how many workers are fetched concurrently.

            int: Number of workers to fetch concurrently
            True: All workers concurrently
            False: One worker at a time

    **memray_kwargs
        Keyword arguments to be passed to memray.Tracker, e.g.
        {"native_traces": True}
    """
    directory = pathlib.Path(directory)
    client = get_client()
    scheduler_info = client.scheduler_info(n_workers=-1)
    worker_addr = scheduler_info["workers"]
    worker_names = {
        addr: winfo["name"] for addr, winfo in scheduler_info["workers"].items()
    }
    if not workers or isinstance(workers, int):
        nworkers = len(worker_addr)
        if isinstance(workers, int):
            nworkers = workers
        workers = list(worker_addr)[:nworkers]
    workers = list(workers)
    filename = uuid.uuid4().hex
    assert all(client.run(_start_memray, filename=filename, **memray_kwargs).values())
    # Sleep for a brief moment such that we get
    # a clear profiling signal when everything starts
    time.sleep(0.1)
    try:
        yield
    finally:
        directory.mkdir(exist_ok=True)

        client = get_client()
        if fetch_reports_parallel is True:
            fetch_parallel = len(workers)
        elif fetch_reports_parallel is False:
            fetch_parallel = 1
        else:
            fetch_parallel = fetch_reports_parallel

        for w in partition(fetch_parallel, workers):
            try:
                profiles = client.run(
                    _fetch_memray_profile,
                    filename=filename,
                    report_args=report_args,
                    workers=w,
                )
                for worker_addr, profile in profiles.items():
                    path = directory / quote(str(worker_names[worker_addr]), safe="")
                    if report_args:
                        suffix = ".html"
                    else:
                        suffix = ".memray"
                    with open(str(path) + suffix, "wb") as fd:
                        fd.write(profile)

            except Exception:
                logger.exception(
                    "Exception during report downloading from worker %s", w
                )


@contextlib.contextmanager
def memray_scheduler(
    directory: str | pathlib.Path = "memray-profiles",
    report_args: Sequence[str] | Literal[False] = (
        "flamegraph",
        "--temporal",
        "--leaks",
    ),
    **memray_kwargs: Any,
) -> Iterator[None]:
    """Generate a Memray profile on the Scheduler and download the generated report.

    Example::

        with memray_scheduler():
            client.submit(my_function).result()

        # Or even while the computation is already running

        fut = client.submit(my_function)

        with memray_scheduler():
            time.sleep(10)

        fut.result()

    Parameters
    ----------
    directory : str
        The directory to save the reports to.
    report_args : tuple[str]
        Particularly for native_traces=True, the reports have to be
        generated on the same host using the same Python interpreter as the
        profile was generated. Otherwise, native traces will yield unusable
        results. Therefore, we're generating the reports on the Scheduler and
        download them afterwards. You can modify the report generation by
        providing additional arguments and we will generate the reports as::

            memray *report_args -f <filename> -o <filename>.html

        If the raw data should be fetched instead of the report, set this to
        False.
    **memray_kwargs
        Keyword arguments to be passed to memray.Tracker, e.g.
        {"native_traces": True}
    """
    directory = pathlib.Path(directory)
    client = get_client()
    filename = uuid.uuid4().hex
    assert client.run_on_scheduler(_start_memray, filename=filename, **memray_kwargs)
    # Sleep for a brief moment such that we get
    # a clear profiling signal when everything starts
    time.sleep(0.1)
    try:
        yield
    finally:
        directory.mkdir(exist_ok=True)

        client = get_client()

        profile = client.run_on_scheduler(
            _fetch_memray_profile,
            filename=filename,
            report_args=report_args,
        )
        path = directory / "scheduler"
        if report_args:
            suffix = ".html"
        else:
            suffix = ".memray"
        with open(str(path) + suffix, "wb") as fd:
            fd.write(profile)
