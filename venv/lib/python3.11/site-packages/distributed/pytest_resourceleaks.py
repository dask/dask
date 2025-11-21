"""A pytest plugin to trace resource leaks.

Usage
-----
This plugin is enabled from the command line with -L / --leaks.
See `pytest --help` for further configuration settings.

You may mark individual tests as known to be leaking with the fixture

    @pytest.mark.leaking(check1, check2, ...)

Where the valid checks are 'fds', 'memory', 'threads', 'processes', 'tracemalloc'.

e.g.

    @pytest.mark.leaking("threads")

If you do, the specified checks won't report errors.

Known issues
------------
- Tests that contain imports will be flagged as leaking RAM (memory and tracemallock
  checks) if it's the first time in the test suite that the import happens; e.g.

      def test1():
          pytest.importorskip("numpy")

  Same issue happens with tests that populate global caches (e.g. linecache, re).
  A previous version of this plugin had an option to silently retry a test once after a
  failure; that version is no longer working as of the latest pytest. Reinstating the
  flag would solve this issue. See pytest_rerunfailures code for inspiration.

- The @gen_cluster fixture leaks 2 fds on the first test decorated with it within a test
  suite; This issue would also be fixed by rerunning failing tests.

- The @pytest.mark.flaky decorator (pytest_rerunfailures) completely disables this
  plugin for the decorated tests.

- You cannot expect the process memory to go down immediately and deterministically as
  soon as you garbage collect Python objects. This makes the 'memory' check very
  unreliable. On Linux, this can be improved by reducing the MALLOC_TRIM glibc setting
  (see distributed.yaml).
"""

from __future__ import annotations

import gc
import os
import sys
import threading
import tracemalloc
from collections import defaultdict
from time import sleep
from typing import Any, ClassVar

import psutil
import pytest

from distributed.metrics import time


def pytest_addoption(parser):
    group = parser.getgroup("resource leaks")
    known_checkers = ", ".join(sorted("'%s'" % s for s in all_checkers))
    group.addoption(
        "-L",
        "--leaks",
        help="List of resources to monitor for leaks before and after each test. "
        "Can be 'all' or a comma-separated list of resource names "
        f"(possible values: {known_checkers}).",
    )
    group.addoption(
        "--leaks-timeout",
        type=float,
        default=0.5,
        help="Wait at most these many seconds before marking a test as leaking "
        "(default: %(default)s)",
    )
    group.addoption(
        "--leaks-fail",
        action="store_true",
        help="Mark leaked tests as failed",
    )


def pytest_configure(config: pytest.Config) -> None:
    leaks = config.getvalue("leaks")
    if not leaks:
        return
    if leaks == "all":
        leaks = sorted(c for c in all_checkers if c != "demo")
    else:
        leaks = leaks.split(",")
    unknown = sorted(set(leaks) - set(all_checkers))
    if unknown:
        raise ValueError(f"unknown resources: {unknown!r}")

    checkers = [all_checkers[leak]() for leak in leaks]
    checker = LeakChecker(
        checkers=checkers,
        grace_delay=config.getvalue("leaks_timeout"),
        mark_failed=config.getvalue("leaks_fail"),
    )
    config.pluginmanager.register(checker, "leaks_checker")


all_checkers: dict[str, type[ResourceChecker]] = {}


class ResourceChecker:
    name: ClassVar[str]

    def __init_subclass__(cls, name: str):
        assert name not in all_checkers
        cls.name = name
        all_checkers[name] = cls

    def on_start_test(self) -> None:
        pass

    def on_stop_test(self) -> None:
        pass

    def on_retry(self) -> None:
        pass

    def measure(self) -> Any:
        raise NotImplementedError

    def has_leak(self, before: Any, after: Any) -> bool:
        raise NotImplementedError

    def format(self, before: Any, after: Any) -> str:
        raise NotImplementedError


class DemoChecker(ResourceChecker, name="demo"):
    """Checker that always leaks. Used to test the core LeakChecker functionality."""

    i: int

    def __init__(self):
        self.i = 0

    def measure(self) -> int:
        self.i += 1
        return self.i

    def has_leak(self, before: int, after: int) -> bool:
        return after > before

    def format(self, before: int, after: int) -> str:
        return f"counter increased from {before} to {after}"


class FDChecker(ResourceChecker, name="fds"):
    def measure(self) -> int:
        # Note: WINDOWS constant doesn't work with `mypy --platform win32`
        if sys.platform == "win32":
            # Don't use num_handles(); you'll get tens of thousands of reported leaks
            return 0
        else:
            return psutil.Process().num_fds()

    def has_leak(self, before: int, after: int) -> bool:
        return after > before

    def format(self, before: int, after: int) -> str:
        return f"leaked {after - before} file descriptor(s) ({before}->{after})"


class RSSMemoryChecker(ResourceChecker, name="memory"):
    LEAK_THRESHOLD = 10 * 2**20

    def measure(self) -> int:
        return psutil.Process().memory_info().rss

    def has_leak(self, before: int, after: int) -> bool:
        return after > before + self.LEAK_THRESHOLD

    def format(self, before: int, after: int) -> str:
        return f"leaked {(after - before) / 2**20:.1f} MiB of RSS memory"


class ActiveThreadsChecker(ResourceChecker, name="threads"):
    def measure(self) -> set[threading.Thread]:
        return set(threading.enumerate())

    def has_leak(
        self, before: set[threading.Thread], after: set[threading.Thread]
    ) -> bool:
        return not after <= before

    def format(
        self, before: set[threading.Thread], after: set[threading.Thread]
    ) -> str:
        leaked = after - before
        assert leaked
        return f"leaked {len(leaked)} Python thread(s): {sorted(leaked, key=str)}"


class ChildProcess:
    """Child process info

    We use pid and creation time as keys to disambiguate between processes (and protect
    against pid reuse); other properties such as cmdline may change for a given process
    """

    pid: int
    name: str
    cmdline: list[str]
    create_time: float

    def __init__(self, p: psutil.Process):
        self.pid = p.pid
        self.name = p.name()
        self.cmdline = p.cmdline()
        self.create_time = p.create_time()

    def __hash__(self) -> int:
        return self.pid

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ChildProcess)
            and self.pid == other.pid
            and self.create_time == other.create_time
        )

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ChildProcess):
            raise TypeError(other)
        return self.pid < other.pid


def _samefile(f1: str, f2: str) -> bool:
    """
    Compare files ignoring a FileNotFoundError caused by zombie processes.
    """
    try:
        return os.path.samefile(f1, f2)
    except FileNotFoundError:
        return False


class ChildProcessesChecker(ResourceChecker, name="processes"):
    def measure(self) -> set[ChildProcess]:
        children = set()
        p = psutil.Process()
        for c in p.children(recursive=True):
            try:
                with c.oneshot():
                    if (
                        c.ppid() == p.pid
                        and _samefile(c.exe(), sys.executable)
                        and any(
                            # Skip multiprocessing resource tracker
                            a.startswith(
                                "from multiprocessing.resource_tracker import main"
                            )
                            # Skip forkserver process; the forkserver's children
                            # however will be recorded normally
                            or a.startswith(
                                "from multiprocessing.forkserver import main"
                            )
                            for a in c.cmdline()
                        )
                    ):
                        continue

                    children.add(ChildProcess(c))
            except psutil.NoSuchProcess:
                pass
        return children

    def has_leak(self, before: set[ChildProcess], after: set[ChildProcess]) -> bool:
        return not after <= before

    def format(self, before: set[ChildProcess], after: set[ChildProcess]) -> str:
        leaked = sorted(after - before)
        assert leaked
        return f"leaked {len(leaked)} processes:\n" + "\n".join(
            f"  - pid={p.pid}, name={p.name!r}, cmdline={p.cmdline!r}" for p in leaked
        )


class TracemallocMemoryChecker(ResourceChecker, name="tracemalloc"):
    # Report a leak if the traced memory increased by at least this many bytes
    LEAK_THRESHOLD = 2**20
    # Report at most this many leaks
    NDIFF = 5
    # Report less than NDIFF leaks if they amount to less than this many bytes
    MIN_SIZE_DIFF = 200 * 1024

    def on_start_test(self) -> None:
        tracemalloc.start(1)

    def on_stop_test(self) -> None:
        tracemalloc.stop()

    def measure(self) -> tuple[int, tracemalloc.Snapshot]:
        current, _ = tracemalloc.get_traced_memory()
        snap = tracemalloc.take_snapshot()
        return current, snap

    def has_leak(
        self,
        before: tuple[int, tracemalloc.Snapshot],
        after: tuple[int, tracemalloc.Snapshot],
    ) -> bool:
        return after[0] > before[0] + self.LEAK_THRESHOLD

    def format(
        self,
        before: tuple[int, tracemalloc.Snapshot],
        after: tuple[int, tracemalloc.Snapshot],
    ) -> str:
        bytes_before, snap_before = before
        bytes_after, snap_after = after
        diff = snap_after.compare_to(snap_before, "traceback")

        lines = [
            f"leaked {(bytes_after - bytes_before) / 2 ** 20:.1f} MiB "
            "of traced Python memory"
        ]
        for stat in diff[: self.NDIFF]:
            size_diff = stat.size_diff or stat.size
            if size_diff < self.MIN_SIZE_DIFF:
                break
            count = stat.count_diff or stat.count
            lines += [f"  - leaked {size_diff / 2**20:.1f} MiB in {count} calls at:"]
            lines += ["    " + line for line in stat.traceback.format()]

        return "\n".join(lines)


class LeakChecker:
    checkers: list[ResourceChecker]
    grace_delay: float
    mark_failed: bool

    # {nodeid: {checkers}}
    skip_checkers: dict[str, set[ResourceChecker]]
    # {nodeid: {checker: [(before, after)]}}
    counters: dict[str, dict[ResourceChecker, list[tuple[Any, Any]]]]
    # {nodeid: [(checker, before, after)]}
    leaks: dict[str, list[tuple[ResourceChecker, Any, Any]]]
    # {nodeid: {outcomes}}
    outcomes: defaultdict[str, set[str]]

    def __init__(
        self,
        checkers: list[ResourceChecker],
        grace_delay: float,
        mark_failed: bool,
    ):
        self.checkers = checkers
        self.grace_delay = grace_delay
        self.mark_failed = mark_failed

        self.skip_checkers = {}
        self.counters = {}
        self.leaks = {}
        self.outcomes = defaultdict(set)

    def cleanup(self) -> None:
        gc.collect()

    def checks_for_item(self, nodeid: str) -> list[ResourceChecker]:
        return [c for c in self.checkers if c not in self.skip_checkers.get(nodeid, ())]

    def measure(self, nodeid: str) -> list[tuple[ResourceChecker, Any]]:
        # Return items in order
        return [(c, c.measure()) for c in self.checks_for_item(nodeid)]

    def measure_before_test(self, nodeid: str) -> None:
        for checker in self.checks_for_item(nodeid):
            checker.on_start_test()
        for checker, before in self.measure(nodeid):
            assert before is not None
            self.counters[nodeid][checker].append((before, None))

    def measure_after_test(self, nodeid: str) -> None:
        outcomes = self.outcomes[nodeid]
        # pytest_rerunfailures (@pytest.mark.flaky) breaks this plugin and causes
        # outcomes to be empty.
        if "passed" not in outcomes:
            # Test failed or skipped
            return

        def run_measurements() -> list[tuple[ResourceChecker, Any, Any]]:
            leaks = []
            for checker, after in self.measure(nodeid):
                c = self.counters[nodeid][checker]
                before, _ = c[-1]
                c[-1] = (before, after)
                if checker.has_leak(before, after):
                    leaks.append((checker, before, after))
            return leaks

        t1 = time()
        deadline = t1 + self.grace_delay
        leaks = run_measurements()
        if leaks:
            self.cleanup()
            for c, _, _ in leaks:
                c.on_retry()
            leaks = run_measurements()

        while leaks and time() < deadline:
            sleep(0.1)
            self.cleanup()
            for c, _, _ in leaks:
                c.on_retry()
            leaks = run_measurements()

        if leaks:
            self.leaks[nodeid] = leaks
        else:
            self.leaks.pop(nodeid, None)

        for checker in self.checks_for_item(nodeid):
            checker.on_stop_test()

    # Note on hook execution order:
    #   pytest_runtest_protocol
    #       pytest_runtest_setup
    #       pytest_report_teststatus
    #       pytest_runtest_call
    #       pytest_report_teststatus
    #       pytest_runtest_teardown
    #       pytest_report_teststatus

    # See also https://github.com/abalkin/pytest-leaks/blob/master/pytest_leaks.py

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item, nextitem):
        if not self.checkers:
            return

        nodeid = item.nodeid
        assert nodeid not in self.counters
        self.counters[nodeid] = {c: [] for c in self.checkers}

        leaking_mark = item.get_closest_marker("leaking")
        if leaking_mark:
            unknown = sorted(set(leaking_mark.args) - set(all_checkers))
            if unknown:
                raise ValueError(
                    f"pytest.mark.leaking: unknown resources {unknown}; "
                    f"must be one of {list(all_checkers)}"
                )
            classes = tuple(all_checkers[a] for a in leaking_mark.args)
            self.skip_checkers[nodeid] = {
                c for c in self.checkers if isinstance(c, classes)
            }

        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item):
        self.measure_before_test(item.nodeid)
        yield

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_runtest_teardown(self, item):
        yield
        self.measure_after_test(item.nodeid)
        leaks = self.leaks.get(item.nodeid)
        if leaks and self.mark_failed:
            # Trigger fail here to allow stopping with `-x`
            pytest.fail()

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_report_teststatus(self, report):
        nodeid = report.nodeid
        self.outcomes[nodeid].add(report.outcome)
        outcome = yield
        if report.when == "teardown":
            leaks = self.leaks.get(report.nodeid)
            if leaks:
                if self.mark_failed:
                    outcome.force_result(("failed", "L", "LEAKED"))
                    report.outcome = "failed"
                    report.longrepr = "\n".join(
                        [
                            f"{nodeid} leaking {checker.name}: "
                            f"{checker.format(before, after)}"
                            for checker, before, after in leaks
                        ]
                    )
                else:
                    outcome.force_result(("leaked", "L", "LEAKED"))

    @pytest.hookimpl
    def pytest_terminal_summary(self, terminalreporter, exitstatus):
        tr = terminalreporter
        leaked = tr.getreports("leaked")
        if leaked:
            # If mark_failed is False, leaks are output as a separate
            # results section
            tr.write_sep("=", "RESOURCE LEAKS")
            for rep in leaked:
                nodeid = rep.nodeid
                for checker, before, after in self.leaks[nodeid]:
                    tr.line(
                        f"{rep.nodeid} leaking {checker.name}: "
                        f"{checker.format(before, after)}"
                    )
