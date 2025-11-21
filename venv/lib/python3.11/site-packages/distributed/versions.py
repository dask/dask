"""utilities for package version introspection"""

from __future__ import annotations

import os
import platform
import struct
import sys
from collections.abc import Iterable
from functools import cache
from importlib.metadata import version
from itertools import chain
from typing import Any

from packaging.requirements import Requirement

BOKEH_REQUIREMENT = Requirement("bokeh>=3.1.0")

required_packages = [
    "dask",
    "distributed",
    "msgpack",
    "cloudpickle",
    "tornado",
    "toolz",
]

optional_packages = [
    "numpy",
    "pandas",
    "lz4",
]


# only these scheduler packages will be checked for version mismatch
scheduler_relevant_packages = set(required_packages) | {
    "lz4",
    "python",
} - {"msgpack"}


# notes to be displayed for mismatch packages
notes_mismatch_package: dict[str, str] = {}


def get_versions(
    packages: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return basic information on our software installation, and our installed versions
    of packages
    """
    return {
        "host": get_system_info(),
        "packages": get_package_info(
            chain(required_packages, optional_packages, packages or [])
        ),
    }


def get_system_info() -> dict[str, Any]:
    uname = platform.uname()
    return {
        "python": "%d.%d.%d.%s.%s" % sys.version_info,
        "python-bits": struct.calcsize("P") * 8,
        "OS": uname.system,
        "OS-release": uname.release,
        "machine": uname.machine,
        "processor": uname.processor,
        "byteorder": sys.byteorder,
        "LC_ALL": os.environ.get("LC_ALL", "None"),
        "LANG": os.environ.get("LANG", "None"),
    }


@cache
def _version_cached(pkg: str) -> str | None:
    """Try to get the version of a package from the cache"""
    try:
        # using importlib.metadata.version is much faster than importing the
        # actual package.
        # However, it is much slower than using pkg.__version__ iff the package
        # is already imported, e.g. in our test suite! Therefore, cache this.
        return version(pkg)
    except Exception:
        return None


def get_package_info(pkgs: Iterable[str]) -> dict[str, str | None]:
    """get package versions for the passed required & optional packages"""

    pversions: dict[str, str | None] = {"python": ".".join(map(str, sys.version_info))}
    for pkg in pkgs:
        pversions[pkg] = _version_cached(pkg)
    return pversions


def error_message(scheduler, workers, source, source_name="Client"):
    from distributed.utils import asciitable

    source = source.get("packages") if source else "UNKNOWN"
    scheduler = scheduler.get("packages") if scheduler else "UNKNOWN"
    workers = {k: v.get("packages") if v else "UNKNOWN" for k, v in workers.items()}

    packages = set()
    packages.update(source)
    packages.update(scheduler)
    for worker in workers:
        packages.update(workers.get(worker))

    errs = []
    notes = []
    for pkg in sorted(packages):
        versions = set()
        scheduler_version = (
            scheduler.get(pkg, "MISSING") if isinstance(scheduler, dict) else scheduler
        )
        if pkg in scheduler_relevant_packages:
            versions.add(scheduler_version)

        source_version = (
            source.get(pkg, "MISSING") if isinstance(source, dict) else source
        )
        versions.add(source_version)

        worker_versions = {
            (
                workers[w].get(pkg, "MISSING")
                if isinstance(workers[w], dict)
                else workers[w]
            )
            for w in workers
        }
        versions |= worker_versions

        if len(versions) <= 1:
            continue
        if len(worker_versions) == 1:
            worker_versions = list(worker_versions)[0]
        elif len(worker_versions) == 0:
            worker_versions = None

        errs.append((pkg, source_version, scheduler_version, worker_versions))
        if pkg in notes_mismatch_package.keys():
            notes.append(f"-  {pkg}: {notes_mismatch_package[pkg]}")

    out = {"warning": "", "error": ""}

    if errs:
        err_table = asciitable(["Package", source_name, "Scheduler", "Workers"], errs)
        err_msg = f"Mismatched versions found\n\n{err_table}"
        if notes:
            err_msg += "\nNotes: \n{}".format("\n".join(notes))
        out["warning"] += err_msg

    return out


class VersionMismatchWarning(Warning):
    """Indicates version mismatch between nodes"""
