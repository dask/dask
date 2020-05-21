""" utilities for package version introspection """

from __future__ import print_function, division, absolute_import

import platform
import struct
import os
import sys
import importlib


required_packages = [
    ("dask", lambda p: p.__version__),
    ("distributed", lambda p: p.__version__),
    ("msgpack", lambda p: ".".join([str(v) for v in p.version])),
    ("cloudpickle", lambda p: p.__version__),
    ("tornado", lambda p: p.version),
    ("toolz", lambda p: p.__version__),
]

optional_packages = [
    ("numpy", lambda p: p.__version__),
    ("lz4", lambda p: p.__version__),
    ("blosc", lambda p: p.__version__),
]


# only these scheduler packages will be checked for version mismatch
scheduler_relevant_packages = set(pkg for pkg, _ in required_packages) | set(
    ["lz4", "blosc"]
)


# notes to be displayed for mismatch packages
notes_mismatch_package = {
    "msgpack": "Variation is ok, as long as everything is above 0.6",
    "lz4": "Variation is ok, but missing libraries are not",
    "python": "Variation is sometimes ok, sometimes not. It depends on your workloads",
}


def get_versions(packages=None):
    """
    Return basic information on our software installation, and our installed versions of packages.
    """
    if packages is None:
        packages = []

    d = {
        "host": get_system_info(),
        "packages": get_package_info(
            required_packages + optional_packages + list(packages)
        ),
    }

    return d


def get_system_info():
    (sysname, nodename, release, version, machine, processor) = platform.uname()
    host = {
        "python": "%d.%d.%d.%s.%s" % sys.version_info[:],
        "python-bits": struct.calcsize("P") * 8,
        "OS": "%s" % sysname,
        "OS-release": "%s" % release,
        "machine": "%s" % machine,
        "processor": "%s" % processor,
        "byteorder": "%s" % sys.byteorder,
        "LC_ALL": "%s" % os.environ.get("LC_ALL", "None"),
        "LANG": "%s" % os.environ.get("LANG", "None"),
    }

    return host


def version_of_package(pkg):
    """ Try a variety of common ways to get the version of a package """
    from contextlib import suppress

    with suppress(AttributeError):
        return pkg.__version__
    with suppress(AttributeError):
        return str(pkg.version)
    with suppress(AttributeError):
        return ".".join(map(str, pkg.version_info))
    return None


def get_package_info(pkgs):
    """ get package versions for the passed required & optional packages """

    pversions = [("python", ".".join(map(str, sys.version_info)))]
    for pkg in pkgs:
        if isinstance(pkg, (tuple, list)):
            modname, ver_f = pkg
        else:
            modname = pkg
            ver_f = version_of_package

        if ver_f is None:
            ver_f = version_of_package

        try:
            mod = importlib.import_module(modname)
            ver = ver_f(mod)
            pversions.append((modname, ver))
        except Exception:
            pversions.append((modname, None))

    return dict(pversions)


def error_message(scheduler, workers, client, client_name="client"):
    from .utils import asciitable

    client = client.get("packages") if client else "UNKNOWN"
    scheduler = scheduler.get("packages") if scheduler else "UNKNOWN"
    workers = {k: v.get("packages") if v else "UNKNOWN" for k, v in workers.items()}

    packages = set()
    packages.update(client)
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

        client_version = (
            client.get(pkg, "MISSING") if isinstance(client, dict) else client
        )
        versions.add(client_version)

        worker_versions = set(
            workers[w].get(pkg, "MISSING")
            if isinstance(workers[w], dict)
            else workers[w]
            for w in workers
        )
        versions |= worker_versions

        if len(versions) <= 1:
            continue
        if len(worker_versions) == 1:
            worker_versions = list(worker_versions)[0]
        elif len(worker_versions) == 0:
            worker_versions = None

        errs.append((pkg, client_version, scheduler_version, worker_versions))
        if pkg in notes_mismatch_package.keys():
            notes.append(f"-  {pkg}: {notes_mismatch_package[pkg]}")

    if errs:
        err_table = asciitable(["Package", client_name, "scheduler", "workers"], errs)
        err_msg = f"Mismatched versions found\n\n{err_table}"
        if notes:
            err_msg += "\nNotes: \n{}".format("\n".join(notes))
        return err_msg
    else:
        return ""


class VersionMismatchWarning(Warning):
    """Indicates version mismatch between nodes"""
