""" utilities for package version introspection """

from __future__ import print_function, division, absolute_import

import platform
import struct
import os
import sys
import locale
import importlib

from .utils import ignoring


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
    ("pandas", lambda p: p.__version__),
    ("bokeh", lambda p: p.__version__),
    ("lz4", lambda p: p.__version__),
    ("dask_ml", lambda p: p.__version__),
    ("blosc", lambda p: p.__version__),
]


def get_versions(packages=None):
    """
    Return basic information on our software installation, and out installed versions of packages.
    """
    if packages is None:
        packages = []

    d = {
        "host": get_system_info(),
        "packages": {
            "required": get_package_info(required_packages),
            "optional": get_package_info(optional_packages + list(packages)),
        },
    }
    return d


def get_system_info():
    (sysname, nodename, release, version, machine, processor) = platform.uname()
    host = [
        ("python", "%d.%d.%d.%s.%s" % sys.version_info[:]),
        ("python-bits", struct.calcsize("P") * 8),
        ("OS", "%s" % (sysname)),
        ("OS-release", "%s" % (release)),
        ("machine", "%s" % (machine)),
        ("processor", "%s" % (processor)),
        ("byteorder", "%s" % sys.byteorder),
        ("LC_ALL", "%s" % os.environ.get("LC_ALL", "None")),
        ("LANG", "%s" % os.environ.get("LANG", "None")),
        ("LOCALE", "%s.%s" % locale.getlocale()),
    ]

    return host


def version_of_package(pkg):
    """ Try a variety of common ways to get the version of a package """
    with ignoring(AttributeError):
        return pkg.__version__
    with ignoring(AttributeError):
        return str(pkg.version)
    with ignoring(AttributeError):
        return ".".join(map(str, pkg.version_info))
    return None


def get_package_info(pkgs):
    """ get package versions for the passed required & optional packages """

    pversions = []
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

    return pversions
