""" utilities for package version introspection """

from __future__ import print_function, division, absolute_import

import platform
import struct
import os
import sys
import locale
import importlib


required_packages = [('dask', lambda p: p.__version__),
                     ('distributed', lambda p: p.__version__),
                     ('msgpack', lambda p: '.'.join([str(v) for v in p.version])),
                     ('cloudpickle', lambda p: p.__version__),
                     ('toolz', lambda p: p.__version__)]

optional_packages = [('numpy', lambda p: p.__version__),
                     ('pandas', lambda p: p.__version__)]


def get_versions():
    """ Return basic information on our software installation,
    and out installed versions of packages. """

    d = {'host': get_system_info(),
         'packages': {'required': get_package_info(required_packages),
                      'optional': get_package_info(optional_packages)}
         }
    return d


def get_system_info():
    (sysname, nodename, release,
     version, machine, processor) = platform.uname()
    host = [("python", "%d.%d.%d.%s.%s" % sys.version_info[:]),
            ("python-bits", struct.calcsize("P") * 8),
            ("OS", "%s" % (sysname)),
            ("OS-release", "%s" % (release)),
            ("machine", "%s" % (machine)),
            ("processor", "%s" % (processor)),
            ("byteorder", "%s" % sys.byteorder),
            ("LC_ALL", "%s" % os.environ.get('LC_ALL', "None")),
            ("LANG", "%s" % os.environ.get('LANG', "None")),
            ("LOCALE", "%s.%s" % locale.getlocale()),
            ]

    return host


def get_package_info(pkgs):
    """ get package versions for the passed required & optional packages """

    pversions = []
    for (modname, ver_f) in pkgs:
        try:
            mod = importlib.import_module(modname)
            ver = ver_f(mod)
            pversions.append((modname, ver))
        except:
            pversions.append((modname, None))

    return pversions
