import atexit
import logging
import os
import shutil
import sys
from importlib import import_module

from .utils import import_file

logger = logging.getLogger(__name__)


def preload_modules(names, parameter=None, file_dir=None):
    """ Imports modules, handles `dask_setup` and `dask_teardown` functions

    Parameters
    ----------
    names: list of strings
        Module names or file paths
    parameter: object
        Parameter passed to `dask_setup` and `dask_teardown`
    file_dir: string
        Path of a directory where files should be copied
    """
    for name in names:
        # import
        if name.endswith(".py"):
            # name is a file path
            if file_dir is not None:
                basename = os.path.basename(name)
                copy_dst = os.path.join(file_dir, basename)
                if os.path.exists(copy_dst):
                    logger.error("File name collision: %s", basename)
                shutil.copy(name, copy_dst)
                module = import_file(copy_dst)[0]
            else:
                module = import_file(name)[0]

        else:
            # name is a module name
            if name not in sys.modules:
                import_module(name)
            module = sys.modules[name]

        # handle special functions
        dask_setup = getattr(module, 'dask_setup', None)
        dask_teardown = getattr(module, 'dask_teardown', None)
        if dask_setup is not None:
            dask_setup(parameter)
        if dask_teardown is not None:
            atexit.register(dask_teardown, parameter)
