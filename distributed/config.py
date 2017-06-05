from __future__ import print_function, division, absolute_import

import logging
import os
import sys
import warnings

from .compatibility import FileExistsError, logging_names

logger = logging.getLogger(__name__)

config = {}


def ensure_config_file(source, destination):
    if not os.path.exists(destination):
        import shutil
        if not os.path.exists(os.path.dirname(destination)):
            try:
                os.mkdir(os.path.dirname(destination))
            except FileExistsError:
                pass
        # Atomically create destination.  Parallel testing discovered
        # a race condition where a process can be busy creating the
        # destination while another process reads an empty config file.
        tmp = '%s.tmp.%d' % (destination, os.getpid())
        shutil.copy(source, tmp)
        try:
            os.rename(tmp, destination)
        except OSError:
            os.remove(tmp)


def determine_config_file():
    path = os.environ.get('DASK_CONFIG')
    if path:
        if (os.path.exists(path) and
            (os.path.isfile(path) or os.path.islink(path))):
            return path
        warnings.warn("DASK_CONFIG set to '%s' but file does not exist "
                      "or is not a regular file" % (path,),
                      UserWarning)

    dirname = os.path.dirname(__file__)
    default_path = os.path.join(dirname, 'config.yaml')
    path = os.path.join(os.path.expanduser('~'), '.dask', 'config.yaml')

    try:
        ensure_config_file(default_path, path)
    except EnvironmentError as e:
        warnings.warn("Could not write default config file to '%s'. "
                      "Received error %s" % (path, e),
                      UserWarning)

    return path if os.path.exists(path) else default_path


def load_config_file(config, path):
    with open(path) as f:
        text = f.read()
        config.update(yaml.load(text))


def load_env_vars(config):
    for name, value in os.environ.items():
        if name.startswith('DASK_'):
            varname = name[5:].lower().replace('_', '-')
            config[varname] = value


def initialize_logging(config):
    loggers = config.get('logging', {})
    loggers.setdefault('distributed', 'info')
    # We could remove those lines and let the default config.yaml handle it
    loggers.setdefault('tornado', 'critical')
    loggers.setdefault('tornado.application', 'error')

    fmt = '%(name)s - %(levelname)s - %(message)s'

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(fmt))
    for name, level in loggers.items():
        if isinstance(level, str):
            level = logging_names[level.upper()]
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.propagate = False


try:
    import yaml
except ImportError:
    pass
else:
    path = determine_config_file()
    load_config_file(config, path)

load_env_vars(config)

initialize_logging(config)
