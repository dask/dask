from __future__ import print_function, division, absolute_import

import logging
import os
import sys

from .compatibility import FileExistsError, logging_names

logger = logging.getLogger(__name__)

config = {}

try:
    import yaml
except ImportError:
    pass
else:
    dirname = os.path.dirname(__file__)
    default_path = os.path.join(dirname, 'config.yaml')
    dask_config_path = os.path.join(os.path.expanduser('~'), '.dask', 'config.yaml')

    def ensure_config_file(destination=dask_config_path):
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
            shutil.copy(default_path, tmp)
            try:
                os.rename(tmp, destination)
            except OSError:
                os.remove(tmp)

    def load_config_file(config, path=dask_config_path):
        if not os.path.exists(path):
            path = default_path
        with open(path) as f:
            text = f.read()
            config.update(yaml.load(text))

    try:
        ensure_config_file()
        load_config_file(config)
    except OSError as e:
        logger.warn("Could not write default config file to %s. Received error %s",
                    default_path, e)


def load_env_vars(config):
    for name, value in os.environ.items():
        if name.startswith('DASK_'):
            varname = name[5:].lower().replace('_', '-')
            config[varname] = value


load_env_vars(config)


def initialize_logging(config):
    loggers = config.get('logging', {})
    loggers.setdefault('distributed', 'info')
    # http://stackoverflow.com/questions/21234772/python-tornado-disable-logging-to-stderr
    loggers.setdefault('tornado', 'critical')
    loggers.setdefault('tornado.application', 'error')

    fmt = '%(name)s - %(levelname)s - %(message)s'

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(fmt))
    for name, level in loggers.items():
        LEVEL = logging_names[level.upper()]
        logger = logging.getLogger(name)
        logger.setLevel(LEVEL)
        logger.addHandler(handler)
        logger.propagate = False


initialize_logging(config)
