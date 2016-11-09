from __future__ import print_function, division, absolute_import

import logging
import os
import sys

from .compatibility import FileExistsError, logging_names

try:
    import yaml
except ImportError:
    config = {}
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
            shutil.copy(default_path, destination)


    def load_config_file(path=dask_config_path):
        if not os.path.exists(path):
            path = default_path
        with open(path) as f:
            text = f.read()
            return yaml.load(text)


    ensure_config_file()
    config = load_config_file()


def initialize_logging(config):
    loggers = config.get('logging', {})
    loggers.setdefault('distributed', 'info')

    fmt = '%(name)s - %(levelname)s - %(message)s'

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(fmt))
    for name, level in loggers.items():
        LEVEL = logging_names[level.upper()]
        logger = logging.getLogger(name)
        logger.setLevel(LEVEL)
        logger.addHandler(handler)
        logger.propagate = False

    # http://stackoverflow.com/questions/21234772/python-tornado-disable-logging-to-stderr
    # XXX this silences legitimate error messages
    logging.getLogger('tornado').setLevel(logging.CRITICAL)


initialize_logging(config)
