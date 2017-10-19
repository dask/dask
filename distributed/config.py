from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
import logging
import logging.config
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


def _initialize_logging_old_style(config):
    """
    Initialize logging using the "old-style" configuration scheme, e.g.:
        {
        'logging': {
            'distributed': 'info',
            'tornado': 'critical',
            'tornado.application': 'error',
            }
        }
    """
    loggers = config.get('logging', {})
    loggers.setdefault('distributed', 'info')
    # We could remove those lines and let the default config.yaml handle it
    loggers.setdefault('tornado', 'critical')
    loggers.setdefault('tornado.application', 'error')

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(log_format))
    for name, level in loggers.items():
        if isinstance(level, str):
            level = logging_names[level.upper()]
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.handlers[:] = []
        logger.addHandler(handler)
        logger.propagate = False


def _initialize_logging_new_style(config):
    """
    Initialize logging using logging's "Configuration dictionary schema".
    (ref.: https://docs.python.org/2/library/logging.config.html#logging-config-dictschema)
    """
    logging.config.dictConfig(config['logging'])


def _initialize_logging_file_config(config):
    """
    Initialize logging using logging's "Configuration file format".
    (ref.: https://docs.python.org/2/library/logging.config.html#configuration-file-format)
    """
    logging.config.fileConfig(config['logging-file-config'], disable_existing_loggers=False)


def initialize_logging(config):
    if 'logging-file-config' in config:
        if 'logging' in config:
            raise RuntimeError("Config options 'logging-file-config' and 'logging' are mutually exclusive.")
        _initialize_logging_file_config(config)
    else:
        log_config = config.get('logging', {})
        if 'version' in log_config:
            # logging module mandates version to be an int
            log_config['version'] = int(log_config['version'])
            _initialize_logging_new_style(config)
        else:
            _initialize_logging_old_style(config)


@contextmanager
def set_config(**kwargs):
    old = {}
    for key in kwargs:
        if key in config:
            old[key] = config[key]

    for key, value in kwargs.items():
        config[key] = value

    try:
        yield
    finally:
        for key in kwargs:
            if key in old:
                config[key] = old[key]
            else:
                del config[key]


try:
    import yaml
except ImportError:
    pass
else:
    path = determine_config_file()
    load_config_file(config, path)

load_env_vars(config)

log_format = config.get('log-format', '%(name)s - %(levelname)s - %(message)s')

initialize_logging(config)
