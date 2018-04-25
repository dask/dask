from __future__ import print_function, division, absolute_import

import ast
from contextlib import contextmanager
import os
import sys


no_default = '__no_default__'


config_paths = [
    '/etc/dask',
    os.path.join(sys.prefix, 'etc', 'dask'),
    os.path.join(os.path.expanduser('~'), '.config', 'dask'),
    os.path.join(os.path.expanduser('~'), '.dask')
]

if 'DASK_CONFIG' in os.environ:
    config_paths.append(os.environ['DASK_CONFIG'])


def update(old, new):
    """ Update a nested dictionary with values from another

    This is like dict.update except that it smoothly merges nested values

    This operates in-place and modifies old

    Example
    -------
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'y': {'b': 3}}
    >>> update(a, b)
    {'x': 1, 'y': {'a': 2, 'b': 3}}

    See Also
    --------
    merge
    """
    for k, v in new.items():
        if k not in old and type(v) is dict:
            old[k] = {}

        if type(v) is dict:
            update(old[k], v)
        else:
            old[k] = v
    return old


def merge(*dicts):
    """ Update a sequence of nested dictionaries

    This prefers the values in the latter dictionaries to those in the former

    Example
    -------
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'y': {'b': 3}}
    >>> merge(a, b)
    {'x': 1, 'y': {'a': 2, 'b': 3}}

    See Also
    --------
    update
    """
    result = {}
    for d in dicts:
        update(result, d)
    return result


def collect_yaml(paths=config_paths):
    """ Collect configuration from yaml files

    This searches through ``config_paths``, expands to find all yaml or json
    files, and then parses each file.
    """
    # Find all paths
    file_paths = []
    for path in paths:
        if os.path.exists(path):
            if os.path.isdir(path):
                file_paths.extend(sorted([
                    p for p in os.listdir(path)
                    if os.splitext(p)[1].lower() in ('json', 'yaml', 'yml')
                ]))
            else:
                file_paths.append(path)

    configs = []

    # Parse yaml files
    for path in file_paths:
        with open(path) as f:
            data = yaml.load(f.read()) or {}
            configs.append(data)

    return configs


def collect_env():
    """ Collect config from environment variables

    This grabs environment variables of the form "DASK_FOO_BAR=123"
    and turns these into config variables of the form ``{"foo-bar": 123}``
    """
    env = {}
    for name, value in os.environ.items():
        if name.startswith('DASK_'):
            varname = name[5:].lower().replace('_', '-')
            try:
                env[varname] = ast.literal_eval(value)
            except ValueError:
                env[varname] = value

    return env


def ensure_config_file(
        source,
        destination=os.path.join(os.path.expanduser('~'), '.config', 'dask')):
    """ Copy file to default location if it does not already exist

    This tries to move a default configuration files to a default location if
    if does not already exist.

    This is to be used by downstream modules (like dask.distributed) that may
    have default configuration files that they wish to include in the default
    configuration path.
    """
    if not os.path.exists(destination):
        import shutil
        if not os.path.exists(os.path.dirname(destination)):
            os.makedirs(os.path.dirname(destination), exists_ok=True)
        # Atomically create destination.  Parallel testing discovered
        # a race condition where a process can be busy creating the
        # destination while another process reads an empty config file.
        tmp = '%s.tmp.%d' % (destination, os.getpid())
        shutil.copy(source, tmp)
        try:
            os.rename(tmp, destination)
        except OSError:
            os.remove(tmp)


@contextmanager
def set_config(arg=None, **kwargs):
    """ Temporarily set configuration values within a context manager

    Examples
    --------
    >>> with set_config({'foo': 123}):
    ...     pass
    """
    if arg and not kwargs:
        kwargs = arg

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


configs = []

try:
    import yaml
except ImportError:
    configs.extend(collect_yaml())

configs.append(collect_env())

config = merge(*configs)


def get(key, default=no_default, config=config):
    """
    Get elements from global config

    Use '.' for nested access

    Examples
    --------
    >>> from dask import config
    >>> config.get('foo')
    {'x': 1, 'y': 2}

    >>> config.get('foo.x')
    1

    >>> config.get('foo.x.y', default=123)
    123
    """
    keys = key.split('.')
    result = config
    for k in keys:
        try:
            result = result[k]
        except (TypeError, IndexError, KeyError):
            if default is not no_default:
                return default
            else:
                raise
    return result
