from __future__ import print_function, division, absolute_import

import ast
import os
import sys
import threading
try:
    import yaml
except ImportError:
    yaml = None

from .compatibility import makedirs


no_default = '__no_default__'


paths = [
    '/etc/dask',
    os.path.join(sys.prefix, 'etc', 'dask'),
    os.path.join(os.path.expanduser('~'), '.config', 'dask'),
    os.path.join(os.path.expanduser('~'), '.dask')
]

if 'DASK_CONFIG' in os.environ:
    PATH = os.environ['DASK_CONFIG']
    paths.append(PATH)
else:
    PATH = os.path.join(os.path.expanduser('~'), '.config', 'dask')


global_config = config = {}


config_lock = threading.Lock()


defaults = []


def update(old, new, priority='new'):
    """ Update a nested dictionary with values from another

    This is like dict.update except that it smoothly merges nested values

    This operates in-place and modifies old

    Parameters
    ----------
    priority: string {'old', 'new'}
        If new (default) then the new dictionary has preference.
        Otherwise the old dictionary does.

    Examples
    --------
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'x': 2, 'y': {'b': 3}}
    >>> update(a, b)  # doctest: +SKIP
    {'x': 2, 'y': {'a': 2, 'b': 3}}

    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'x': 2, 'y': {'b': 3}}
    >>> update(a, b, priority='old')  # doctest: +SKIP
    {'x': 1, 'y': {'a': 2, 'b': 3}}

    See Also
    --------
    dask.config.merge
    """
    for k, v in new.items():
        if k not in old and type(v) is dict:
            old[k] = {}

        if type(v) is dict:
            update(old[k], v, priority=priority)
        else:
            if priority == 'new' or k not in old:
                old[k] = v

    return old


def merge(*dicts):
    """ Update a sequence of nested dictionaries

    This prefers the values in the latter dictionaries to those in the former

    Examples
    --------
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'y': {'b': 3}}
    >>> merge(a, b)  # doctest: +SKIP
    {'x': 1, 'y': {'a': 2, 'b': 3}}

    See Also
    --------
    dask.config.update
    """
    result = {}
    for d in dicts:
        update(result, d)
    return result


def collect_yaml(paths=paths):
    """ Collect configuration from yaml files

    This searches through a list of paths, expands to find all yaml or json
    files, and then parses each file.
    """
    # Find all paths
    file_paths = []
    for path in paths:
        if os.path.exists(path):
            if os.path.isdir(path):
                file_paths.extend(sorted([
                    os.path.join(path, p)
                    for p in os.listdir(path)
                    if os.path.splitext(p)[1].lower() in ('.json', '.yaml', '.yml')
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


def collect_env(env=None):
    """ Collect config from environment variables

    This grabs environment variables of the form "DASK_FOO__BAR_BAZ=123" and
    turns these into config variables of the form ``{"foo": {"bar-baz": 123}}``
    It transforms the key and value in the following way:

    -  Lower-cases the key text
    -  Treats ``__`` (double-underscore) as nested access
    -  Replaces ``_`` (underscore) with a hyphen.
    -  Calls ``ast.literal_eval`` on the value
    """
    if env is None:
        env = os.environ
    d = {}
    for name, value in env.items():
        if name.startswith('DASK_'):
            varname = name[5:].lower().replace('__', '.').replace('_', '-')
            try:
                d[varname] = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                d[varname] = value

    result = {}
    set(d, config=result)

    return result


def ensure_file(
        source,
        destination=None,
        comment=True):
    """
    Copy file to default location if it does not already exist

    This tries to move a default configuration file to a default location if
    if does not already exist.  It also comments out that file by default.

    This is to be used by downstream modules (like dask.distributed) that may
    have default configuration files that they wish to include in the default
    configuration path.

    Parameters
    ----------
    source : string, filename
        Source configuration file, typically within a source directory.
    destination : string, directory
        Destination directory. Configurable by ``DASK_CONFIG`` environment
        variable, falling back to ~/.config/dask.
    comment : bool, True by default
        Whether or not to comment out the config file when copying.
    """
    if destination is None:
        destination = PATH

    # destination is a file and already exists, never overwrite
    if os.path.isfile(destination):
        return

    # If destination is not an existing file, interpret as a directory,
    # use the source basename as the filename
    directory = destination
    destination = os.path.join(directory, os.path.basename(source))

    try:
        if not os.path.exists(destination):
            makedirs(directory, exist_ok=True)

            # Atomically create destination.  Parallel testing discovered
            # a race condition where a process can be busy creating the
            # destination while another process reads an empty config file.
            tmp = '%s.tmp.%d' % (destination, os.getpid())
            with open(source) as f:
                lines = list(f)

            if comment:
                lines = ['# ' + line
                         if line.strip() and not line.startswith('#')
                         else line
                         for line in lines]

            with open(tmp, 'w') as f:
                f.write(''.join(lines))

            try:
                os.rename(tmp, destination)
            except OSError:
                os.remove(tmp)
    except OSError:
        pass


class set(object):
    """ Temporarily set configuration values within a context manager

    Examples
    --------
    >>> import dask
    >>> with dask.config.set({'foo': 123}):
    ...     pass

    See Also
    --------
    dask.config.get
    """
    def __init__(self, arg=None, config=config, lock=config_lock, **kwargs):
        if arg and not kwargs:
            kwargs = arg

        with lock:
            self.config = config
            self.old = {}

            for key, value in kwargs.items():
                self._assign(key.split('.'), value, config, old=self.old)

    def __enter__(self):
        return self.config

    def __exit__(self, type, value, traceback):
        for keys, value in self.old.items():
            if value == '--delete--':
                d = self.config
                try:
                    while len(keys) > 1:
                        d = d[keys[0]]
                        keys = keys[1:]
                    del d[keys[0]]
                except KeyError:
                    pass
            else:
                self._assign(keys, value, self.config)

    @classmethod
    def _assign(cls, keys, value, d, old=None, path=[]):
        """ Assign value into a nested configuration dictionary

        Optionally record the old values in old

        Parameters
        ----------
        keys: Sequence[str]
            The nested path of keys to assign the value, similar to toolz.put_in
        value: object
        d: dict
            The part of the nested dictionary into which we want to assign the
            value
        old: dict, optional
            If provided this will hold the old values
        path: List[str]
            Used internally to hold the path of old values
        """
        if len(keys) == 1:
            if old is not None:
                path_key = tuple(path + [keys[0]])
                if keys[0] in d:
                    old[path_key] = d[keys[0]]
                else:
                    old[path_key] = '--delete--'
            d[keys[0]] = value
        else:
            key = keys[0]
            if key not in d:
                d[key] = {}
                if old is not None:
                    old[tuple(path + [key])] = '--delete--'
                old = None
            cls._assign(keys[1:], value, d[key], path=path + [key], old=old)


def collect(paths=paths, env=None):
    """
    Collect configuration from paths and environment variables

    Parameters
    ----------
    paths : List[str]
        A list of paths to search for yaml config files

    env : dict
        The system environment variables

    Returns
    -------
    config: dict

    See Also
    --------
    dask.config.refresh: collect configuration and update into primary config
    """
    if env is None:
        env = os.environ
    configs = []

    if yaml:
        configs.extend(collect_yaml(paths=paths))

    configs.append(collect_env(env=env))

    return merge(*configs)


def refresh(config=config, defaults=defaults, **kwargs):
    """
    Update configuration by re-reading yaml files and env variables

    This mutates the global dask.config.config, or the config parameter if
    passed in.

    This goes through the following stages:

    1.  Clearing out all old configuration
    2.  Updating from the stored defaults from downstream libraries
        (see update_defaults)
    3.  Updating from yaml files and environment variables

    Note that some functionality only checks configuration once at startup and
    may not change behavior, even if configuration changes.  It is recommended
    to restart your python process if convenient to ensure that new
    configuration changes take place.

    See Also
    --------
    dask.config.collect: for parameters
    dask.config.update_defaults
    """
    config.clear()

    for d in defaults:
        update(config, d, priority='old')

    update(config, collect(**kwargs))


def get(key, default=no_default, config=config):
    """
    Get elements from global config

    Use '.' for nested access

    Examples
    --------
    >>> from dask import config
    >>> config.get('foo')  # doctest: +SKIP
    {'x': 1, 'y': 2}

    >>> config.get('foo.x')  # doctest: +SKIP
    1

    >>> config.get('foo.x.y', default=123)  # doctest: +SKIP
    123

    See Also
    --------
    dask.config.set
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


def rename(aliases, config=config):
    """ Rename old keys to new keys

    This helps migrate older configuration versions over time
    """
    old = list()
    new = dict()
    for o, n in aliases.items():
        value = get(o, None, config=config)
        if value is not None:
            old.append(o)
            new[n] = value

    for k in old:
        del config[k]  # TODO: support nested keys

    set(new, config=config)


def update_defaults(new, config=config, defaults=defaults):
    """ Add a new set of defaults to the configuration

    It does two things:

    1.  Add the defaults to a global collection to be used by refresh later
    2.  Updates the global config with the new configuration
        prioritizing older values over newer ones
    """
    defaults.append(new)
    update(config, new, priority='old')


refresh()
