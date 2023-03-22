from __future__ import annotations

import ast
import base64
import builtins  # Explicitly use builtins.set as 'set' will be shadowed by a function
import json
import os
import site
import sys
import threading
import warnings
from collections.abc import Mapping, Sequence
from typing import Any, Literal

import yaml

no_default = "__no_default__"


def _get_paths():
    """Get locations to search for YAML configuration files.

    This logic exists as a separate function for testing purposes.
    """

    paths = [
        os.getenv("DASK_ROOT_CONFIG", "/etc/dask"),
        os.path.join(sys.prefix, "etc", "dask"),
        *[os.path.join(prefix, "etc", "dask") for prefix in site.PREFIXES],
        os.path.join(os.path.expanduser("~"), ".config", "dask"),
    ]
    if "DASK_CONFIG" in os.environ:
        paths.append(os.environ["DASK_CONFIG"])

    # Remove duplicate paths while preserving ordering
    paths = list(reversed(list(dict.fromkeys(reversed(paths)))))

    return paths


paths = _get_paths()

if "DASK_CONFIG" in os.environ:
    PATH = os.environ["DASK_CONFIG"]
else:
    PATH = os.path.join(os.path.expanduser("~"), ".config", "dask")


config: dict = {}
global_config = config  # alias


config_lock = threading.Lock()


defaults: list[Mapping] = []


def canonical_name(k: str, config: dict) -> str:
    """Return the canonical name for a key.

    Handles user choice of '-' or '_' conventions by standardizing on whichever
    version was set first. If a key already exists in either hyphen or
    underscore form, the existing version is the canonical name. If neither
    version exists the original key is used as is.
    """
    try:
        if k in config:
            return k
    except TypeError:
        # config is not a mapping, return the same name as provided
        return k

    altk = k.replace("_", "-") if "_" in k else k.replace("-", "_")

    if altk in config:
        return altk

    return k


def update(old: dict, new: Mapping, priority: Literal["old", "new"] = "new") -> dict:
    """Update a nested dictionary with values from another

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
        k = canonical_name(k, old)

        if isinstance(v, Mapping):
            if k not in old or old[k] is None:
                old[k] = {}
            update(old[k], v, priority=priority)
        else:
            if priority == "new" or k not in old:
                old[k] = v

    return old


def merge(*dicts: Mapping) -> dict:
    """Update a sequence of nested dictionaries

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
    result: dict = {}
    for d in dicts:
        update(result, d)
    return result


def _load_config_file(path: str) -> dict | None:
    """A helper for loading a config file from a path, and erroring
    appropriately if the file is malformed."""
    try:
        with open(path, "rb") as f:
            config = yaml.safe_load(f.read())
    except OSError:
        # Ignore permission errors
        return None
    except Exception as exc:
        raise ValueError(
            f"A dask config file at {path!r} is malformed, original error "
            f"message:\n\n{exc}"
        ) from None
    if config is not None and not isinstance(config, dict):
        raise ValueError(
            f"A dask config file at {path!r} is malformed - config files must have "
            f"a dict as the top level object, got a {type(config).__name__} instead"
        )
    return config


def collect_yaml(paths: Sequence[str] = paths) -> list[dict]:
    """Collect configuration from yaml files

    This searches through a list of paths, expands to find all yaml or json
    files, and then parses each file.
    """
    # Find all paths
    file_paths = []
    for path in paths:
        if os.path.exists(path):
            if os.path.isdir(path):
                try:
                    file_paths.extend(
                        sorted(
                            os.path.join(path, p)
                            for p in os.listdir(path)
                            if os.path.splitext(p)[1].lower()
                            in (".json", ".yaml", ".yml")
                        )
                    )
                except OSError:
                    # Ignore permission errors
                    pass
            else:
                file_paths.append(path)

    configs = []

    # Parse yaml files
    for path in file_paths:
        config = _load_config_file(path)
        if config is not None:
            configs.append(config)

    return configs


def collect_env(env: Mapping[str, str] | None = None) -> dict:
    """Collect config from environment variables

    This grabs environment variables of the form "DASK_FOO__BAR_BAZ=123" and
    turns these into config variables of the form ``{"foo": {"bar-baz": 123}}``
    It transforms the key and value in the following way:

    -  Lower-cases the key text
    -  Treats ``__`` (double-underscore) as nested access
    -  Calls ``ast.literal_eval`` on the value

    Any serialized config passed via ``DASK_INTERNAL_INHERIT_CONFIG`` is also set here.

    """

    if env is None:
        env = os.environ

    if "DASK_INTERNAL_INHERIT_CONFIG" in env:
        d = deserialize(env["DASK_INTERNAL_INHERIT_CONFIG"])
    else:
        d = {}

    for name, value in env.items():
        if name.startswith("DASK_"):
            varname = name[5:].lower().replace("__", ".")
            try:
                d[varname] = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                d[varname] = value

    result: dict = {}
    set(d, config=result)
    return result


def ensure_file(
    source: str, destination: str | None = None, comment: bool = True
) -> None:
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
            os.makedirs(directory, exist_ok=True)

            # Atomically create destination.  Parallel testing discovered
            # a race condition where a process can be busy creating the
            # destination while another process reads an empty config file.
            tmp = "%s.tmp.%d" % (destination, os.getpid())
            with open(source, "rb") as f:
                lines = list(f)

            if comment:
                lines = [
                    b"# " + line if line.strip() and not line.startswith(b"#") else line
                    for line in lines
                ]

            with open(tmp, "wb") as f:
                f.write(b"".join(lines))

            try:
                os.rename(tmp, destination)
            except OSError:
                os.remove(tmp)
    except OSError:
        pass


class set:
    """Temporarily set configuration values within a context manager

    Parameters
    ----------
    arg : mapping or None, optional
        A mapping of configuration key-value pairs to set.
    **kwargs :
        Additional key-value pairs to set. If ``arg`` is provided, values set
        in ``arg`` will be applied before those in ``kwargs``.
        Double-underscores (``__``) in keyword arguments will be replaced with
        ``.``, allowing nested values to be easily set.

    Examples
    --------
    >>> import dask

    Set ``'foo.bar'`` in a context, by providing a mapping.

    >>> with dask.config.set({'foo.bar': 123}):
    ...     pass

    Set ``'foo.bar'`` in a context, by providing a keyword argument.

    >>> with dask.config.set(foo__bar=123):
    ...     pass

    Set ``'foo.bar'`` globally.

    >>> dask.config.set(foo__bar=123)  # doctest: +SKIP

    See Also
    --------
    dask.config.get
    """

    config: dict
    # [(op, path, value), ...]
    _record: list[tuple[Literal["insert", "replace"], tuple[str, ...], Any]]

    def __init__(
        self,
        arg: Mapping | None = None,
        config: dict = config,
        lock: threading.Lock = config_lock,
        **kwargs,
    ):
        with lock:
            self.config = config
            self._record = []

            if arg is not None:
                for key, value in arg.items():
                    key = check_deprecations(key)
                    self._assign(key.split("."), value, config)
            if kwargs:
                for key, value in kwargs.items():
                    key = key.replace("__", ".")
                    key = check_deprecations(key)
                    self._assign(key.split("."), value, config)

    def __enter__(self):
        return self.config

    def __exit__(self, type, value, traceback):
        for op, path, value in reversed(self._record):
            d = self.config
            if op == "replace":
                for key in path[:-1]:
                    d = d.setdefault(key, {})
                d[path[-1]] = value
            else:  # insert
                for key in path[:-1]:
                    try:
                        d = d[key]
                    except KeyError:
                        break
                else:
                    d.pop(path[-1], None)

    def _assign(
        self,
        keys: Sequence[str],
        value: Any,
        d: dict,
        path: tuple[str, ...] = (),
        record: bool = True,
    ) -> None:
        """Assign value into a nested configuration dictionary

        Parameters
        ----------
        keys : Sequence[str]
            The nested path of keys to assign the value.
        value : object
        d : dict
            The part of the nested dictionary into which we want to assign the
            value
        path : tuple[str], optional
            The path history up to this point.
        record : bool, optional
            Whether this operation needs to be recorded to allow for rollback.
        """
        key = canonical_name(keys[0], d)

        path = path + (key,)

        if len(keys) == 1:
            if record:
                if key in d:
                    self._record.append(("replace", path, d[key]))
                else:
                    self._record.append(("insert", path, None))
            d[key] = value
        else:
            if key not in d:
                if record:
                    self._record.append(("insert", path, None))
                d[key] = {}
                # No need to record subsequent operations after an insert
                record = False
            self._assign(keys[1:], value, d[key], path, record=record)


def collect(paths: list[str] = paths, env: Mapping[str, str] | None = None) -> dict:
    """
    Collect configuration from paths and environment variables

    Parameters
    ----------
    paths : list[str]
        A list of paths to search for yaml config files

    env : Mapping[str, str]
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

    configs = collect_yaml(paths=paths)
    configs.append(collect_env(env=env))

    return merge(*configs)


def refresh(
    config: dict = config, defaults: list[Mapping] = defaults, **kwargs
) -> None:
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
        update(config, d, priority="old")

    update(config, collect(**kwargs))


def get(
    key: str,
    default: Any = no_default,
    config: dict = config,
    override_with: Any | None = None,
) -> Any:
    """
    Get elements from global config

    If ``override_with`` is not None this value will be passed straight back.
    Useful for getting kwarg defaults from Dask config.

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

    >>> config.get('foo.y', override_with=None)  # doctest: +SKIP
    2

    >>> config.get('foo.y', override_with=3)  # doctest: +SKIP
    3

    See Also
    --------
    dask.config.set
    """
    if override_with is not None:
        return override_with
    keys = key.split(".")
    result = config
    for k in keys:
        k = canonical_name(k, result)
        try:
            result = result[k]
        except (TypeError, IndexError, KeyError):
            if default is not no_default:
                return default
            else:
                raise
    return result


def rename(aliases: Mapping, config: dict = config) -> None:
    """Rename old keys to new keys

    This helps migrate older configuration versions over time
    """
    old = []
    new = {}
    for o, n in aliases.items():
        value = get(o, None, config=config)
        if value is not None:
            old.append(o)
            new[n] = value

    for k in old:
        del config[canonical_name(k, config)]  # TODO: support nested keys

    set(new, config=config)


def update_defaults(
    new: Mapping, config: dict = config, defaults: list[Mapping] = defaults
) -> None:
    """Add a new set of defaults to the configuration

    It does two things:

    1.  Add the defaults to a global collection to be used by refresh later
    2.  Updates the global config with the new configuration
        prioritizing older values over newer ones
    """
    defaults.append(new)
    update(config, new, priority="old")


def expand_environment_variables(config: Any) -> Any:
    """Expand environment variables in a nested config dictionary

    This function will recursively search through any nested dictionaries
    and/or lists.

    Parameters
    ----------
    config : dict, iterable, or str
        Input object to search for environment variables

    Returns
    -------
    config : same type as input

    Examples
    --------
    >>> expand_environment_variables({'x': [1, 2, '$USER']})  # doctest: +SKIP
    {'x': [1, 2, 'my-username']}
    """
    if isinstance(config, Mapping):
        return {k: expand_environment_variables(v) for k, v in config.items()}
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, builtins.set)):
        return type(config)(expand_environment_variables(v) for v in config)
    else:
        return config


deprecations = {
    "fuse_ave_width": "optimization.fuse.ave-width",
    "fuse_max_height": "optimization.fuse.max-height",
    "fuse_max_width": "optimization.fuse.max-width",
    "fuse_subgraphs": "optimization.fuse.subgraphs",
    "fuse_rename_keys": "optimization.fuse.rename-keys",
    "fuse_max_depth_new_edges": "optimization.fuse.max-depth-new-edges",
    # See https://github.com/dask/distributed/pull/4916
    "ucx.cuda_copy": "distributed.ucx.cuda_copy",
    "ucx.tcp": "distributed.ucx.tcp",
    "ucx.nvlink": "distributed.ucx.nvlink",
    "ucx.infiniband": "distributed.ucx.infiniband",
    "ucx.rdmacm": "distributed.ucx.rdmacm",
    "ucx.net-devices": "distributed.ucx.net-devices",
    "ucx.reuse-endpoints": "distributed.ucx.reuse-endpoints",
    "rmm.pool-size": "distributed.rmm.pool-size",
    "shuffle": "dataframe.shuffle.algorithm",
    "dataframe.shuffle.algorithm": "dataframe.shuffle.method",
    "dataframe.shuffle-compression": "dataframe.shuffle.compression",
}


def check_deprecations(key: str, deprecations: dict = deprecations) -> str:
    """Check if the provided value has been renamed or removed

    Parameters
    ----------
    key : str
        The configuration key to check
    deprecations : Dict[str, str]
        The mapping of aliases

    Examples
    --------
    >>> deprecations = {"old_key": "new_key", "invalid": None}
    >>> check_deprecations("old_key", deprecations=deprecations)  # doctest: +SKIP
    UserWarning: Configuration key "old_key" has been deprecated. Please use "new_key" instead.

    >>> check_deprecations("invalid", deprecations=deprecations)
    Traceback (most recent call last):
        ...
    ValueError: Configuration value "invalid" has been removed

    >>> check_deprecations("another_key", deprecations=deprecations)
    'another_key'

    Returns
    -------
    new: str
        The proper key, whether the original (if no deprecation) or the aliased
        value
    """
    if key in deprecations:
        new = deprecations[key]
        if new:
            warnings.warn(
                'Configuration key "{}" has been deprecated. '
                'Please use "{}" instead'.format(key, new)
            )
            return new
        else:
            raise ValueError(f'Configuration value "{key}" has been removed')
    else:
        return key


def serialize(data: Any) -> str:
    """Serialize config data into a string.

    Typically used to pass config via the ``DASK_INTERNAL_INHERIT_CONFIG`` environment variable.

    Parameters
    ----------
    data: json-serializable object
        The data to serialize

    Returns
    -------
    serialized_data: str
        The serialized data as a string

    """
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def deserialize(data: str) -> Any:
    """De-serialize config data into the original object.

    Typically when receiving config via the ``DASK_INTERNAL_INHERIT_CONFIG`` environment variable.

    Parameters
    ----------
    data: str
        String serialized by :func:`dask.config.serialize`

    Returns
    -------
    deserialized_data: obj
        The de-serialized data

    """
    return json.loads(base64.urlsafe_b64decode(data.encode()).decode())


def _initialize() -> None:
    fn = os.path.join(os.path.dirname(__file__), "dask.yaml")

    with open(fn, "rb") as f:
        _defaults = yaml.safe_load(f)

    update_defaults(_defaults)


refresh()
_initialize()
