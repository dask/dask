import atexit
import logging
import os
import shutil
import sys
import filecmp
from importlib import import_module

import click

from .utils import import_file

logger = logging.getLogger(__name__)


def validate_preload_argv(ctx, param, value):
    """Click option callback providing validation of preload subcommand arguments."""
    if not value and not ctx.params.get("preload", None):
        # No preload argv provided and no preload modules specified.
        return value

    if value and not ctx.params.get("preload", None):
        # Report a usage error matching standard click error conventions.
        unexpected_args = [v for v in value if v.startswith("-")]
        for a in unexpected_args:
            raise click.NoSuchOption(a)
        raise click.UsageError(
            "Got unexpected extra argument%s: (%s)" %
            ("s" if len(value) > 1 else "", " ".join(value))
        )

    preload_modules = _import_modules(ctx.params.get("preload"))

    preload_commands = [
        m["dask_setup"] for m in preload_modules.values()
        if isinstance(m["dask_setup"], click.Command)
    ]

    if len(preload_commands) > 1:
        raise click.UsageError(
            "Multiple --preload modules with click-configurable setup: %s" %
            list(preload_modules.keys()))

    if value and not preload_commands:
        raise click.UsageError(
            "Unknown argument specified: %r Was click-configurable --preload target provided?")
    if not preload_commands:
        return value
    else:
        preload_command = preload_commands[0]

    ctx = click.Context(preload_command, allow_extra_args=False)
    preload_command.parse_args(ctx, list(value))

    return value


def _import_modules(names, file_dir=None):
    """ Imports modules and extracts preload interface functions.

    Imports modules specified by names and extracts 'dask_setup'
    and 'dask_teardown' if present.


    Parameters
    ----------
    names: list of strings
        Module names or file paths
    file_dir: string
        Path of a directory where files should be copied

    Returns
    -------
    Nest dict of names to extracted module interface components if present
    in imported module.
    """
    result_modules = {}

    for name in names:
        # import
        if name.endswith(".py"):
            # name is a file path
            if file_dir is not None:
                basename = os.path.basename(name)
                copy_dst = os.path.join(file_dir, basename)
                if os.path.exists(copy_dst):
                    if not filecmp.cmp(name, copy_dst):
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

        result_modules[name] = {
            attrname : getattr(module, attrname, None)
            for attrname in ("dask_setup", "dask_teardown")
        }

    return result_modules


def preload_modules(names, parameter=None, file_dir=None, argv=None):
    """ Imports modules, handles `dask_setup` and `dask_teardown`.

    Parameters
    ----------
    names: list of strings
        Module names or file paths
    parameter: object
        Parameter passed to `dask_setup` and `dask_teardown`
    argv: [string]
        List of string arguments passed to click-configurable `dask_setup`.
    file_dir: string
        Path of a directory where files should be copied
    """

    imported_modules = _import_modules(names, file_dir=file_dir)

    for name, interface in imported_modules.items():
        dask_setup = interface.get("dask_setup", None)
        dask_teardown = interface.get("dask_teardown", None)

        if dask_setup:
            if isinstance(dask_setup, click.Command):
                context = dask_setup.make_context("dask_setup", list(argv), allow_extra_args=False)
                dask_setup.callback(parameter, *context.args, **context.params)
            else:
                dask_setup(parameter)

        if interface["dask_teardown"]:
            atexit.register(interface["dask_teardown"], parameter)
