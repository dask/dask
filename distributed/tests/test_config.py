from __future__ import print_function, division, absolute_import

import logging
import subprocess
import sys
import tempfile
import os

import pytest

from distributed.utils_test import (
    captured_handler,
    captured_logger,
    new_config,
    new_config_file,
)
from distributed.config import initialize_logging


def dump_logger_list():
    root = logging.getLogger()
    loggers = root.manager.loggerDict
    print()
    print("== Loggers (name, level, effective level, propagate) ==")

    def logger_info(name, logger):
        return (
            name,
            logging.getLevelName(logger.level),
            logging.getLevelName(logger.getEffectiveLevel()),
            logger.propagate,
        )

    infos = []
    infos.append(logger_info("<root>", root))

    for name, logger in sorted(loggers.items()):
        if not isinstance(logger, logging.Logger):
            # Skip 'PlaceHolder' objects
            continue
        assert logger.name == name
        infos.append(logger_info(name, logger))

    for info in infos:
        print("%-40s %-8s %-8s %-5s" % info)

    print()


def test_logging_default():
    """
    Test default logging configuration.
    """
    d = logging.getLogger("distributed")
    assert len(d.handlers) == 1
    assert isinstance(d.handlers[0], logging.StreamHandler)

    # Work around Bokeh messing with the root logger level
    # https://github.com/bokeh/bokeh/issues/5793
    root = logging.getLogger("")
    old_root_level = root.level
    root.setLevel("WARN")

    for handler in d.handlers:
        handler.setLevel("INFO")

    try:
        dfb = logging.getLogger("distributed.foo.bar")
        f = logging.getLogger("foo")
        fb = logging.getLogger("foo.bar")

        with captured_handler(d.handlers[0]) as distributed_log:
            with captured_logger(root, level=logging.ERROR) as foreign_log:
                h = logging.StreamHandler(foreign_log)
                fmt = "[%(levelname)s in %(name)s] - %(message)s"
                h.setFormatter(logging.Formatter(fmt))
                fb.addHandler(h)
                fb.propagate = False

                # For debugging
                dump_logger_list()

                d.debug("1: debug")
                d.info("2: info")
                dfb.info("3: info")
                fb.info("4: info")
                fb.error("5: error")
                f.info("6: info")
                f.error("7: error")

        distributed_log = distributed_log.getvalue().splitlines()
        foreign_log = foreign_log.getvalue().splitlines()

        # distributed log is configured at INFO level by default
        assert distributed_log == [
            "distributed - INFO - 2: info",
            "distributed.foo.bar - INFO - 3: info",
        ]

        # foreign logs should be unaffected by distributed's logging
        # configuration.  They get the default ERROR level from logging.
        assert foreign_log == ["[ERROR in foo.bar] - 5: error", "7: error"]

    finally:
        root.setLevel(old_root_level)


def test_logging_empty_simple():
    with new_config({}):
        test_logging_default()


def test_logging_simple():
    """
    Test simple ("old-style") logging configuration.
    """
    c = {"logging": {"distributed.foo": "info", "distributed.foo.bar": "error"}}
    # Must test using a subprocess to avoid wrecking pre-existing configuration
    with new_config_file(c):
        code = """if 1:
            import logging
            import dask

            from distributed.utils_test import captured_handler

            d = logging.getLogger('distributed')
            assert len(d.handlers) == 1
            assert isinstance(d.handlers[0], logging.StreamHandler)
            df = logging.getLogger('distributed.foo')
            dfb = logging.getLogger('distributed.foo.bar')

            with captured_handler(d.handlers[0]) as distributed_log:
                df.info("1: info")
                dfb.warning("2: warning")
                dfb.error("3: error")

            distributed_log = distributed_log.getvalue().splitlines()

            assert distributed_log == [
                "distributed.foo - INFO - 1: info",
                "distributed.foo.bar - ERROR - 3: error",
                ], (dask.config.config, distributed_log)
            """

        subprocess.check_call([sys.executable, "-c", code])


def test_logging_extended():
    """
    Test extended ("new-style") logging configuration.
    """
    c = {
        "logging": {
            "version": "1",
            "formatters": {
                "simple": {"format": "%(levelname)s: %(name)s: %(message)s"}
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                    "formatter": "simple",
                }
            },
            "loggers": {
                "distributed.foo": {
                    "level": "INFO",
                    #'handlers': ['console'],
                },
                "distributed.foo.bar": {
                    "level": "ERROR",
                    #'handlers': ['console'],
                },
            },
            "root": {"level": "WARNING", "handlers": ["console"]},
        }
    }
    # Must test using a subprocess to avoid wrecking pre-existing configuration
    with new_config_file(c):
        code = """if 1:
            import logging

            from distributed.utils_test import captured_handler

            root = logging.getLogger()
            d = logging.getLogger('distributed')
            df = logging.getLogger('distributed.foo')
            dfb = logging.getLogger('distributed.foo.bar')

            with captured_handler(root.handlers[0]) as root_log:
                df.info("1: info")
                dfb.warning("2: warning")
                dfb.error("3: error")
                d.info("4: info")
                d.warning("5: warning")

            root_log = root_log.getvalue().splitlines()
            print(root_log)

            assert root_log == [
                "INFO: distributed.foo: 1: info",
                "ERROR: distributed.foo.bar: 3: error",
                "WARNING: distributed: 5: warning",
                ]
            """

        subprocess.check_call([sys.executable, "-c", code])


def test_logging_mutual_exclusive():
    """
    Ensure that 'logging-file-config' and 'logging' have to be mutual exclusive.
    """
    config = {"logging": {"dask": "warning"}, "logging-file-config": "/path/to/config"}
    with pytest.raises(RuntimeError):
        initialize_logging(config)


def test_logging_file_config():
    """
    Test `logging-file-config` logging configuration
    """
    logging_config_contents = """
[handlers]
keys=console

[formatters]
keys=simple

[loggers]
keys=root, foo, foo_bar

[handler_console]
class=StreamHandler
level=INFO
formatter=simple
args=(sys.stdout,)

[formatter_simple]
format=%(levelname)s: %(name)s: %(message)s

[logger_root]
level=WARNING
handlers=console

[logger_foo]
level=INFO
handlers=console
qualname=foo

[logger_foo_bar]
level=ERROR
handlers=console
qualname=foo.bar
"""
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as logging_config:
        logging_config.write(logging_config_contents)
    dask_config = {"logging-file-config": logging_config.name}
    with new_config_file(dask_config):
        code = """if 1:
            import logging
            from distributed import config
            foo = logging.getLogger('foo')
            bar = logging.getLogger('foo.bar')
            assert logging.INFO == foo.getEffectiveLevel()
            assert logging.ERROR == bar.getEffectiveLevel()
            """
        subprocess.check_call([sys.executable, "-c", code])
    os.remove(logging_config.name)
