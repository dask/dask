from __future__ import annotations

import asyncio
import logging.config
import os
import sys
from collections.abc import Callable
from typing import Any

import yaml

import dask
from dask.utils import import_required

config = dask.config.config


fn = os.path.join(os.path.dirname(__file__), "distributed.yaml")

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update_defaults(defaults)

deprecations = {
    "allowed-failures": "distributed.scheduler.allowed-failures",
    "bandwidth": "distributed.scheduler.bandwidth",
    "default-data-size": "distributed.scheduler.default-data-size",
    "work-stealing": "distributed.scheduler.work-stealing",
    "worker-ttl": "distributed.scheduler.worker-ttl",
    "multiprocessing-method": "distributed.worker.multiprocessing-method",
    "use-file-locking": "distributed.worker.use-file-locking",
    "profile-interval": "distributed.worker.profile.interval",
    "profile-cycle-interval": "distributed.worker.profile.cycle",
    "worker-memory-target": "distributed.worker.memory.target",
    "worker-memory-spill": "distributed.worker.memory.spill",
    "worker-memory-pause": "distributed.worker.memory.pause",
    "worker-memory-terminate": "distributed.worker.memory.terminate",
    "heartbeat-interval": "distributed.client.heartbeat",
    "compression": "distributed.comm.compression",
    "connect-timeout": "distributed.comm.timeouts.connect",
    "tcp-timeout": "distributed.comm.timeouts.tcp",
    "default-scheme": "distributed.comm.default-scheme",
    "socket-backlog": "distributed.comm.socket-backlog",
    "diagnostics-link": "distributed.dashboard.link",
    "bokeh-export-tool": "distributed.dashboard.export-tool",
    "tick-time": "distributed.admin.tick.interval",
    "tick-maximum-delay": "distributed.admin.tick.limit",
    "log-length": "distributed.admin.log-length",
    "log-format": "distributed.admin.log-format",
    "pdb-on-err": "distributed.admin.pdb-on-err",
    "ucx": "distributed.comm.ucx",
    "rmm": "distributed.rmm",
    # low-level-log-length aliases
    "transition-log-length": "distributed.admin.low-level-log-length",
    "distributed.scheduler.transition-log-length": "distributed.admin.low-level-log-length",
    "distributed.scheduler.events-log-length": "distributed.admin.low-level-log-length",
    "recent-messages-log-length": "distributed.admin.low-level-log-length",
    "distributed.comm.recent-messages-log-length": "distributed.admin.low-level-log-length",
    "distributed.p2p.disk": "distributed.p2p.storage.disk",
}

# Affects yaml and env variables configs, as well as calls to dask.config.set()
# before importing distributed
dask.config.rename(deprecations)
# Affects dask.config.set() from now on
dask.config.deprecations.update(deprecations)


#########################
# Logging specific code #
#########################
#
# Here we enact the policies in the logging part of the configuration

logger = logging.getLogger(__name__)


if sys.version_info >= (3, 11):
    _logging_get_level_names_mapping = logging.getLevelNamesMapping
else:

    def _logging_get_level_names_mapping() -> dict[str, int]:
        return logging._nameToLevel.copy()


def _initialize_logging_old_style(config: dict[Any, Any]) -> None:
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
    loggers: dict[str, str | int] = {  # default values
        "distributed": "info",
        "distributed.client": "warning",
        "distributed.gc": "warning",
        "bokeh": "error",
        "tornado": "critical",
        "tornado.application": "error",
    }
    base_config = _find_logging_config(config)
    loggers.update(base_config.get("logging", {}))

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            dask.config.get("distributed.admin.log-format", config=config)
        )
    )
    logging_names = _logging_get_level_names_mapping()
    for name, raw_level in sorted(loggers.items()):
        level = (
            logging_names[raw_level.upper()]
            if isinstance(raw_level, str)
            else raw_level
        )
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Ensure that we're not registering the logger twice in this hierarchy.
        anc = logging.getLogger(None)
        already_registered = False

        for ancestor in name.split("."):
            if anc.handlers:
                already_registered = True
                break
            anc.getChild(ancestor)

        if not already_registered:
            logger.addHandler(handler)
            logger.propagate = False


def _initialize_logging_new_style(config: dict[Any, Any]) -> None:
    """
    Initialize logging using logging's "Configuration dictionary schema".
    (ref.: https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema)
    """
    base_config = _find_logging_config(config)
    logging.config.dictConfig(base_config.get("logging"))  # type: ignore[arg-type]


def _initialize_logging_file_config(config: dict[Any, Any]) -> None:
    """
    Initialize logging using logging's "Configuration file format".
    (ref.: https://docs.python.org/3/howto/logging.html#configuring-logging)
    """
    base_config = _find_logging_config(config)
    logging.config.fileConfig(
        base_config.get("logging-file-config"),  # type: ignore[arg-type]
        disable_existing_loggers=False,
    )


def _find_logging_config(config: dict[Any, Any]) -> dict[Any, Any]:
    """
    Look for the dictionary containing logging-specific configurations,
    starting in the 'distributed' dictionary and then trying the top-level
    """
    logging_keys = {"logging-file-config", "logging"}
    if logging_keys & config.get("distributed", {}).keys():
        return config["distributed"]
    else:
        return config


def initialize_logging(config: dict[Any, Any]) -> None:
    base_config = _find_logging_config(config)
    if "logging-file-config" in base_config:
        if "logging" in base_config:
            raise RuntimeError(
                "Config options 'logging-file-config' and 'logging' are mutually exclusive."
            )
        _initialize_logging_file_config(config)
    else:
        log_config = base_config.get("logging", {})
        if "version" in log_config:
            # logging module mandates version to be an int
            log_config["version"] = int(log_config["version"])
            _initialize_logging_new_style(config)
        else:
            _initialize_logging_old_style(config)


def get_loop_factory() -> Callable[[], asyncio.AbstractEventLoop] | None:
    event_loop = dask.config.get("distributed.admin.event-loop")
    if event_loop == "uvloop":
        uvloop = import_required(
            "uvloop",
            "The distributed.admin.event-loop configuration value "
            "is set to 'uvloop' but the uvloop module is not installed"
            "\n\n"
            "Please either change the config value or install one of the following\n"
            "    conda install uvloop\n"
            "    pip install uvloop",
        )
        return uvloop.new_event_loop
    if event_loop in {"asyncio", "tornado"}:
        if sys.platform == "win32":
            # ProactorEventLoop is not compatible with tornado 6
            # fallback to the pre-3.8 default of Selector
            # https://github.com/tornadoweb/tornado/issues/2608
            return asyncio.SelectorEventLoop
        return None
    raise ValueError(
        "Expected distributed.admin.event-loop to be in ('asyncio', 'tornado', 'uvloop'), got %s"
        % dask.config.get("distributed.admin.event-loop")
    )


initialize_logging(dask.config.config)
