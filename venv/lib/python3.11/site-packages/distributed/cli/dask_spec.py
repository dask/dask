from __future__ import annotations

import asyncio
import json
import sys

import click
import yaml

from distributed._signals import wait_for_signals
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.deploy.spec import init_spec


@click.command(name="spec", context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1)
@click.option("--spec", type=str, default="", help="")
@click.option("--spec-file", type=str, default=None, help="")
@click.version_option()
def main(args: list, spec: str, spec_file: str) -> None:
    """Launch a Dask process defined by a JSON/YAML specification"""

    if spec and spec_file or not spec and not spec_file:
        print("Must specify exactly one of --spec and --spec-file")
        sys.exit(1)
    _spec = {}
    if spec_file:
        with open(spec_file) as f:
            _spec.update(yaml.safe_load(f))

    if spec:
        _spec.update(json.loads(spec))

    if "cls" in _spec:  # single worker spec
        _spec = {_spec["opts"].get("name", 0): _spec}

    async def run() -> None:
        servers = init_spec(_spec, *args)

        async def wait_for_servers_to_finish() -> None:
            await asyncio.gather(*servers.values())
            await asyncio.gather(*(w.finished() for w in servers.values()))

        async def wait_for_signals_and_close() -> None:
            await wait_for_signals()
            await asyncio.gather(*(w.close() for w in servers.values()))

        wait_for_signals_and_close_task = asyncio.create_task(
            wait_for_signals_and_close()
        )
        wait_for_servers_to_finish_task = asyncio.create_task(
            wait_for_servers_to_finish()
        )

        done, _ = await asyncio.wait(
            [wait_for_signals_and_close_task, wait_for_servers_to_finish_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Re-raise exceptions from done tasks
        [task.result() for task in done]

    asyncio_run(run(), loop_factory=get_loop_factory())


if __name__ == "__main__":
    main()
