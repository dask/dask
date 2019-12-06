import asyncio
import click
import json
import sys
import yaml

from distributed.deploy.spec import run_spec


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1)
@click.option("--spec", type=str, default="", help="")
@click.option("--spec-file", type=str, default=None, help="")
@click.version_option()
def main(args, spec: str, spec_file: str):
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

    async def run():
        servers = await run_spec(_spec, *args)
        try:
            await asyncio.gather(*[w.finished() for w in servers.values()])
        except KeyboardInterrupt:
            await asyncio.gather(*[w.close() for w in servers.values()])

    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    main()
