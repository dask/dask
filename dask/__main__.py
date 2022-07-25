import sys

try:
    import click
except (ModuleNotFoundError, ImportError):
    click = None


def main():

    if click is None:
        msg = (
            "The dask command requires click to be installed.\n\n"
            "Install with conda or pip:\n\n"
            " conda install click\n"
            " pip install click\n"
        )
        print(msg, file=sys.stderr)
        return 1

    from dask.cli import run_cli

    run_cli()


if __name__ == "__main__":
    main()
