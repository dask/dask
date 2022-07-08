from jinja2 import Environment, Template

try:
    from dask.widgets.widgets import (
        FILTERS,
        TEMPLATE_PATHS,
        get_environment,
        get_template,
    )

except ImportError as e:
    msg = (
        "Dask diagnostics requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask                     # either conda install\n"
        '  python -m pip install "dask[diagnostics]" --upgrade  # or python -m pip install'
    )
    exception = e  # Explicit reference for e as it will be lost outside the try block
    FILTERS = {}
    TEMPLATE_PATHS = []

    def get_environment() -> Environment:
        raise ImportError(msg) from exception

    def get_template(name: str) -> Template:
        raise ImportError(msg) from exception
