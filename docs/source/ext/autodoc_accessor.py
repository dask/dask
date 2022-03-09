from sphinx.ext.autodoc import Documenter

# from dask.datasets import timeseries


class AccessorLevelDocumenter(Documenter):
    """Specialized Documenter subclass"""

    objtype = "accessormethod"
    directivetype = "method"
    priority = 10 + Documenter.priority

    #  str_series =


def setup(app):
    app.setup_extension("sphinx.ext.autodoc")
    app.add_autodocumenter(AccessorLevelDocumenter)
