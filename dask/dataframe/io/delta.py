import pandas as pd

from ...base import tokenize
from ...highlevelgraph import HighLevelGraph
from ...layers import DataFrameIOLayer
from ...utils import import_required
from ..core import new_dd_object

__all__ = ("read_delta_table",)


class DeltaFunctionWrapper:
    """
    Delta Function-Wrapper Class
    """

    def __init__(self, path, version, columns, schema):
        self.path = path
        self.columns = columns
        self.schema = schema
        self.version = version

    def project_columns(self, columns):
        """Return a new DeltaFunctionWrapper object with
        a sub-column projection.
        """
        if columns == self.columns:
            return self
        return DeltaFunctionWrapper(self.fs, columns, self.schema)

    def __call__(self, version):

        from deltalake import DeltaTable

        dt = DeltaTable(self.path, self.version)
        pdf = dt.to_pandas(columns=self.columns)
        return pdf


def read_delta_table(path, version=None, columns=None, schema=None):
    """Read dataframe from ORC file(s)

    Parameters
    ----------
    path: str or list(str)
        Location of file(s), which can be a full URL with protocol specifier,
        and may include glob character if a single string.
    version: int
        Version of dataset to be read (Use this parameter for time travel)
    columns: None or list(str)
        Columns to load. If None, loads all.
    schema: Arrow schema
        Further parameters to pass to the bytes backend.

    Returns
    -------
    Dask.DataFrame (even if there is only one column)

    Examples
    --------
    >>> df = dd.read_delta_table('https://github.com/apache/orc/raw/'
    ...                  'master/examples/demo-11-zlib.orc')  # doctest: +SKIP
    """
    import_required("deltalake", "Please install deltalake")
    from deltalake import DeltaTable

    # Create Blockwise layer
    label = "read-deltalake-"
    output_name = label + tokenize(path, columns)
    parts = DeltaTable(path, version).file_uris()
    meta = pd.read_parquet(parts[0])

    layer = DataFrameIOLayer(
        output_name,
        columns,
        parts,
        DeltaFunctionWrapper(path, version, columns, schema),
        label=label,
    )

    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, [None] * (len(parts) + 1))
