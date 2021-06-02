import copy

from fsspec.core import get_fs_token_paths

from ....base import tokenize
from ....highlevelgraph import HighLevelGraph
from ....layers import DataFrameIOLayer
from ...core import new_dd_object


class ORCFunctionWrapper:
    """
    ORC Function-Wrapper Class
    Reads ORC data from disk to produce a partition.
    """

    def __init__(self, fs, columns, schema, engine):
        self.fs = fs
        self.columns = columns
        self.schema = schema
        self.engine = engine

    def project_columns(self, columns):
        """Return a new ORCFunctionWrapper object with
        a sub-column projection.
        """
        if columns == self.columns:
            return self
        func = copy.deepcopy(self)
        func.columns = columns
        return func

    def __call__(self, parts):
        return self.engine.read_partition(
            self.fs,
            parts,
            self.schema,
            self.columns,
        )


class ORCEngine:
    """The API necessary to provide a new ORC reader/writer"""

    @classmethod
    def read_metadata(cls, fs, paths, columns, partition_stripes, **kwargs):
        raise NotImplementedError()

    @classmethod
    def read_partition(cls, fs, part, columns, **kwargs):
        raise NotImplementedError()

    @classmethod
    def write_partition(cls, df, path, fs, **kwargs):
        raise NotImplementedError


def read_orc(
    path,
    engine="pyarrow",
    columns=None,
    partition_stripes=None,
    storage_options=None,
):
    """Read dataframe from ORC file(s)

    Parameters
    ----------
    path: str or list(str)
        Location of file(s), which can be a full URL with protocol specifier,
        and may include glob character if a single string.
    columns: None or list(str)
        Columns to load. If None, loads all.
    storage_options: None or dict
        Further parameters to pass to the bytes backend.

    Returns
    -------
    Dask.DataFrame (even if there is only one column)

    Examples
    --------
    >>> df = dd.read_orc('https://github.com/apache/orc/raw/'
    ...                  'master/examples/demo-11-zlib.orc')  # doctest: +SKIP
    """

    if engine == "pyarrow":
        from .arrow import ArrowORCEngine

        engine = ArrowORCEngine
    elif not isinstance(engine, ORCEngine):
        raise TypeError("engine must be 'pyarrow', or an ORCEngine object")

    storage_options = storage_options or {}
    fs, fs_token, paths = get_fs_token_paths(
        path, mode="rb", storage_options=storage_options
    )

    partition_stripes = partition_stripes or 1
    parts, schema, meta = engine.read_metadata(
        fs,
        paths,
        columns,
        partition_stripes,
    )

    # Create Blockwise layer
    label = "read-orc-"
    output_name = label + tokenize(fs_token, path, columns)
    layer = DataFrameIOLayer(
        output_name,
        columns,
        parts,
        ORCFunctionWrapper(fs, columns, schema, engine),
        label=label,
    )

    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, [None] * (len(parts) + 1))
