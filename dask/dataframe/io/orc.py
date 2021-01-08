from distutils.version import LooseVersion

from .utils import _get_pyarrow_dtypes, _meta_from_dtypes
from ..core import DataFrame
from ...base import tokenize
from ...blockwise import Blockwise, blockwise_token, destringify_tuple
from ...bytes.core import get_fs_token_paths
from ...highlevelgraph import HighLevelGraph
from ...utils import import_required

__all__ = ("read_orc",)


class ORCFunctionWrapper:
    """
    ORC Function-Wrapper Class

     Reads ORC data from disk to produce a partition (given a key).
    """

    def __init__(self, name, fs, columns, stripe_map):
        self.name = name
        self.fs = fs
        self.columns = columns
        self.stripe_map = stripe_map

    def __call__(self, key):

        try:
            name, i = destringify_tuple(key)
        except ValueError:
            # too many / few values to unpack
            raise KeyError(key) from None

        if name != self.name:
            raise KeyError(key)

        path, stripe = self.stripe_map.get(i, (None, None))
        return _read_orc_stripe(self.fs, path, stripe, self.columns)


def _read_orc_stripe(fs, path, stripe, columns=None):
    """Pull out specific data from specific part of ORC file"""
    orc = import_required("pyarrow.orc", "Please install pyarrow >= 0.9.0")
    import pyarrow as pa

    with fs.open(path, "rb") as f:
        o = orc.ORCFile(f)
        table = o.read_stripe(stripe, columns)
    if pa.__version__ < LooseVersion("0.11.0"):
        return table.to_pandas()
    else:
        return table.to_pandas(date_as_object=False)


def read_orc(path, columns=None, storage_options=None):
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
    orc = import_required("pyarrow.orc", "Please install pyarrow >= 0.9.0")
    import pyarrow as pa

    if LooseVersion(pa.__version__) == "0.10.0":
        raise RuntimeError(
            "Due to a bug in pyarrow 0.10.0, the ORC reader is "
            "unavailable. Please either downgrade pyarrow to "
            "0.9.0, or use the pyarrow master branch (in which "
            "this issue is fixed).\n\n"
            "For more information see: "
            "https://issues.apache.org/jira/browse/ARROW-3009"
        )

    storage_options = storage_options or {}
    fs, fs_token, paths = get_fs_token_paths(
        path, mode="rb", storage_options=storage_options
    )
    schema = None
    nstripes_per_file = []
    for path in paths:
        with fs.open(path, "rb") as f:
            o = orc.ORCFile(f)
            if schema is None:
                schema = o.schema
            elif schema != o.schema:
                raise ValueError("Incompatible schemas while parsing ORC files")
            nstripes_per_file.append(o.nstripes)
    schema = _get_pyarrow_dtypes(schema, categories=None)
    if columns is not None:
        ex = set(columns) - set(schema)
        if ex:
            raise ValueError(
                "Requested columns (%s) not in schema (%s)" % (ex, set(schema))
            )
    else:
        columns = list(schema)
    meta = _meta_from_dtypes(columns, schema, [], [])

    # Define the "blockwise" graph
    N = 0
    output_name = "read-orc-" + tokenize(fs_token, path, columns)
    name = "blockwise-io-" + output_name
    stripe_map = {}
    for path, n in zip(paths, nstripes_per_file):
        for stripe in range(n):
            stripe_map[N] = (path, stripe)
            N += 1
    io_func_wrapper = ORCFunctionWrapper(name, fs, columns, stripe_map)
    dsk = {output_name: (io_func_wrapper, blockwise_token(0))}

    # Create Blockwise layer
    npartitions = N
    layer = Blockwise(
        output_name,
        "i",
        dsk,
        [(name, "i")],
        {name: (npartitions,)},
        io_deps={
            name,
        },
    )
    graph = HighLevelGraph({output_name: layer}, {output_name: set()})

    return DataFrame(graph, output_name, meta, [None] * (npartitions + 1))
