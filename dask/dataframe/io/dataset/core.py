from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from dask.utils import apply


class ReadFunction:
    """Wrapper-function class for reading data"""

    def project_columns(self, columns):
        if columns == self.columns:
            return self
        return type(self)(columns, self.filters)

    def __call__(self, source):
        raise NotImplementedError


class DatasetSource:
    def __init__(self, path, partition_base_dir=None):
        self.path = path  # Directory path, file path, list of paths, or glob
        self.partition_base_dir = partition_base_dir  # Required if this DatasetSource is part of partitioned dataset


class DatasetEngine:
    def get_collection_mapping(
        self,
        path,
        columns=None,
        filters=None,
        index=None,
        full_return=True,
    ):
        """Returns required inputs to from_map"""
        raise NotImplementedError


def get_engine(engine, **engine_options):
    if engine == "pyarrow":
        from dask.dataframe.io.dataset.arrow import ArrowDataset

        return ArrowDataset(**engine_options)
    elif engine == "pyarrow-parquet":
        from dask.dataframe.io.dataset.arrow import ArrowParquetDataset

        return ArrowParquetDataset(**engine_options)
    elif isinstance(engine, DatasetEngine):
        # DatasetEngine class is already initialized
        if engine_options:
            raise ValueError(
                f"engine_options not supported if the engine is already "
                f"intialized. Pass in the engine class name, or remove "
                f"the engine_options: {engine_options}."
            )
        return engine

    return engine(**engine_options)


def _finalize_plan(plan_list):
    fragments, divisions, meta, io_func = plan_list[0]

    if None in divisions:
        divisions = None
    else:
        divisions = list(divisions)

    for frags, divs in plan_list[1:]:
        fragments += frags
        if divisions:
            if divisions[-1] is None or divs[0] is None or (divs[0] < divisions[-1]):
                divisions = None
            else:
                divisions += list(divs[1:])

    return fragments, divisions or (None,) * (len(fragments) + 1), meta, io_func


def from_dataset(
    path,
    columns=None,
    filters=None,
    index=None,
    engine="pyarrow",
    compute_options=None,
    **engine_options,
):
    """Create a Dask-DataFrame collection from a stored dataset

    WARNING: This API is completely unstable and experimental!!

    Parameters
    ----------
    path : str or list
        Source directory for data, or path(s) to individual parquet files.
        Prefix with a protocol like ``s3://`` to read from alternative
        filesystems. To read from multiple files you can pass a globstring or a
        list of paths, with the caveat that they must all have the same
        protocol.
    columns : str or list, default None
        Field name(s) to read in as columns in the output. By default all
        non-index fields will be read (as determined by the pandas parquet
        metadata, if present). Provide a single field name instead of a list to
        read in the data as a Series.
    filters : Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
        List of filters to apply, like ``[[('col1', '==', 0), ...], ...]``.
        Predicates should be expressed in disjunctive normal form (DNF). This
        means that the innermost tuple describes a single column predicate.
        These inner predicates are combined with an AND conjunction into a
        larger predicate. The outer-most list then combines all of the
        combined filters with an OR disjunction.
    index : str or False, default None
        Field name to use as the output frame index. By default, the index may
        be inferred from file metadata (if present). Use False to read all
        fields as columns.
    engine : str or DatasetEngine, default 'pyarrow'
        DatasetEngine class to use. Defaults to `ArrowDataset`, which supports
        “parquet”, “ipc”/”arrow”/”feather”, “csv”, and “orc” (the specific
        file format should be specified with the ``format=`` argument). For
        Parquet-formatted data, `engine="pyarrow-parquet"` is recommended
        (which is the same as `engine=ArrowParquetDataset`).
    compute_options : dict, optional
        Dictionary of compute options for internal metadata parsing. This option
        only applies when ``path`` is a list of ``DatasetSource`` objects.
    **engine_options :
        Engine-specific options to use to initialize ``engine``.

    See Also
    --------
    dask.dataframe.io.dataset.arrow.ArrowDataset
    dask.dataframe.io.dataset.arrow.ArrowParquetDataset
    """

    # Create output-collection name
    label = "from-dataset-"
    input_kwargs = {
        "columns": columns,
        "filters": filters,
        "index": index,
        "engine": engine,
        "engine_options": engine_options,
    }
    token = tokenize(path, **input_kwargs)
    output_name = label + token

    # Get dataset engine
    engine = get_engine(engine, **(engine_options or {}))

    # If path is DatasetSource, covernt to single-lngth
    # list to execute on worker
    if isinstance(path, DatasetSource):
        path = [path]

    # Get collection-mapping information
    if isinstance(path, list) and all(isinstance(v, DatasetSource) for v in path):
        # We have a list of DatasetSource objects,
        # so we can parallelize over these objects and
        # construct the collection plan on the workers
        meta_name = "parse-metadata-" + token
        dsk = {}
        for i, source in enumerate(path):
            dsk[(meta_name, i)] = (
                apply,
                engine.get_collection_mapping,
                [source],
                {
                    "columns": columns,
                    "filters": filters,
                    "index": index,
                    "full_return": i == 0,
                },
            )
        dsk["final-" + meta_name] = (_finalize_plan, list(dsk.keys()))
        fragments, divisions, meta, io_func = Delayed(
            "final-" + meta_name, dsk
        ).compute(**(compute_options or {}))
    else:
        # Not operating on a list of DatasetSource objects,
        # wo we will construct our collection plan on the client
        fragments, divisions, meta, io_func = engine.get_collection_mapping(
            path if isinstance(path, DatasetSource) else DatasetSource(path),
            columns=columns,
            filters=filters,
            index=index,
            full_return=True,
        )

    # Special Case (Empty DataFrame)
    if len(divisions) < 2:
        io_func = lambda x: x
        fragments = [meta]
        divisions = (None, None)

    # Create Blockwise layer
    # TODO: use `from_map`
    layer = DataFrameIOLayer(
        output_name,
        columns,
        fragments,
        io_func,
        label=label,
        creation_info={
            "func": from_dataset,
            "args": (path,),
            "kwargs": input_kwargs,
        },
    )
    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, divisions)
