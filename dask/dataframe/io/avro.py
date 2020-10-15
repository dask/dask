import pandas as pd
from ..utils import insert_meta_param_description, make_meta

from fsspec.core import get_fs_token_paths
from fsspec.utils import stringify_path

from ..core import new_dd_object
from ...base import tokenize
from ...utils import natural_sort_key, parse_bytes

try:
    import fastavro as fa
except ImportError:
    fa = None


@insert_meta_param_description
def read_avro(
    url_path,
    storage_options=None,
    blocksize_rows=None,
    blocksize_bytes="100MB",
    meta=None,
    engine=None,
    index=None,
    columns=None,
    divisions=None,
    **kwargs,
):
    """Create a dataframe from a set of Avro files

    Parameters
    ----------
    url_path: str, list of str
        Location to read from. If a string, can include a glob character to
        find a set of file names.
        Supports protocol specifications such as ``"s3://"``.
    storage_options: dict
        Passed to backend file-system implementation
    blocksize_rows: None or int
        If None, files are not blocked, and you get one partition per input
        file. If int, Avro blocks will be aggregated together for each
        partition.
    engine : AvroEngine Class
        The underlying Engine that dask will use to read the dataset.
    $META

    Returns
    -------
    dask.DataFrame
    """

    if engine is None:
        if fa is None:
            raise ValueError("read_avro requires the fastavro library.")
        engine = FastAvroEngine

    if hasattr(url_path, "name"):
        url_path = stringify_path(url_path)
    fs, _, paths = get_fs_token_paths(
        url_path, mode="rb", storage_options=storage_options
    )

    paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

    # Get list of pieces for each output
    blocksize_bytes = parse_bytes(blocksize_bytes)
    pieces = engine.process_metadata(
        fs, paths, index, columns, blocksize_rows, blocksize_bytes, **kwargs
    )
    npartitions = len(pieces)

    token = tokenize(fs, paths, index, columns)
    read_avro_name = "read-avro-partition-" + token
    dsk = {
        (read_avro_name, i): (engine.read_partition, fs, piece, index, columns)
        for i, piece in enumerate(pieces)
    }

    # Create metadata
    if meta is None:
        meta = engine.read_partition(fs, pieces[0], index, columns)
    meta = make_meta(meta)

    # Create collection
    if divisions is None or divisions == "sorted":
        divs = [None] * (npartitions + 1)
    else:
        divs = tuple(divisions)
        if len(divs) != npartitions + 1:
            raise ValueError("divisions should be a tuple of npartitions + 1")
    df = new_dd_object(dsk, read_avro_name, meta, divs)

    # Sort divisions if necessary
    if divisions == "sorted":
        from ..shuffle import compute_and_set_divisions

        df = compute_and_set_divisions(df)

    return df


class AvroEngine:
    """ The API necessary to provide a new Avro reader/writer """

    @classmethod
    def process_metadata(
        cls, fs, paths, index, columns, blocksize, blocksize_bytes, **kwargs
    ):
        raise NotImplementedError()

    @classmethod
    def read_partition(cls, fs, piece, index, columns):
        raise NotImplementedError()


class FastAvroEngine(AvroEngine):
    @classmethod
    def process_metadata(
        cls, fs, paths, index, columns, blocksize_rows, blocksize_bytes, **kwargs
    ):

        sizes = []
        for path in paths:
            size = 0
            with fs.open(path, "rb") as fo:
                avro_reader = fa.block_reader(fo)
                for block in avro_reader:
                    n = block.num_records
                    size += n
                    if blocksize_rows is None:
                        # Use first avro block to estimate memory req
                        # for each row
                        row_mem = (
                            pd.DataFrame.from_records(list(block))
                            .memory_usage(deep=True)
                            .sum()
                            / n
                        )
                        blocksize_rows = int(blocksize_bytes // row_mem)
            sizes.append(size)

        pieces = []
        if blocksize_rows >= max(sizes):
            # Map entire file to each partition
            # TODO: Handle many small files.
            for path in paths:
                pieces.append((path, None, None, None))
        else:
            # Break apart files at the "Avro block" granularity
            for path in paths:
                global_offset = part_offset = part_records = part_blocks = 0
                with fs.open(path, "rb") as fo:
                    avro_reader = fa.block_reader(fo)
                    for i, block in enumerate(avro_reader):
                        n = block.num_records
                        if part_records + n >= blocksize_rows:
                            pieces.append(
                                (path, part_offset, part_records, part_blocks)
                            )
                            part_records = part_blocks = 0
                            part_offset = global_offset + n
                        else:
                            part_records += n
                            part_blocks += 1
                        global_offset += n
                    if part_blocks:
                        pieces.append((path, part_offset, part_records, part_blocks))

        # `pieces` is a list with the following tuple elements:
        # (<file-path>, <record-index-offset>, <record-count>, <block-count>)
        #
        # Note: If only the file-path is not None, the entire file
        #       should be read.

        return pieces

    @classmethod
    def read_partition(cls, fs, piece, index, columns):

        path, part_offset, part_records, part_blocks = piece

        if False:  # part_blocks:
            pass
        else:
            reader = fa.reader(fs.open(path, "rb"))
            df = pd.DataFrame.from_records(reader)
            if index:
                df.set_index(index, inplace=True)
            if columns is None:
                columns = list(df.columns)
            return df[columns]
