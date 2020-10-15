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
    split_blocks=None,
    chunksize=None,
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
    split_blocks: bool or int (default True)
        Whether output partitions will include groups of avro blocks. If
        False, each partition will correspond to a full file.  If an integer
        value is specified, that number of blocks (or feewer) will be assigned
        to each partition.  If True, the chunksize parameter will be used to
        estimate a reasonable number of blocks for each partition.
    chunksize: int or str
        Maximum size (in bytes) of the desired output partitions.  If
        split_blocks is True, multiple avro blocks will be grouped
        together into each output partitions.  The number of blocks in each
        partition will be chosen to target this total chunksize. Default
        value is engine specific.
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
    split_blocks = split_blocks or True
    pieces = engine.process_metadata(
        fs,
        paths,
        index=index,
        columns=columns,
        split_blocks=split_blocks,
        chunksize=chunksize,
        **kwargs,
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
        # Use first block for metadata
        meta_piece = {
            "path": paths[0],
            "blocks": (0, 1),
            "rows": (0, 5),
        }
        meta = engine.read_partition(fs, meta_piece, index, columns)
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
        cls,
        fs,
        paths,
        index=None,
        columns=None,
        split_blocks=True,  # Whether to split by avro blocks
        chunksize=None,  # Target partition size (in bytes)
        **kwargs,
    ):
        chunksize = parse_bytes(chunksize or "100MB")
        if split_blocks:
            # Use first avro block to estimate memory req
            # for each row
            with fs.open(paths[0], "rb") as fo:
                avro_reader = fa.block_reader(fo)
                block = avro_reader.__next__()
                block_mem = (
                    pd.DataFrame.from_records(list(block)).memory_usage(deep=True).sum()
                )
                split_blocks = int(chunksize // block_mem)
            split_blocks = max(split_blocks, 1)

        pieces = []
        if split_blocks:
            # Break apart files at the "Avro block" granularity
            for path in paths:
                (file_row_offset, part_row_count) = (0, 0)
                (file_block_offset, part_block_count) = (0, 0)
                (file_byte_offset, part_byte_count) = (0, 0)
                with fs.open(path, "rb") as fo:
                    avro_reader = fa.block_reader(fo)
                    for i, block in enumerate(avro_reader):
                        part_row_count += block.num_records
                        part_block_count += 1
                        part_byte_count += block.size

                        if (i + 1) % split_blocks == 0:
                            pieces.append(
                                {
                                    "path": path,
                                    "rows": (file_row_offset, part_row_count),
                                    "blocks": (file_block_offset, part_block_count),
                                    "bytes": (file_byte_offset, part_byte_count),
                                }
                            )
                            (file_row_offset, part_row_count) = (
                                file_row_offset + part_row_count,
                                0,
                            )
                            (file_block_offset, part_block_count) = (
                                file_block_offset + part_block_count,
                                0,
                            )
                            (file_byte_offset, part_byte_count) = (
                                file_byte_offset + part_byte_count,
                                0,
                            )

                    if part_block_count:
                        pieces.append(
                            {
                                "path": path,
                                "rows": (file_row_offset, part_row_count),
                                "blocks": (file_block_offset, part_block_count),
                                "bytes": (file_byte_offset, part_byte_count),
                            }
                        )

        else:
            # Map entire file to each partition
            # TODO: Handle many small files.
            for path in paths:
                pieces.append({"path": path})

        # `pieces` is a list with the following dict elements:
        # {
        #    "path" : <file-path>,
        #    "rows" : (<file-row-offset>, <row-count>),
        #    "blocks" : (<file-block-offset>, <block-count>),
        #    "bytes" : (<file-byte-offset>, <byte-count>),
        # }
        #
        # If the element dict only contains a "path" element, the
        # entire file will be read in at once.

        return pieces

    @classmethod
    def read_partition(cls, fs, piece, index, columns):

        if "blocks" in piece:
            # Reading specific block range
            path = piece["path"]
            block_offset, part_blocks = piece["blocks"]
            block_partitions = True
        else:
            # Reading entire file at once
            path = piece["path"]
            block_partitions = False

        # Read data with fastavro
        if block_partitions:
            records = []
            with open(path, "rb") as fo:
                avro_reader = fa.block_reader(fo)
                for i, block in enumerate(avro_reader):
                    if i == block_offset + part_blocks:
                        break
                    if i >= block_offset:
                        records += list(block)
            df = pd.DataFrame.from_records(records)
        else:
            with fs.open(path, "rb") as fo:
                reader = fa.reader(fo)
                df = pd.DataFrame.from_records(reader)

        # Deal with index and column selection
        if index:
            df.set_index(index, inplace=True)
        if columns is None:
            columns = list(df.columns)
        return df[columns]
