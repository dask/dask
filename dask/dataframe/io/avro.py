import pandas as pd
from ..utils import insert_meta_param_description, make_meta

from fsspec.core import get_fs_token_paths
from fsspec.utils import stringify_path

from ..core import new_dd_object
from ...base import tokenize
from ...utils import import_required, natural_sort_key, parse_bytes

try:
    import fastavro as fa
except ImportError:
    fa = None

try:
    import uavro as ua
except ImportError:
    ua = None


_ENGINES = {}


def get_engine(engine):
    """Get the parquet engine backend implementation.

    Parameters
    ----------
    engine : {'auto', 'uavro', 'fastavro'}, default 'auto'
        Parquet reader library to use. Defaults to uavro if both are
        installed.

    Returns
    -------
    An AvroEngine instance.
    """
    if engine in _ENGINES:
        return _ENGINES[engine]

    if engine == "auto":
        for eng in ["uavro", "fastavro"]:
            try:
                return get_engine(eng)
            except RuntimeError:
                pass
        else:
            raise RuntimeError("Please install either uavro or fastavro")

    elif engine == "uavro":
        import_required("uavro", "`uavro` not installed")

        _ENGINES["uavro"] = eng = UAvroEngine
        return eng

    elif engine == "fastavro":
        import_required("fastavro", "`fastavro` not installed")

        _ENGINES["fastavro"] = eng = FastAvroEngine
        return eng

    else:
        raise ValueError(
            'Unsupported engine: "{0}".'.format(engine)
            + '  Valid choices include "uavro" and "fastavro".'
        )


@insert_meta_param_description
def read_avro(
    url_path,
    storage_options=None,
    blocksize=None,
    meta=None,
    engine=None,
    index=None,
    columns=None,
    divisions=None,
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
    blocksize: int, str or False
        Approximate contiguous byte size (in avro binary format) to assign to
        each output partition.  If False, each partition will correspond to
        an entire file.  Default value is engine specific.
    engine : str or AvroEngine
        The underlying Engine that dask will use to read the dataset.
    $META

    Returns
    -------
    dask.DataFrame
    """

    if isinstance(engine, str):
        engine = get_engine(engine)

    if hasattr(url_path, "name"):
        url_path = stringify_path(url_path)
    fs, _, paths = get_fs_token_paths(
        url_path, mode="rb", storage_options=storage_options
    )

    paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

    # Get list of pieces for each output
    pieces, meta = engine.process_metadata(
        fs,
        paths,
        index=index,
        columns=columns,
        blocksize=blocksize,
        meta=meta,
    )
    npartitions = len(pieces)

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

    token = tokenize(fs, paths, index, columns)
    read_avro_name = "read-avro-partition-" + token
    dsk = {
        (read_avro_name, i): (engine.read_partition, fs, piece, index, columns)
        for i, piece in enumerate(pieces)
    }

    # Check metadata
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
        cls,
        fs,
        paths,
        index=None,
        columns=None,
        blocksize=None,
        meta=None,
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
        blocksize=None,
        meta=None,
    ):

        if meta is None:
            # Use first block for metadata
            meta_piece = {
                "path": paths[0],
                "blocks": (0, 1),
            }
            meta = cls.read_partition(fs, meta_piece, index, columns)

        pieces = []
        if blocksize is not False:
            # Break apart files at the "Avro block" granularity
            blocksize = parse_bytes(blocksize or "100MB")
            for path in paths:
                file_size = fs.du(path)
                if file_size > blocksize:
                    with fs.open(path, "rb") as fo:

                        (file_row_offset, part_row_count) = (0, 0)
                        (file_block_offset, part_block_count) = (0, 0)
                        (file_byte_offset, part_byte_count) = (0, 0)

                        avro_reader = fa.block_reader(fo)
                        for i, block in enumerate(avro_reader):
                            if i == 0:
                                # Correct the initial offset
                                file_byte_offset = block.offset
                            part_row_count += block.num_records
                            part_block_count += 1
                            part_byte_count += block.size
                            if part_byte_count >= blocksize:
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
                    pieces.append({"path": path})
        else:
            # Map entire file to each partition
            # TODO: Handle many small files.
            for path in paths:
                pieces.append({"path": path})

        return pieces, meta

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


class UAvroEngine(AvroEngine):
    @classmethod
    def process_metadata(
        cls,
        fs,
        paths,
        index=None,
        columns=None,
        split_blocks=True,
        blocksize=None,
        meta=None,
    ):

        if meta is None:
            # Use first block for metadata
            with open(paths[0], "rb") as fo:
                header = ua.core.read_header(fo)
                blocks = header["blocks"]
                fo.seek(blocks[0]["offset"])
                meta = ua.core.filelike_to_dataframe(fo, blocks[0]["size"], header)

        pieces = []
        if blocksize is not False:
            # Break apart files at the "Avro block" granularity
            blocksize = parse_bytes(blocksize or "100MB")
            for path in paths:
                file_size = fs.du(path)
                if file_size > blocksize:
                    with open(path, "rb") as fo:
                        header = ua.core.read_header(fo)
                        ua.core.scan_blocks(fo, header, file_size)
                        blocks = header["blocks"]

                        (file_row_offset, part_row_count) = (0, 0)
                        (file_block_offset, part_block_count) = (0, 0)
                        (file_byte_offset, part_byte_count) = (blocks[0]["offset"], 0)

                        for i, block in enumerate(blocks):
                            part_row_count += block["nrows"]
                            part_block_count += 1
                            part_byte_count += block["size"]
                            if part_byte_count >= blocksize:
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
                    pieces.append({"path": path})
        else:
            # Map entire file to each partition
            # TODO: Handle many small files.
            for path in paths:
                pieces.append({"path": path})

        return pieces, meta

    @classmethod
    def read_partition(cls, fs, piece, index, columns):

        if "bytes" in piece:
            # Read specific byte range
            byte_offset, part_bytes = piece["bytes"]
            with open(piece["path"], "rb") as fo:
                header = ua.core.read_header(fo)
                header["blocks"] = []
                fo.seek(byte_offset)
                df = ua.core.filelike_to_dataframe(fo, part_bytes, header)
                pass
        else:
            # Read entire file at once
            file_size = fs.du(piece["path"])
            with fs.open(piece["path"], "rb") as fo:
                header = ua.core.read_header(fo)
                df = ua.core.filelike_to_dataframe(fo, file_size, header)

        # Deal with index and column selection
        if index:
            df.set_index(index, inplace=True)
        if columns is None:
            columns = list(df.columns)
        return df[columns]
