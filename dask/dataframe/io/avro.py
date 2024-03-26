import pandas as pd
from ..utils import insert_meta_param_description, make_meta

from fsspec.core import get_fs_token_paths
from fsspec.utils import stringify_path

from ..core import new_dd_object, _concat
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
    """Get the avro engine backend implementation.

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
    engine="auto",
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
    def _get_blocks(cls, fo, file_size):
        return fa.block_reader(fo)

    @classmethod
    def _get_block_size(cls, block):
        return block.num_records, block.size, block.offset

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

        # Construct list of pieces
        blocksize = parse_bytes(blocksize or "100MB")
        pieces = _construct_pieces(
            cls,
            fs,
            paths,
            blocksize,
            index=index,
            columns=columns,
        )

        pieces = _aggregate_files(pieces, blocksize)

        return pieces, meta

    @classmethod
    def read_partition(cls, fs, piece, index, columns):

        # Read data with fastavro
        if isinstance(piece, dict):
            records = []
            block_offset, part_blocks = piece["blocks"]
            with open(piece["path"], "rb") as fo:
                avro_reader = fa.block_reader(fo)
                for i, block in enumerate(avro_reader):
                    if i == block_offset + part_blocks:
                        break
                    if i >= block_offset:
                        records += list(block)
            df = pd.DataFrame.from_records(records)
        else:
            df = []
            for item in piece:
                with fs.open(item["path"], "rb") as fo:
                    reader = fa.reader(fo)
                    df.append(pd.DataFrame.from_records(reader))
            df = _concat(df, ignore_index=True)

        # Deal with index and column selection
        if index:
            df.set_index(index, inplace=True)
        if columns is None:
            columns = list(df.columns)
        return df[columns]


class UAvroEngine(AvroEngine):
    @classmethod
    def _get_blocks(cls, fo, file_size):
        header = ua.core.read_header(fo)
        ua.core.scan_blocks(fo, header, file_size)
        return header["blocks"]

    @classmethod
    def _get_block_size(cls, block):
        return block["nrows"], block["size"], block["offset"]

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
            with open(paths[0], "rb") as fo:
                header = ua.core.read_header(fo)
                blocks = header["blocks"]
                fo.seek(blocks[0]["offset"])
                meta = ua.core.filelike_to_dataframe(fo, blocks[0]["size"], header)

        # Construct list of pieces
        blocksize = parse_bytes(blocksize or "100MB")
        pieces = _construct_pieces(
            cls,
            fs,
            paths,
            blocksize,
            index=index,
            columns=columns,
        )

        pieces = _aggregate_files(pieces, blocksize)

        return pieces, meta

    @classmethod
    def read_partition(cls, fs, piece, index, columns):

        if isinstance(piece, dict):
            # Read specific block range
            file_size = fs.du(piece["path"])
            block_offset, part_blocks = piece["blocks"]
            with fs.open(piece["path"], "rb") as fo:
                header = ua.core.read_header(fo)
                ua.core.scan_blocks(fo, header, file_size)
                header["blocks"] = header["blocks"][
                    block_offset : block_offset + part_blocks
                ]

                # Adjust the total row count
                nrows = 0
                for block in header["blocks"]:
                    nrows += block["nrows"]
                header["nrows"] = nrows

                df = ua.core.filelike_to_dataframe(fo, file_size, header, scan=False)
        else:
            # Read entire file at once
            df = []
            for item in piece:
                file_size = fs.du(item["path"])
                with fs.open(item["path"], "rb") as fo:
                    header = ua.core.read_header(fo)
                    df.append(ua.core.filelike_to_dataframe(fo, file_size, header))
            df = _concat(df, ignore_index=True)

        # Deal with index and column selection
        if index:
            df.set_index(index, inplace=True)
        if columns is None:
            columns = list(df.columns)
        return df[columns]


def _aggregate_files(pieces, blocksize):
    new_pieces = []
    item = []
    size = 0
    for piece in pieces:
        if "blocks" in piece:
            # This is not a full file.
            # Just append the piece and move on.
            if size:
                new_pieces.append(item)
                size = 0
                item = []
            new_pieces.append(piece)
            continue

        if size + piece["file_size"] <= blocksize:
            # This piece can be added to the item list
            size += piece["file_size"]
            item.append(piece)
        else:
            # This piece is too big to add to the item list.
            # We need to flush whatever we already have in
            # `item` before adding this piece.
            if size:
                new_pieces.append(item)
            size = piece["file_size"]
            item = [piece]

    if size:
        new_pieces.append(item)

    return new_pieces


def _construct_pieces(
    engine,
    fs,
    paths,
    blocksize,
    index=None,
    columns=None,
):
    """Construct list of pieces using either uavro an fastavro"""
    pieces = []
    if blocksize is not False:
        # Break apart files at the "Avro block" granularity
        for path in paths:
            file_size = fs.du(path)
            if file_size > blocksize:
                part_count = 0
                with fs.open(path, "rb") as fo:

                    file_row_offset, part_row_count = 0, 0
                    file_block_offset, part_block_count = 0, 0
                    file_byte_offset, part_byte_count = 0, 0

                    blocks = engine._get_blocks(fo, file_size)
                    for i, block in enumerate(blocks):

                        nrows, nbytes, offset = engine._get_block_size(block)
                        if i == 0:
                            # Correct the initial offset
                            file_byte_offset = offset
                        part_row_count += nrows
                        part_block_count += 1
                        part_byte_count += nbytes

                        if part_byte_count >= blocksize:
                            pieces.append(
                                {
                                    "path": path,
                                    "rows": (file_row_offset, part_row_count),
                                    "blocks": (file_block_offset, part_block_count),
                                    "bytes": (file_byte_offset, part_byte_count),
                                    "file_size": file_size,
                                }
                            )
                            part_count += 1
                            file_row_offset += part_row_count
                            file_block_offset += part_block_count
                            file_byte_offset += part_byte_count
                            part_row_count = part_block_count = part_byte_count = 0

                    if part_block_count:
                        pieces.append(
                            {
                                "path": path,
                                "rows": (file_row_offset, part_row_count),
                                "blocks": (file_block_offset, part_block_count),
                                "bytes": (file_byte_offset, part_byte_count),
                                "file_size": file_size,
                            }
                        )
                        part_count += 1
                if part_count == 1:
                    # No need to specify a block/byte range since we
                    # will need to read the entire file anyway.
                    pieces[-1] = {"path": pieces[-1]["path"], "file_size": file_size}
            else:
                pieces.append({"path": path, "file_size": file_size})
    else:
        # Map entire file to each partition.
        # Note that multiple small files will be aggregated into
        # the same partition in `_aggregate_files`
        for path in paths:
            pieces.append({"path": path, "file_size": fs.du(path)})

    return pieces
