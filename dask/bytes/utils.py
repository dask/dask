from fsspec.core import infer_compression, expand_paths_if_needed
from fsspec.utils import (seek_delimiter, stringify_path, build_name_function,
                          infer_storage_options, update_storage_options,
                          read_block)


def SeekableFile(x):
    return x
