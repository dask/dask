from __future__ import print_function, division, absolute_import

import math
import os
import re
import pathlib

from toolz import identity

from ..compatibility import PY2, urlsplit
from fsspec.core import infer_compression, expand_paths_if_needed
from fsspec.utils import (seek_delimiter, stringify_path, build_name_function,
                          infer_storage_options, update_storage_options, read_block)


def SeekableFile(x):
    return x
