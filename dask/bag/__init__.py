try:
    from ..base import compute
    from .avro import read_avro
    from .core import Bag, Item
    from .core import bag_map as map
    from .core import bag_range as range
    from .core import bag_zip as zip
    from .core import (
        concat,
        from_delayed,
        from_sequence,
        from_url,
        map_partitions,
        to_textfiles,
    )
    from .text import read_text
    from .utils import assert_eq
except ImportError as e:
    msg = (
        "Dask bag requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask               # either conda install\n"
        '  python -m pip install "dask[bag]" --upgrade  # or python -m pip install'
    )
    raise ImportError(str(e) + "\n\n" + msg) from e
