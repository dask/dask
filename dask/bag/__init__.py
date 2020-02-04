try:
    from .core import (
        Bag,
        Item,
        from_sequence,
        from_url,
        to_textfiles,
        concat,
        from_delayed,
        map_partitions,
        bag_range as range,
        bag_zip as zip,
        bag_map as map,
    )
    from .text import read_text
    from .utils import assert_eq
    from .avro import read_avro
    from ..base import compute
except ImportError as e:
    msg = (
        "Dask bag requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask               # either conda install\n"
        "  python -m pip install dask[bag] --upgrade  # or python -m pip install"
    )
    raise ImportError(str(e) + "\n\n" + msg)
