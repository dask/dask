from __future__ import annotations

import os.path

from dask.utils import key_split
from dask.widgets import FILTERS, TEMPLATE_PATHS

TEMPLATE_PATHS.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
)
FILTERS["key_split"] = key_split
