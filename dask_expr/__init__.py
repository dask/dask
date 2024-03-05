import dask.dataframe

from dask_expr import _version, datasets
from dask_expr._collection import *
from dask_expr._dispatch import get_collection_type
from dask_expr._dummies import get_dummies
from dask_expr._groupby import Aggregation
from dask_expr.io._delayed import from_delayed
from dask_expr.io.bag import to_bag
from dask_expr.io.csv import to_csv
from dask_expr.io.hdf import read_hdf, to_hdf
from dask_expr.io.json import read_json, to_json
from dask_expr.io.orc import read_orc, to_orc
from dask_expr.io.parquet import to_parquet
from dask_expr.io.records import to_records
from dask_expr.io.sql import read_sql, read_sql_query, read_sql_table, to_sql

__version__ = _version.get_versions()["version"]

import pandas as pd

pd.set_option("mode.copy_on_write", True)
del pd
