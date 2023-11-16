from __future__ import annotations

from dask.dataframe.utils import dask_expr_enabled

if dask_expr_enabled():
    from dask_expr import (
        DataFrame,
        Index,
        Series,
        concat,
        from_dask_dataframe,
        from_graph,
        from_map,
        from_pandas,
        read_csv,
        read_parquet,
    )

    import dask.dataframe._pyarrow_compat
    from dask.base import compute
    from dask.dataframe import backends, dispatch

else:
    try:
        from dask.base import compute
        from dask.dataframe import backends, dispatch
        from dask.dataframe.core import (
            DataFrame,
            Index,
            Series,
            _Frame,
            map_partitions,
            repartition,
            to_datetime,
            to_timedelta,
        )
        from dask.dataframe.groupby import Aggregation
        from dask.dataframe.io import (
            demo,
            from_array,
            from_dask_array,
            from_delayed,
            from_dict,
            from_map,
            from_pandas,
            read_csv,
            read_fwf,
            read_hdf,
            read_json,
            read_sql,
            read_sql_query,
            read_sql_table,
            read_table,
            to_bag,
            to_csv,
            to_hdf,
            to_json,
            to_records,
            to_sql,
        )
        from dask.dataframe.multi import concat, merge, merge_asof
        from dask.dataframe.numeric import to_numeric
        from dask.dataframe.optimize import optimize
        from dask.dataframe.reshape import get_dummies, melt, pivot_table
        from dask.dataframe.utils import assert_eq

        try:
            from dask.dataframe.io import read_parquet, to_parquet
        except ImportError:
            pass
        try:
            from dask.dataframe.io import read_orc, to_orc
        except ImportError:
            pass
        try:
            from dask.dataframe.core import isna
        except ImportError:
            pass
    except ImportError as e:
        msg = (
            "Dask dataframe requirements are not installed.\n\n"
            "Please either conda or pip install as follows:\n\n"
            "  conda install dask                     # either conda install\n"
            '  python -m pip install "dask[dataframe]" --upgrade  # or python -m pip install'
        )
        raise ImportError(msg) from e
