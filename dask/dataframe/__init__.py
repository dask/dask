from __future__ import annotations

import importlib


def _dask_expr_enabled() -> bool:
    import dask

    use_dask_expr = dask.config.get("dataframe.query-planning")
    if use_dask_expr is False:
        raise NotImplementedError("The legacy implementation is no longer supported")

    return True


try:
    _dask_expr_enabled()

    import dask_expr as dd

    # trigger loading of dask-expr which will in-turn import dask.dataframe and run remainder
    # of this module's init updating attributes to be dask-expr
    # note: needs reload, in case dask-expr imported before dask.dataframe; works fine otherwise
    dd = importlib.reload(dd)
except ImportError as e:
    msg = (
        "Dask dataframe requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask                     # either conda install\n"
        '  python -m pip install "dask[dataframe]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


try:

    # Ensure that dtypes are registered
    from dask_expr import (
        DataFrame,
        Index,
        Scalar,
        Series,
        concat,
        from_array,
        from_dask_array,
        from_delayed,
        from_dict,
        from_graph,
        from_map,
        from_pandas,
        get_collection_type,
        get_dummies,
        isna,
        map_overlap,
        map_partitions,
        melt,
        merge,
        merge_asof,
        pivot_table,
        read_csv,
        read_fwf,
        read_hdf,
        read_json,
        read_orc,
        read_parquet,
        read_sql,
        read_sql_query,
        read_sql_table,
        read_table,
        repartition,
        to_bag,
        to_csv,
        to_datetime,
        to_hdf,
        to_json,
        to_numeric,
        to_orc,
        to_parquet,
        to_records,
        to_sql,
        to_timedelta,
    )

    import dask.dataframe._dtypes
    import dask.dataframe._pyarrow_compat
    from dask.base import compute
    from dask.dataframe import backends, dispatch
    from dask.dataframe.groupby import Aggregation
    from dask.dataframe.io import demo
    from dask.dataframe.utils import assert_eq
except ImportError:
    import dask.dataframe as dd

    dd = importlib.reload(dd)
