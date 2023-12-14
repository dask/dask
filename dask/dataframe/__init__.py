from __future__ import annotations


def _dask_expr_enabled() -> bool:
    import dask

    use_dask_expr = dask.config.get("dataframe.query-planning")
    if use_dask_expr:
        try:
            import dask_expr  # noqa: F401
        except ImportError:
            raise ValueError("Must install dask-expr to activate query planning.")
    return use_dask_expr


if _dask_expr_enabled():
    from dask_expr import (
        DataFrame,
        Index,
        Series,
        concat,
        from_dask_dataframe,
        from_graph,
        from_map,
        from_pandas,
        merge,
        pivot_table,
        read_csv,
        read_parquet,
        repartition,
        to_datetime,
        to_numeric,
        to_parquet,
        to_timedelta,
    )

    import dask.dataframe._pyarrow_compat
    from dask.base import compute
    from dask.dataframe import backends, dispatch
    from dask.dataframe.utils import assert_eq

    def raise_not_implemented_error(attr_name):
        def inner_func(*args, **kwargs):
            raise NotImplementedError(
                f"Function {attr_name} is not implemented for dask-expr."
            )

        return inner_func

    _Frame = raise_not_implemented_error("_Frame")
    map_partitions = raise_not_implemented_error("map_partitions")
    Aggregation = raise_not_implemented_error("Aggregation")
    from_array = raise_not_implemented_error("from_array")
    from_dask_array = raise_not_implemented_error("from_dask_array")
    from_delayed = raise_not_implemented_error("from_delayed")
    from_dict = raise_not_implemented_error("from_dict")
    read_fwf = raise_not_implemented_error("read_fwf")
    read_hdf = raise_not_implemented_error("read_hdf")
    read_json = raise_not_implemented_error("read_json")
    read_sql = raise_not_implemented_error("read_sql")
    read_sql_query = raise_not_implemented_error("read_sql_query")
    read_sql_table = raise_not_implemented_error("read_sql_table")
    read_table = raise_not_implemented_error("read_table")
    to_bag = raise_not_implemented_error("to_bag")
    to_csv = raise_not_implemented_error("to_csv")
    to_hdf = raise_not_implemented_error("to_hdf")
    to_json = raise_not_implemented_error("to_json")
    to_records = raise_not_implemented_error("to_records")
    to_sql = raise_not_implemented_error("to_sql")
    merge_asof = raise_not_implemented_error("merge_asof")
    get_dummies = raise_not_implemented_error("get_dummies")
    melt = raise_not_implemented_error("melt")
    read_orc = raise_not_implemented_error("read_orc")
    to_orc = raise_not_implemented_error("to_orc")
    isna = raise_not_implemented_error("isna")

else:
    try:
        import dask.dataframe._pyarrow_compat
        from dask.base import compute
        from dask.dataframe import backends, dispatch, rolling
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
