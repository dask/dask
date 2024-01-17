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
        from_array,
        from_dask_array,
        from_dask_dataframe,
        from_delayed,
        from_dict,
        from_graph,
        from_map,
        from_pandas,
        get_dummies,
        isna,
        map_overlap,
        map_partitions,
        merge,
        merge_asof,
        pivot_table,
        read_csv,
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

    import dask._dataframe._pyarrow_compat
    from dask._dataframe import backends, dispatch
    from dask._dataframe.io import demo
    from dask._dataframe.utils import assert_eq
    from dask.base import compute

    def raise_not_implemented_error(attr_name):
        def inner_func(*args, **kwargs):
            raise NotImplementedError(
                f"Function {attr_name} is not implemented for dask-expr."
            )

        return inner_func

    _Frame = raise_not_implemented_error("_Frame")
    Aggregation = raise_not_implemented_error("Aggregation")
    read_fwf = raise_not_implemented_error("read_fwf")
    melt = raise_not_implemented_error("melt")

else:
    try:
        import dask._dataframe._pyarrow_compat
        from dask._dataframe import backends, dispatch, rolling
        from dask._dataframe.core import (
            DataFrame,
            Index,
            Series,
            _Frame,
            map_partitions,
            repartition,
            to_datetime,
            to_timedelta,
        )
        from dask._dataframe.groupby import Aggregation
        from dask._dataframe.multi import concat, merge, merge_asof
        from dask._dataframe.numeric import to_numeric
        from dask._dataframe.optimize import optimize
        from dask._dataframe.reshape import get_dummies, melt, pivot_table
        from dask._dataframe.rolling import map_overlap
        from dask._dataframe.utils import assert_eq
        from dask.base import compute
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

        try:
            from dask.dataframe.io import read_parquet, to_parquet
        except ImportError:
            pass
        try:
            from dask.dataframe.io import read_orc, to_orc
        except ImportError:
            pass
        try:
            from dask._dataframe.core import isna
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
