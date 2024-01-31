from __future__ import annotations


def _dask_expr_enabled() -> bool:
    import dask

    use_dask_expr = dask.config.get("dataframe.query-planning")
    if use_dask_expr:
        try:
            import dask_expr  # noqa: F401
        except ImportError:
            raise ValueError("Must install dask-expr to activate query planning.")
    return use_dask_expr if use_dask_expr is not None else False


if _dask_expr_enabled():
    try:
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

        import dask.dataframe._pyarrow_compat
        from dask.base import compute
        from dask.dataframe import backends, dispatch
        from dask.dataframe.io import demo
        from dask.dataframe.utils import assert_eq

        def raise_not_implemented_error(attr_name):
            def inner_func(*args, **kwargs):
                raise NotImplementedError(
                    f"Function {attr_name} is not implemented for dask-expr."
                )

            return inner_func

        _Frame = raise_not_implemented_error("_Frame")
        Aggregation = raise_not_implemented_error("Aggregation")
        melt = raise_not_implemented_error("melt")

    # Due to the natural circular imports caused from dask-expr
    # wanting to import things from dask.dataframe, this module's init
    # can be run multiple times as it walks code trying to import
    # dask-expr while dask-expr is also trying to import from dask.dataframe
    # Each time this happens and hits a circular import, we can reload
    # dask.dataframe to update itself until dask-expr is fully initialized.
    # TODO: This can go away when dask-expr is merged into dask
    except ImportError:
        import importlib

        import dask.dataframe as dd

        dd = importlib.reload(dd)

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
        from dask.dataframe.rolling import map_overlap
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


from dask.dataframe._testing import test_dataframe
