from .methods import concat_dispatch
from .core import get_parallel_type, meta_nonempty, make_meta


######################################
# cuDF: Pandas Dataframes on the GPU #
######################################


@concat_dispatch.register_lazy('cudf')
@get_parallel_type.register_lazy('cudf')
@meta_nonempty.register_lazy('cudf')
@make_meta.register_lazy('cudf')
def _register_cudf():
    import cudf
    import dask_cudf
    get_parallel_type.register(cudf.DataFrame, lambda _: dask_cudf.DataFrame)
    get_parallel_type.register(cudf.Series, lambda _: dask_cudf.Series)
    get_parallel_type.register(cudf.Index, lambda _: dask_cudf.Index)

    @meta_nonempty.register((cudf.DataFrame, cudf.Series, cudf.Index))
    def _(x):
        y = meta_nonempty(x.to_pandas())  # TODO: add iloc[:5]
        return cudf.from_pandas(y)

    @make_meta.register((cudf.Series, cudf.DataFrame))
    def _(x):
        return x.head(0)

    @make_meta.register(cudf.Index)
    def _(x):
        return x[:0]

    concat_dispatch.register(
        (cudf.DataFrame, cudf.Series, cudf.Index),
        cudf.concat
    )
