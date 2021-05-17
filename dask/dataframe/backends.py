from .core import get_parallel_type, make_meta, meta_nonempty
from .methods import concat_dispatch
from .utils import group_split_dispatch, hash_object_dispatch

######################################
# cuDF: Pandas Dataframes on the GPU #
######################################


@concat_dispatch.register_lazy("cudf")
@hash_object_dispatch.register_lazy("cudf")
@group_split_dispatch.register_lazy("cudf")
@get_parallel_type.register_lazy("cudf")
@meta_nonempty.register_lazy("cudf")
@make_meta.register_lazy("cudf")
def _register_cudf():
    import dask_cudf  # noqa: F401
