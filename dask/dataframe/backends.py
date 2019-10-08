from .methods import concat_dispatch
from .core import get_parallel_type, meta_nonempty, make_meta
from .utils import hash_object_dispatch, group_split_dispatch


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
