from .core import get_parallel_type, make_meta, meta_nonempty
from .methods import concat_dispatch
from .utils import group_split_dispatch, hash_object_dispatch

######################################
# cuDF: Pandas Dataframes on the GPU #
######################################

@concat_dispatch.register_lazy("pycylon")
@hash_object_dispatch.register_lazy("pycylon")
@group_split_dispatch.register_lazy("pycylon")
@get_parallel_type.register_lazy("pycylon")
@meta_nonempty.register_lazy("pycylon")
@make_meta.register_lazy("pycylon")
def _register_pycylon():
    print('inside register dask_cylon')
    import dask_cylon
    #from dask_cylon import backends

@concat_dispatch.register_lazy("cudf")
@hash_object_dispatch.register_lazy("cudf")
@group_split_dispatch.register_lazy("cudf")
@get_parallel_type.register_lazy("cudf")
@meta_nonempty.register_lazy("cudf")
@make_meta.register_lazy("cudf")
def _register_cudf():
    import dask_cudf  # noqa: F401


