"""
Dispatch in dask.array.

Also see backends.py
"""

from dask.backends import CreationDispatch, DaskBackendEntrypoint
from dask.utils import Dispatch

concatenate_lookup = Dispatch("concatenate")
tensordot_lookup = Dispatch("tensordot")
einsum_lookup = Dispatch("einsum")
empty_lookup = Dispatch("empty")
divide_lookup = Dispatch("divide")
percentile_lookup = Dispatch("percentile")
numel_lookup = Dispatch("numel")
nannumel_lookup = Dispatch("nannumel")


array_creation_dispatch = CreationDispatch(
    config_field="array.backend.library",
    default="numpy",
    name="array_creation_dispatch",
)


class ArrayBackendEntrypoint(DaskBackendEntrypoint):
    def __init__(self):
        """Register data-directed dispatch functions"""
        raise NotImplementedError

    def ones(self, *args, **kwargs):
        raise NotImplementedError

    def zeros(self, *args, **kwargs):
        raise NotImplementedError

    def empty(self, *args, **kwargs):
        raise NotImplementedError

    def full(self, *args, **kwargs):
        raise NotImplementedError

    def arange(self, *args, **kwargs):
        raise NotImplementedError

    def default_random_state(self):
        raise NotImplementedError

    def new_random_state(self, state):
        raise NotImplementedError

    def from_array(self, *args, **kwargs):
        raise NotImplementedError
