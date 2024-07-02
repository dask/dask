import toolz
from dask.array.optimization import fuse_slice
from dask.array.slicing import normalize_slice, slice_array
from dask.array.utils import meta_from_array
from dask.utils import cached_property

from dask_expr.array.core import Array


class Slice(Array):
    _parameters = ["array", "index"]

    @property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=len(self.chunks))

    @cached_property
    def _info(self):
        return slice_array(
            self._name,
            self.array._name,
            self.array.chunks,
            self.index,
            self.array.dtype.itemsize,
        )

    def _layer(self):
        return self._info[0]

    @property
    def chunks(self):
        return self._info[1]

    def _simplify_down(self):
        if all(
            isinstance(idx, slice) and idx == slice(None, None, None)
            for idx in self.index
        ):
            return self.array
        if isinstance(self.array, Slice):
            return Slice(
                self.array.array,
                normalize_slice(
                    fuse_slice(self.array.index, self.index), self.array.array.ndim
                ),
            )

        if isinstance(self.array, Elemwise):
            index = self.index + (slice(None),) * (self.ndim - len(self.index))
            args = []
            for arg, ind in toolz.partition(2, self.array.args):
                if ind is None:
                    args.append(arg)
                else:
                    idx = tuple(index[self.array.out_ind.index(i)] for i in ind)
                    args.append(arg[idx])
            return Elemwise(*self.array.operands[: -len(args)], *args)

        if isinstance(self.array, Transpose):
            if any(isinstance(idx, (int)) or idx is None for idx in self.index):
                return None  # can't handle changes in dimension
            else:
                index = self.index + (slice(None),) * (self.ndim - len(self.index))
                new = tuple(index[i] for i in self.array.axes)
                return self.array.substitute(self.array.array, self.array.array[new])


from dask_expr.array.blockwise import Elemwise, Transpose
