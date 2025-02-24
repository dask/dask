from __future__ import annotations

import dask.array._array_expr._backends
from dask.array._array_expr import random
from dask.array._array_expr._collection import Array, blockwise, elemwise, rechunk
from dask.array._array_expr._reductions import _tree_reduce, reduction
