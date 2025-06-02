# dask/array/kron.py

from __future__ import annotations

import numpy as np

import dask.array as da
from dask.array.blockwise import blockwise


def kron(x, y):
    """
    Kronecker product of two Dask arrays.

    - 2D x 2D: fully distributed using blockwise + adjust_chunks.
    - 1D x 1D, empty or ND > 2: fallback to NumPy and wrap in Dask.

    Producto de Kronecker entre dos arrays de Dask.

    - 2D x 2D: totalmente distribuido usando blockwise + adjust_chunks.
    - 1D x 1D, vacíos o ND > 2: usa NumPy y lo envuelve en Dask.

    Parameters / Parámetros
    ----------
    x : dask.array.Array
        First input array / Primer array de entrada.
    y : dask.array.Array
        Second input array / Segundo array de entrada.

    Returns / Retorna
    -------
    dask.array.Array
        Kronecker product / Producto de Kronecker.
    """
    x = da.asarray(x)
    y = da.asarray(y)
    dtype = np.result_type(x, y)

    # 1D case, empty or ND>2 → fallback to NumPy
    # Caso 1D, vacío o ND>2 → usa NumPy
    if x.ndim != 2 or y.ndim != 2 or x.size == 0 or y.size == 0:
        res = np.kron(x.compute(), y.compute())
        return da.from_array(res, chunks=res.shape)

    # 2D distributed case:
    # Caso 2D distribuido:
    # Each block x[i0:i1, j0:j1] and y[k0:k1, l0:l1] produces an output block
    # Cada bloque x[i0:i1, j0:j1] y y[k0:k1, l0:l1] produce un bloque de salida
    # np.kron(x_block, y_block) with shape ((i1-i0)*(k1-k0), (j1-j0)*(l1-l0))
    # np.kron(x_block, y_block) de forma ((i1-i0)*(k1-k0), (j1-j0)*(l1-l0))
    return blockwise(
        np.kron,                # function to apply per block / función por bloque
        "ab",                   # output axes / ejes de salida
        x, "ij",                # x has axes i,j / x tiene ejes i,j
        y, "kl",                # y has axes k,l / y tiene ejes k,l
        dtype=dtype,
        concatenate=True,       # concatenate resulting blocks / concatena bloques
        new_axes={
            "a": x.shape[0] * y.shape[0],  # total size of axis a / tamaño total eje a
            "b": x.shape[1] * y.shape[1],  # total size of axis b / tamaño total eje b
        },
        adjust_chunks={
            # For each chunk of x along 'i', scale by y.shape[0]
            # Para cada chunk de x en 'i', escalar por y.shape[0]
            "i": lambda cs: tuple(ci * y.shape[0] for ci in cs),
            # For each chunk of y along 'k', scale by x.shape[0]
            # Para cada chunk de y en 'k', escalar por x.shape[0]
            "k": lambda cs: tuple(ck * x.shape[0] for ck in cs),
            # For each chunk of x along 'j', scale by y.shape[1]
            # Para cada chunk de x en 'j', escalar por y.shape[1]
            "j": lambda cs: tuple(cj * y.shape[1] for cj in cs),
            # For each chunk of y along 'l', scale by x.shape[1]
            # Para cada chunk de y en 'l', escalar por x.shape[1]
            "l": lambda cs: tuple(cl * x.shape[1] for cl in cs),
        },
    )
