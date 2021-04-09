try:
    from ..base import compute
    from . import backends, fft, lib, linalg, ma, overlap, random
    from .blockwise import atop, blockwise
    from .chunk_types import register_chunk_type
    from .core import (
        Array,
        PerformanceWarning,
        asanyarray,
        asarray,
        block,
        broadcast_arrays,
        broadcast_to,
        concatenate,
        from_array,
        from_delayed,
        from_npy_stack,
        from_zarr,
        map_blocks,
        stack,
        store,
        to_hdf5,
        to_npy_stack,
        to_zarr,
        unify_chunks,
    )
    from .creation import (
        arange,
        diag,
        diagonal,
        empty_like,
        eye,
        fromfunction,
        full_like,
        indices,
        linspace,
        meshgrid,
        ones_like,
        pad,
        repeat,
        tile,
        tri,
        zeros_like,
    )
    from .gufunc import apply_gufunc, as_gufunc, gufunc
    from .numpy_compat import moveaxis, rollaxis
    from .optimization import optimize
    from .overlap import map_overlap
    from .percentile import percentile
    from .rechunk import rechunk
    from .reductions import (
        all,
        any,
        argmax,
        argmin,
        argtopk,
        cumprod,
        cumsum,
        max,
        mean,
        median,
        min,
        moment,
        nanargmax,
        nanargmin,
        nancumprod,
        nancumsum,
        nanmax,
        nanmean,
        nanmedian,
        nanmin,
        nanprod,
        nanstd,
        nansum,
        nanvar,
        prod,
        reduction,
        std,
        sum,
        topk,
        trace,
        var,
    )
    from .reshape import reshape
    from .routines import (
        allclose,
        append,
        apply_along_axis,
        apply_over_axes,
        argwhere,
        around,
        array,
        atleast_1d,
        atleast_2d,
        atleast_3d,
        average,
        bincount,
        choose,
        coarsen,
        compress,
        corrcoef,
        count_nonzero,
        cov,
        delete,
        diff,
        digitize,
        dot,
        dstack,
        ediff1d,
        einsum,
        extract,
        flatnonzero,
        flip,
        fliplr,
        flipud,
        gradient,
        histogram,
        histogramdd,
        hstack,
        insert,
        isclose,
        isin,
        isnull,
        matmul,
        nonzero,
        notnull,
        outer,
        piecewise,
        ptp,
        ravel,
        ravel_multi_index,
        result_type,
        roll,
        rot90,
        round,
        shape,
        squeeze,
        swapaxes,
        take,
        tensordot,
        transpose,
        tril,
        tril_indices,
        tril_indices_from,
        triu,
        triu_indices,
        triu_indices_from,
        union1d,
        unique,
        unravel_index,
        vdot,
        vstack,
        where,
    )
    from .tiledb_io import from_tiledb, to_tiledb
    from .ufunc import (
        absolute,
        add,
        angle,
        arccos,
        arccosh,
        arcsin,
        arcsinh,
        arctan,
        arctan2,
        arctanh,
        bitwise_and,
        bitwise_not,
        bitwise_or,
        bitwise_xor,
        cbrt,
        ceil,
        clip,
        conj,
        copysign,
        cos,
        cosh,
        deg2rad,
        degrees,
        divide,
        divmod,
        equal,
        exp,
        exp2,
        expm1,
        fabs,
        fix,
        float_power,
        floor,
        floor_divide,
        fmax,
        fmin,
        fmod,
        frexp,
        frompyfunc,
        greater,
        greater_equal,
        hypot,
        i0,
        imag,
        invert,
        iscomplex,
        isfinite,
        isinf,
        isnan,
        isneginf,
        isposinf,
        isreal,
        ldexp,
        less,
        less_equal,
        log,
        log1p,
        log2,
        log10,
        logaddexp,
        logaddexp2,
        logical_and,
        logical_not,
        logical_or,
        logical_xor,
        maximum,
        minimum,
        mod,
        modf,
        multiply,
        nan_to_num,
        negative,
        nextafter,
        not_equal,
        power,
        rad2deg,
        radians,
        real,
        reciprocal,
        remainder,
        rint,
        sign,
        signbit,
        sin,
        sinc,
        sinh,
        spacing,
        sqrt,
        square,
        subtract,
        tan,
        tanh,
        true_divide,
        trunc,
    )
    from .utils import assert_eq
    from .wrap import empty, full, ones, zeros

except ImportError as e:
    msg = (
        "Dask array requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask                 # either conda install\n"
        '  python -m pip install "dask[array]" --upgrade  # or python -m pip install'
    )
    raise ImportError(str(e) + "\n\n" + msg) from e
