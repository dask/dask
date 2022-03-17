import operator
import warnings

import numpy as np

import dask.dataframe as dd
from dask.blockwise import Blockwise
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from dask.utils import M, apply, is_arraylike


def attempt_predicate_pushdown(ddf: dd.DataFrame) -> dd.DataFrame:
    """Use graph information to update IO-level filters

    The original `ddf` will be returned if/when the
    predicate-pushdown optimization fails.

    This is a special optimization that must be called
    eagerly on a DataFrame collection when filters are
    applied. The "eager" requirement for this optimization
    is due to the fact that `npartitions` and `divisions`
    may change when this optimization is applied (invalidating
    npartition/divisions-specific logic in following Layers).
    """

    # Get output layer name and HLG
    name = ddf._name

    # Start by converting the HLG to a `RegenerableGraph`.
    # Succeeding here means that all layers in the graph
    # are regenerable.
    try:
        dsk = RegenerableGraph.from_hlg(ddf.dask)
    except (ValueError, TypeError):
        return ddf

    # Extract a DNF-formatted filter expression
    try:
        filters = dsk.layers[name]._dnf_filter_expression(dsk)
        if filters:
            if isinstance(filters[0], (list, tuple)):
                filters = list(filters)
            else:
                filters = [filters]
        else:
            return ddf
        if not isinstance(filters, list):
            filters = [filters]
    except ValueError:
        # DNF dispatching failed for 1+ layers
        return ddf

    # We were able to extract a DNF filter expression.
    # Check that we have a single IO layer with `filters` support
    io_layer = []
    for k, v in dsk.layers.items():
        if isinstance(v.layer, DataFrameIOLayer):
            io_layer.append(k)
            if (
                "filters" not in v.creation_info.get("kwargs", {})
                or v.creation_info["kwargs"]["filters"] is not None
            ):
                # No filters support, or filters is already set
                return ddf
    if len(io_layer) != 1:
        # Not a single IO layer
        return ddf
    io_layer = io_layer.pop()

    # Regenerate collection with filtered IO layer
    try:
        return dsk.layers[name]._regenerate_collection(
            dsk,
            new_kwargs={io_layer: {"filters": filters}},
        )
    except ValueError as err:
        # Most-likely failed to apply filters in read_parquet.
        # We can just bail on predicate pushdown, but we also
        # raise a warning to encourage the user to file an issue.
        warnings.warn(
            f"Predicate pushdown failed for filters={filters}. "
            f"Please open a bug report at "
            f"https://github.com/dask/dask/issues/new/choose "
            f"and include the following error message: {err}"
        )

        return ddf


# Define all supported comparison functions
# (and their mapping to a string expression)
_comparison_symbols = {
    operator.eq: "==",
    operator.ne: "!=",
    operator.lt: "<",
    operator.le: "<=",
    operator.gt: ">",
    operator.ge: ">=",
    np.greater: ">",
    np.greater_equal: ">=",
    np.less: "<",
    np.less_equal: "<=",
    np.equal: "==",
    np.not_equal: "!=",
}

# Define set of all "regenerable" operations.
# Predicate pushdown is supported for graphs
# comprised of `Blockwise` layers based on these
# operations
_regenerable_ops = set(_comparison_symbols.keys()) | {
    operator.and_,
    operator.or_,
    operator.getitem,
    M.fillna,
}

# Specify functions that must be generated with
# a different API at the dataframe-collection level
_special_op_mappings = {M.fillna: dd._Frame.fillna}


class RegenerableLayer:
    """Regenerable Layer

    Wraps ``dask.highlevelgraph.Blockwise`` to ensure that a
    ``creation_info`` attribute  is defined. This class
    also defines the necessary methods for recursive
    layer regeneration and filter-expression generation.
    """

    def __init__(self, layer, creation_info):
        self.layer = layer  # Original Blockwise layer reference
        self.creation_info = creation_info

    def _regenerate_collection(
        self,
        dsk,
        new_kwargs: dict = None,
        _regen_cache: dict = None,
    ):
        """Regenerate a Dask collection for this layer using the
        provided inputs and key-word arguments
        """

        # Return regenerated layer if the work was
        # already done
        _regen_cache = _regen_cache or {}
        if self.layer.output in _regen_cache:
            return _regen_cache[self.layer.output]

        # Recursively generate necessary inputs to
        # this layer to generate the collection
        inputs = []
        for key, ind in self.layer.indices:
            if ind is None:
                if isinstance(key, (str, tuple)) and key in dsk.layers:
                    continue
                inputs.append(key)
            elif key in self.layer.io_deps:
                continue
            else:
                inputs.append(
                    dsk.layers[key]._regenerate_collection(
                        dsk,
                        new_kwargs=new_kwargs,
                        _regen_cache=_regen_cache,
                    )
                )

        # Extract the callable func and key-word args.
        # Then return a regenerated collection
        func = self.creation_info.get("func", None)
        if func is None:
            raise ValueError(
                "`_regenerate_collection` failed. "
                "Not all HLG layers are regenerable."
            )
        regen_args = self.creation_info.get("args", [])
        regen_kwargs = self.creation_info.get("kwargs", {}).copy()
        regen_kwargs = {k: v for k, v in self.creation_info.get("kwargs", {}).items()}
        regen_kwargs.update((new_kwargs or {}).get(self.layer.output, {}))
        result = func(*inputs, *regen_args, **regen_kwargs)
        _regen_cache[self.layer.output] = result
        return result

    def _dnf_filter_expression(self, dsk):
        """Return a DNF-formatted filter expression for the
        graph terminating at this layer
        """
        op = self.creation_info["func"]
        if op in _comparison_symbols.keys():
            func = _blockwise_comparison_dnf
        elif op in (operator.and_, operator.or_):
            func = _blockwise_logical_dnf
        elif op == operator.getitem:
            func = _blockwise_getitem_dnf
        elif op == dd._Frame.fillna:
            func = _blockwise_fillna_dnf
        else:
            raise ValueError(f"No DNF expression for {op}")

        return func(op, self.layer.indices, dsk)


class RegenerableGraph:
    """Regenerable Graph

    This class is similar to ``dask.highlevelgraph.HighLevelGraph``.
    However, all layers in a ``RegenerableGraph`` graph must be
    ``RegenerableLayer`` objects (which wrap ``Blockwise`` layers).
    """

    def __init__(self, layers: dict):
        self.layers = layers

    @classmethod
    def from_hlg(cls, hlg: HighLevelGraph):
        """Construct a ``RegenerableGraph`` from a ``HighLevelGraph``"""

        if not isinstance(hlg, HighLevelGraph):
            raise TypeError(f"Expected HighLevelGraph, got {type(hlg)}")

        _layers = {}
        for key, layer in hlg.layers.items():
            regenerable_layer = None
            if isinstance(layer, DataFrameIOLayer):
                regenerable_layer = RegenerableLayer(layer, layer.creation_info or {})
            elif isinstance(layer, Blockwise):
                tasks = list(layer.dsk.values())
                if len(tasks) == 1 and tasks[0]:
                    kwargs = {}
                    if tasks[0][0] == apply:
                        op = tasks[0][1]
                        options = tasks[0][3]
                        if isinstance(options, dict):
                            kwargs = options
                        elif (
                            isinstance(options, tuple)
                            and options
                            and callable(options[0])
                        ):
                            kwargs = options[0](*options[1:])
                    else:
                        op = tasks[0][0]
                    if op in _regenerable_ops:
                        regenerable_layer = RegenerableLayer(
                            layer,
                            {
                                "func": _special_op_mappings.get(op, op),
                                "kwargs": kwargs,
                            },
                        )

            if regenerable_layer is None:
                raise ValueError(f"Graph contains non-regenerable layer: {layer}")

            _layers[key] = regenerable_layer

        return RegenerableGraph(_layers)


def _get_blockwise_input(input_index, indices: list, dsk: RegenerableGraph):
    # Simple utility to get the required input expressions
    # for a Blockwise layer (using indices)
    key = indices[input_index][0]
    if indices[input_index][1] is None:
        return key
    return dsk.layers[key]._dnf_filter_expression(dsk)


def _blockwise_comparison_dnf(op, indices: list, dsk: RegenerableGraph):
    # Return DNF expression pattern for a simple comparison
    left = _get_blockwise_input(0, indices, dsk)
    right = _get_blockwise_input(1, indices, dsk)

    def _inv(symbol: str):
        return {
            ">": "<",
            "<": ">",
            ">=": "<=",
            "<=": ">=",
        }.get(symbol, symbol)

    if is_arraylike(left) and hasattr(left, "item") and left.size == 1:
        left = left.item()
        # Need inverse comparison in read_parquet
        return (right, _inv(_comparison_symbols[op]), left)
    if is_arraylike(right) and hasattr(right, "item") and right.size == 1:
        right = right.item()
    return (left, _comparison_symbols[op], right)


def _blockwise_logical_dnf(op, indices: list, dsk: RegenerableGraph):
    # Return DNF expression pattern for logical "and" or "or"
    left = _get_blockwise_input(0, indices, dsk)
    right = _get_blockwise_input(1, indices, dsk)

    def _maybe_list(val):
        if isinstance(val, tuple) and val and isinstance(val[0], (tuple, list)):
            return list(val)
        return [val]

    def _maybe_tuple(val):
        if isinstance(val, tuple) and val and isinstance(val[0], tuple):
            return val
        return (val,)

    if op == operator.or_:
        # NDF "or" is List[List[Tuple]]
        return [_maybe_list(left), _maybe_list(right)]
    elif op == operator.and_:
        # NDF "and" is List[Tuple]
        # However, we don't want to add the outer list
        # until the filter is finished, or this expression
        # is combined with another in an "or" expression
        return _maybe_tuple(left) + _maybe_tuple(right)
    else:
        raise ValueError


def _blockwise_getitem_dnf(op, indices: list, dsk: RegenerableGraph):
    # Return dnf of key (selected by getitem)
    key = _get_blockwise_input(1, indices, dsk)
    return key


def _blockwise_fillna_dnf(op, indices: list, dsk: RegenerableGraph):
    # Return dnf of input collection
    return _get_blockwise_input(0, indices, dsk)
