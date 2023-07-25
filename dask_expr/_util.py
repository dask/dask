from __future__ import annotations

from collections.abc import Sequence
from types import LambdaType

from dask import config
from dask.base import normalize_token, tokenize


def _convert_to_list(column) -> list | None:
    if column is None or isinstance(column, list):
        pass
    elif isinstance(column, tuple):
        column = list(column)
    elif hasattr(column, "dtype"):
        column = column.tolist()
    else:
        column = [column]
    return column


def is_scalar(x):
    # np.isscalar does not work for some pandas scalars, for example pd.NA
    return not (isinstance(x, Sequence) or hasattr(x, "dtype")) or isinstance(x, str)


@normalize_token.register(LambdaType)
def _normalize_lambda(func):
    return str(func)


def _tokenize_deterministic(*args, **kwargs):
    # Utility to be strict about deterministic tokens
    with config.set({"tokenize.ensure-deterministic": True}):
        return tokenize(*args, **kwargs)
