from dask_expr._accessor import Accessor
from dask_expr._expr import Blockwise
from dask_expr._reductions import Reduction


class StringAccessor(Accessor):
    """Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """

    _accessor_name = "str"

    _accessor_methods = (
        "capitalize",
        "casefold",
        "center",
        "contains",
        "count",
        "decode",
        "encode",
        "endswith",
        "extract",
        "extractall",
        "find",
        "findall",
        "fullmatch",
        "get",
        "index",
        "isalnum",
        "isalpha",
        "isdecimal",
        "isdigit",
        "islower",
        "isnumeric",
        "isspace",
        "istitle",
        "isupper",
        "join",
        "len",
        "ljust",
        "lower",
        "lstrip",
        "match",
        "normalize",
        "pad",
        "partition",
        "removeprefix",
        "removesuffix",
        "repeat",
        "replace",
        "rfind",
        "rindex",
        "rjust",
        "rpartition",
        "rstrip",
        "slice",
        "slice_replace",
        "startswith",
        "strip",
        "swapcase",
        "title",
        "translate",
        "upper",
        "wrap",
        "zfill",
    )
    _accessor_properties = ()

    def _split(self, method, pat=None, n=-1, expand=False):
        if expand:
            if n == -1:
                raise NotImplementedError(
                    "To use the expand parameter you must specify the number of "
                    "expected splits with the n= parameter. Usually n splits "
                    "result in n+1 output columns."
                )
        return self._function_map(method, pat=pat, n=n, expand=expand)

    def split(self, pat=None, n=-1, expand=False):
        """Known inconsistencies: ``expand=True`` with unknown ``n`` will raise a ``NotImplementedError``."""
        return self._split("split", pat=pat, n=n, expand=expand)

    def rsplit(self, pat=None, n=-1, expand=False):
        return self._split("rsplit", pat=pat, n=n, expand=expand)

    def cat(self, others=None, sep=None, na_rep=None):
        import pandas as pd
        from dask.dataframe.core import Index, Series

        from dask_expr._collection import new_collection

        if others is None:
            return new_collection(Cat(self._series.expr, sep, na_rep))

        valid_types = (Series, Index, pd.Series, pd.Index)
        if isinstance(others, valid_types):
            others = [others]
        elif not all(isinstance(a, valid_types) for a in others):
            raise TypeError("others must be Series/Index")

        return new_collection(CatBlockwise(self._series.expr, others, sep, na_rep))

    def __getitem__(self, index):
        return self._function_map("__getitem__", index)


class CatBlockwise(Blockwise):
    _parameters = ["frame", "others", "sep", "na_rep"]

    @staticmethod
    def operation(ser, *args, **kwargs):
        return ser.str.cat(*args, **kwargs)


class Cat(Reduction):
    _parameters = ["frame", "sep", "na_rep"]

    @property
    def chunk_kwargs(self):
        return {"sep": self.sep, "na_rep": self.na_rep}

    @staticmethod
    def reduction_chunk(ser, *args, **kwargs):
        return ser.str.cat(*args, **kwargs)
