import pytest
from dask.dataframe import assert_eq

from dask_expr._collection import from_pandas
from dask_expr.tests._util import _backend_library

lib = _backend_library()


@pytest.fixture()
def ser():
    return lib.Series(["a", "b", "1", "aaa", "bbb", "ccc", "ddd", "abcd"])


@pytest.fixture()
def dser(ser):
    return from_pandas(ser, npartitions=3)


@pytest.mark.parametrize(
    "func, kwargs",
    [
        ("len", {}),
        ("capitalize", {}),
        ("casefold", {}),
        ("contains", {"pat": "a"}),
        ("count", {"pat": "a"}),
        ("endswith", {"pat": "a"}),
        ("extract", {"pat": r"[ab](\d)"}),
        ("extractall", {"pat": r"[ab](\d)"}),
        ("find", {"sub": "a"}),
        ("findall", {"pat": "a"}),
        ("fullmatch", {"pat": "a"}),
        ("get", {"i": 0}),
        ("isalnum", {}),
        ("isalpha", {}),
        ("isdecimal", {}),
        ("isdigit", {}),
        ("islower", {}),
        ("isspace", {}),
        ("istitle", {}),
        ("isupper", {}),
        ("join", {"sep": "-"}),
        ("len", {}),
        ("ljust", {"width": 3}),
        ("lower", {}),
        ("lstrip", {}),
        ("match", {"pat": r"[ab](\d)"}),
        ("normalize", {"form": "NFC"}),
        ("pad", {"width": 3}),
        ("removeprefix", {"prefix": "a"}),
        ("removesuffix", {"suffix": "a"}),
        ("repeat", {"repeats": 2}),
        ("replace", {"pat": "a", "repl": "b"}),
        ("rfind", {"sub": "a"}),
        ("rjust", {"width": 3}),
        ("rstrip", {}),
        ("slice", {"start": 0, "stop": 1}),
        ("slice_replace", {"start": 0, "stop": 1, "repl": "a"}),
        ("startswith", {"pat": "a"}),
        ("strip", {}),
        ("swapcase", {}),
        ("title", {}),
        ("upper", {}),
        ("wrap", {"width": 2}),
        ("zfill", {"width": 2}),
        ("split", {"pat": "a"}),
        ("rsplit", {"pat": "a"}),
        ("cat", {}),
        ("cat", {"others": lib.Series(["a"])}),
    ],
)
def test_string_accessor(ser, dser, func, kwargs):
    assert_eq(getattr(ser.str, func)(**kwargs), getattr(dser.str, func)(**kwargs))
