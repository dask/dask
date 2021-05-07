import pytest

import dask


def test_mimesis():
    pytest.importorskip("mimesis")

    b = dask.datasets.make_people()
    assert b.take(5)

    assert b.take(3) == b.take(3)


def test_full_dataset():
    pytest.importorskip("mimesis")
    b = dask.datasets.make_people(npartitions=2, records_per_partition=10)
    assert b.count().compute() == 20


def test_no_mimesis():
    try:
        import mimesis  # noqa: F401
    except ImportError:
        with pytest.raises(Exception) as info:
            dask.datasets.make_people()

        assert "python -m pip install mimesis" in str(info.value)


def test_deterministic():
    pytest.importorskip("mimesis")

    b = dask.datasets.make_people(seed=123)
    assert b.take(1)[0]["name"] == ("Leandro", "Orr")
