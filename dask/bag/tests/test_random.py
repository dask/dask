import dask.bag as db
from dask.bag import random


def test_choices_size():
    """
    Number of randomly sampled elements are exactly k.
    """
    a = db.from_sequence(range(20), npartitions=3)
    s = random.choices(a, k=2)
    assert len(list(s.compute())) == 2


def test_choices_size_over():
    """
    Number of randomly sampled are more than the elements.
    """
    a = db.from_sequence(range(3), npartitions=3)
    s = random.choices(a, k=4)
    assert len(list(s.compute())) == 4


def test_choices_size_over_repartition():
    """
    Number of randomly sampled are more than the elements on each partition.
    """
    a = db.from_sequence(range(10), partition_size=9)
    a = a.repartition(3)
    assert a.map_partitions(len).compute() == (9, 0, 1)
    s = random.choices(a, k=2)
    assert len(list(s.compute())) == 2


def test_choices_size_over_perpartition():
    """
    Number of randomly sampled are more than the elements of a partition.
    """
    a = db.from_sequence(range(10), partition_size=9)
    s = random.choices(a, k=2)
    assert len(list(s.compute())) == 2


def test_choices_size_over_two_perpartition():
    """
    Number of randomly sampled are more than the elements of two partitions.
    """
    a = db.from_sequence(range(10), partition_size=9)
    s = random.choices(a, k=10)
    assert len(list(s.compute())) == 10
