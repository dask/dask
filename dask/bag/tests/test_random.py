import pytest

import dask.bag as db
from dask.bag import random


def test_choices_size_exactly_k():
    seq = range(20)
    sut = db.from_sequence(seq, npartitions=3)
    li = list(random.choices(sut, k=2).compute())
    assert len(li) == 2
    assert all(i in seq for i in li)


def test_choices_k_bigger_than_bag_size():
    seq = range(3)
    sut = db.from_sequence(seq, npartitions=3)
    li = list(random.choices(sut, k=4).compute())
    assert len(li) == 4
    assert all(i in seq for i in li)


def test_choices_empty_partition():
    seq = range(10)
    sut = db.from_sequence(seq, partition_size=9)
    sut = sut.repartition(3)
    li = list(random.choices(sut, k=2).compute())
    assert sut.map_partitions(len).compute() == (9, 0, 1)
    assert len(li) == 2
    assert all(i in seq for i in li)


def test_choices_k_bigger_than_smallest_partition_size():
    seq = range(10)
    sut = db.from_sequence(seq, partition_size=9)
    li = list(random.choices(sut, k=2).compute())
    assert sut.map_partitions(len).compute() == (9, 1)
    assert len(li) == 2
    assert all(i in seq for i in li)


def test_choices_k_equal_bag_size_with_unbalanced_partitions():
    seq = range(10)
    sut = db.from_sequence(seq, partition_size=9)
    li = list(random.choices(sut, k=10).compute())
    assert sut.map_partitions(len).compute() == (9, 1)
    assert len(li) == 10
    assert all(i in seq for i in li)


def test_sample_size_exactly_k():
    seq = range(20)
    sut = db.from_sequence(seq, npartitions=3)
    li = list(random.sample(sut, k=2).compute())
    assert sut.map_partitions(len).compute() == (7, 7, 6)
    assert len(li) == 2
    assert all(i in seq for i in li)
    assert len(set(li)) == len(li)


def test_sample_k_bigger_than_bag_size():
    seq = range(3)
    sut = db.from_sequence(seq, npartitions=3)
    # should raise: Sample larger than population or is negative
    with pytest.raises(
        ValueError, match="Sample larger than population or is negative"
    ):
        random.sample(sut, k=4).compute()


def test_sample_empty_partition():
    seq = range(10)
    sut = db.from_sequence(seq, partition_size=9)
    sut = sut.repartition(3)
    li = list(random.sample(sut, k=2).compute())
    assert sut.map_partitions(len).compute() == (9, 0, 1)
    assert len(li) == 2
    assert all(i in seq for i in li)
    assert len(set(li)) == len(li)


def test_sample_size_k_bigger_than_smallest_partition_size():
    seq = range(10)
    sut = db.from_sequence(seq, partition_size=9)
    li = list(random.sample(sut, k=2).compute())
    assert sut.map_partitions(len).compute() == (9, 1)
    assert len(li) == 2
    assert all(i in seq for i in li)
    assert len(set(li)) == len(li)


def test_sample_k_equal_bag_size_with_unbalanced_partitions():
    seq = range(10)
    sut = db.from_sequence(seq, partition_size=9)
    li = list(random.sample(sut, k=10).compute())
    assert sut.map_partitions(len).compute() == (9, 1)
    assert len(li) == 10
    assert all(i in seq for i in li)
    assert len(set(li)) == len(li)


def test_weighted_sampling_without_replacement():
    population = range(4)
    p = [0.01, 0.33, 0.33, 0.33]
    k = 3
    sampled = random._weighted_sampling_without_replacement(
        population=population, weights=p, k=k
    )
    assert len(set(sampled)) == k
