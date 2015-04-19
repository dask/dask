from pbag import PBag
import pickle


def first(x):
    return x[0]


def test_pbag():
    pb = PBag(first, 4)

    pb.extend([[i, i**2] for i in range(10)])
    pb.extend(iter([[i, i**3] for i in range(10)]))

    for j in range(4):
        assert pb.get_partition(j) == (
                [[i, i**2] for i in range(10) if i % 4 == j]
              + [[i, i**3] for i in range(10) if i % 4 == j])


def test_load():
    a = PBag(first, 4)
    a.extend([(i, i**2) for i in range(10)])

    b = PBag(first, 4, a.path)
    assert all(a.get_partition(i) == b.get_partition(i) for i in range(4))


def test_bags_are_serializable():
    a = PBag(first, 4)
    a.extend([(i, i**2) for i in range(10)])

    b = pickle.loads(pickle.dumps(a))
    assert b.dump == a.dump
    assert b.load == a.load
    assert b.path == a.path
    assert b.npartitions == a.npartitions
    assert b.grouper == a.grouper


def test_pbags_are_serializable_with_lambdas():
    a = PBag(lambda x: x[0], 4)
    a.extend([(i, i**2) for i in range(10)])

    b = pickle.loads(pickle.dumps(a))
    assert b.grouper('hello') == 'h'
