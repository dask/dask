from pbag import PBag


def first(x):
    return x[0]


def test_pbag():
    pb = PBag(first, 4)

    pb.extend([(i, i**2) for i in range(10)])
    pb.extend(iter([(i, i**3) for i in range(10)]))

    for j in range(4):
        assert pb.get_partition(j) == (
                [(i, i**2) for i in range(10) if i % 4 == j]
              + [(i, i**3) for i in range(10) if i % 4 == j])
