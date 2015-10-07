from distributed.utils_test import cluster

def test_cluster():
    with cluster() as (c, [a, b]):
        pass

