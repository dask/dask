from distributed.utils_test import cluster, loop

def test_cluster(loop):
    with cluster() as (c, [a, b]):
        pass

