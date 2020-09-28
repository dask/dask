import pytest
from distributed import LocalCluster, Client
from distributed.utils import LoopRunner


# Test if Client stops LoopRunner on close.
@pytest.mark.parametrize("with_own_loop", [True, False])
def test_close_loop_sync(with_own_loop):
    loop_runner = loop = None

    # Setup simple cluster with one threaded worker.
    # Complex setup is not required here since we test only IO loop teardown.
    cluster_params = dict(n_workers=1, dashboard_address=None, processes=False)

    loops_before = LoopRunner._all_loops.copy()

    # Start own loop or use current thread's one.
    if with_own_loop:
        loop_runner = LoopRunner()
        loop_runner.start()
        loop = loop_runner.loop

    with LocalCluster(loop=loop, **cluster_params) as cluster:
        with Client(cluster, loop=loop) as client:
            client.run(max, 1, 2)

    # own loop must be explicitly stopped.
    if with_own_loop:
        loop_runner.stop()

    # Internal loops registry must the same as before cluster running.
    # This means loop runners in LocalCluster and Client correctly stopped.
    # See LoopRunner._stop_unlocked().
    assert loops_before == LoopRunner._all_loops
