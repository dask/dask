from ..client import Client

import pytest


class ClusterTest:
    Cluster = None
    kwargs = {}

    def setUp(self):
        self.cluster = self.Cluster(2, scheduler_port=0, **self.kwargs)
        self.client = Client(self.cluster.scheduler_address)

    def tearDown(self):
        self.client.close()
        self.cluster.close()

    @pytest.mark.xfail()
    def test_cores(self):
        info = self.client.scheduler_info()
        assert len(self.client.nthreads()) == 2

    def test_submit(self):
        future = self.client.submit(lambda x: x + 1, 1)
        assert future.result() == 2

    def test_context_manager(self):
        with self.Cluster(**self.kwargs) as c:
            with Client(c) as e:
                assert e.nthreads()

    def test_no_workers(self):
        with self.Cluster(0, scheduler_port=0, **self.kwargs):
            pass
