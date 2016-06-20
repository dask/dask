from ..executor import Executor

class ClusterTest(object):
    Cluster = None

    def setUp(self):
        self.cluster = self.Cluster(2, scheduler_port=0)
        self.executor = Executor(self.cluster.scheduler_address)

    def tearDown(self):
        self.executor.shutdown()
        self.cluster.close()

    def test_cores(self):
        assert len(self.executor.ncores()) == 2

    def test_submit(self):
        future = self.executor.submit(lambda x: x + 1, 1)
        assert future.result() == 2

    def test_start_worker(self):
        a = self.executor.ncores()
        w = self.cluster.start_worker(ncores=3)
        b = self.executor.ncores()

        assert len(b) == 1 + len(a)
        assert any(v == 3 for v in b.values())

        self.cluster.stop_worker(w)

        c = self.executor.ncores()
        assert c == a

    def test_context_manager(self):
        with self.Cluster() as c:
            with Executor(c) as e:
                assert e.ncores()
