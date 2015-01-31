from dask.multiprocessing import get, dill_apply_async
import multiprocessing
import dill


def test_apply_lambda():
    p = multiprocessing.Pool()
    try:
        result = dill_apply_async(p.apply_async, lambda x: x + 1, args=[1])
        assert isinstance(result, multiprocessing.pool.ApplyResult)
        assert result.get() == 2
    finally:
        p.close()
