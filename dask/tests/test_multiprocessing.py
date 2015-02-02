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


def bad():
    raise ValueError("12345")


def test_errors_propagate():
    dsk = {'x': (bad,)}

    try:
        result = get(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ValueError)
        assert "bad" in str(e)
        assert "12345" in str(e)


def make_bad_result():
    return lambda x: x + 1


def test_unpicklable_results_genreate_errors():

    dsk = {'x': (make_bad_result,)}

    try:
        result = get(dsk, 'x')
    except Exception as e:
        # can't use type because pickle / cPickle distinction
        assert type(e).__name__ == 'PicklingError'
