import pytest

import dask


@pytest.mark.gpu
@pytest.mark.parametrize("backend", ["pandas", "cudf"])
def test_CreationDispatch_error_informative_message(backend):
    # Check that an informative error is emitted when a backend dispatch
    # method fails
    pytest.importorskip(backend)
    dd = pytest.importorskip("dask.dataframe")
    data = {"a": [1, 2, 3, 4], "B": [10, 11, 12, 13]}
    with dask.config.set({"dataframe.backend": backend}):
        with pytest.raises(TypeError) as excinfo:
            dd.from_dict(data, npartitions=2, unsupported_kwarg=True)

        msg = str(excinfo.value)
        assert "error occurred while calling the from_dict method" in msg
        assert backend in msg


@pytest.mark.skipif(not pytest.mark.gpu, reason="cpu-only test")
def test_cudf_dispatch_error():
    # Check that the error message directs the user to
    # install dask_cudf after failing to dispatch to the
    # "cudf" backend (and dask_cudf is not installed)

    # Make sure dask_cudf is not installed
    try:
        import dask_cudf  # noqa: F401

        pytest.skip("cpu-only test")
    except ImportError:
        pass

    dd = pytest.importorskip("dask.dataframe")
    data = {"a": [1, 2, 3, 4], "B": [10, 11, 12, 13]}
    with dask.config.set({"dataframe.backend": "cudf"}):
        with pytest.raises(ValueError) as excinfo:
            dd.from_dict(data, npartitions=2)

        msg = str(excinfo.value)
        assert "Please try installing dask_cudf>=22.12" in msg


@pytest.mark.skipif(not pytest.mark.gpu, reason="cpu-only test")
def test_cupy_dispatch_error():
    # Check that the error message directs the user to
    # install cupy after failing to dispatch to the
    # "cupy" backend (and cupy is not installed)

    # Make sure cupy is not installed
    try:
        import cupy  # noqa: F401

        pytest.skip("cpu-only test")
    except ImportError:
        pass

    da = pytest.importorskip("dask.array")
    with dask.config.set({"array.backend": "cupy"}):
        with pytest.raises(ImportError) as excinfo:
            da.ones([0])

        msg = str(excinfo.value)
        assert "Please install `cupy`" in msg
