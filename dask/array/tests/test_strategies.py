import hypothesis.strategies as st
import pytest
from hypothesis import given

from dask.array import strategies


class TestChunksStrategy:
    def test_invalid_args(self):
        with pytest.raises(ValueError):
            strategies.chunks(shape=0).example()

        with pytest.raises(ValueError):
            strategies.chunks(shape=(2, 3), axes=2).example()

        with pytest.raises(ValueError):
            strategies.chunks(shape=(2, 3), min_chunk_length=0).example()

        with pytest.raises(ValueError):
            strategies.chunks(shape=(2, 3), max_chunk_length=0).example()

    @pytest.mark.parametrize("shape", [(2, 3)])
    @pytest.mark.parametrize("axes", [None, 0, 1, (0, 1)])
    @pytest.mark.parametrize("min_chunk_length", [1, 3])
    @pytest.mark.parametrize("max_chunk_length", [None, 1, 3])
    @given(data=st.data())
    def test_valid_chunks(self, data, shape, axes, min_chunk_length, max_chunk_length):
        chunks = data.draw(strategies.chunks(shape, axes=axes))

        # assert that chunks add up to array lengths along all axes
        lengths = tuple(sum(list(chunks_along_axis)) for chunks_along_axis in chunks)
        expected = shape
        assert lengths == expected
