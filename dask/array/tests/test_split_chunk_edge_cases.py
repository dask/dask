"""
Test for _split_up_single_chunk division by zero fix
"""

from dask.array.core import _split_up_single_chunk


def test_split_chunk_zero_proposed():
    """Test _split_up_single_chunk handles proposed=0 without ZeroDivisionError"""
    # Should not raise ZeroDivisionError
    result = _split_up_single_chunk(
        c=100, this_chunksize_tolerance=1.25, max_chunk_size=50, proposed=0
    )
    assert len(result) > 0
    assert sum(result) == 100


def test_split_chunk_both_zero():
    """Test _split_up_single_chunk when both proposed and max_chunk_size are zero"""
    # Should fallback to single chunk
    result = _split_up_single_chunk(
        c=100, this_chunksize_tolerance=1.25, max_chunk_size=0, proposed=0
    )
    assert result == [100]


def test_split_chunk_negative_proposed():
    """Test _split_up_single_chunk handles negative proposed value"""
    result = _split_up_single_chunk(
        c=100, this_chunksize_tolerance=1.25, max_chunk_size=50, proposed=-5
    )
    assert len(result) > 0
    assert sum(result) == 100


def test_split_chunk_normal_case():
    """Ensure normal operation still works correctly"""
    result = _split_up_single_chunk(
        c=100, this_chunksize_tolerance=1.25, max_chunk_size=50, proposed=25
    )
    assert sum(result) == 100
    # Should split into 4 chunks of 25 each
    assert result == [25, 25, 25, 25]
