"""
Test for potential division by zero in _split_up_single_chunk
"""
import dask.array as da
import numpy as np


def test_split_chunk_with_zero_proposed():
    """Test that _split_up_single_chunk handles proposed=0 gracefully"""
    from dask.array.core import _split_up_single_chunk
    
    # Test 1: proposed=0 with valid max_chunk_size
    try:
        result = _split_up_single_chunk(
            c=100,
            this_chunksize_tolerance=1.25,
            max_chunk_size=50,
            proposed=0
        )
        print(f"✓ Test 1 passed - proposed=0: {result}")
        assert len(result) > 0
        assert sum(result) == 100
    except ZeroDivisionError as e:
        print(f"✗ Test 1 failed - ZeroDivisionError: {e}")
        raise
        
    # Test 2: proposed=0 AND max_chunk_size=0 (worst case)
    try:
        result = _split_up_single_chunk(
            c=100,
            this_chunksize_tolerance=1.25,
            max_chunk_size=0,
            proposed=0
        )
        print(f"✓ Test 2 passed - both zero: {result}")
        assert result == [100]  # Fallback to single chunk
    except Exception as e:
        print(f"✗ Test 2 failed: {e}")
        raise
        
    # Test 3: Negative values
    try:
        result = _split_up_single_chunk(
            c=100,
            this_chunksize_tolerance=1.25,
            max_chunk_size=50,
            proposed=-5
        )
        print(f"✓ Test 3 passed - negative proposed: {result}")
        assert len(result) > 0
        assert sum(result) == 100
    except Exception as e:
        print(f"✗ Test 3 failed: {e}")
        raise
    
    # Test 4: Normal case still works
    try:
        result = _split_up_single_chunk(
            c=100,
            this_chunksize_tolerance=1.25,
            max_chunk_size=50,
            proposed=25
        )
        print(f"✓ Test 4 passed - normal case: {result}")
        assert sum(result) == 100
    except Exception as e:
        print(f"✗ Test 4 failed: {e}")
        raise
        

def test_auto_chunking_edge_cases():
    """Test auto chunking with edge cases"""
    # Test with very small limit that might cause issues
    try:
        x = da.zeros((1000, 1000), chunks='auto', dtype='uint8')
        x = x.rechunk(chunks=(1, 'auto'))
        print(f"✓ Auto rechunk succeeded: {x.chunks}")
    except Exception as e:
        print(f"✗ Auto rechunk failed: {e}")
        
    # Test with zero-sized dimensions
    try:
        x = da.zeros((0, 100), chunks='auto')
        print(f"✓ Zero-sized auto chunk: {x.chunks}")
    except Exception as e:
        print(f"✗ Zero-sized failed: {e}")


if __name__ == "__main__":
    test_split_chunk_with_zero_proposed()
    test_auto_chunking_edge_cases()
