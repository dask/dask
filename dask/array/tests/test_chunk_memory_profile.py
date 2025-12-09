"""
Tests for chunk memory profiling utilities.
"""

import math

import numpy as np
import pytest

from dask.array.chunk_memory_profile import (
    ChunkMemoryProfile,
    MemoryProfiler,
    estimate_chunk_memory,
    profile_chunk_memory,
    suggest_chunk_size,
)


class TestChunkMemoryProfile:
    """Tests for ChunkMemoryProfile class."""

    def test_record_chunk(self):
        """Test recording chunk memory usage."""
        profile = ChunkMemoryProfile("test")
        data = np.arange(1000, dtype=np.float64)

        profile.record_chunk("chunk_0", data)

        assert "chunk_0" in profile.chunk_sizes
        assert profile.chunk_sizes["chunk_0"] == data.nbytes
        assert profile.peak_memory == data.nbytes

    def test_record_multiple_chunks(self):
        """Test recording multiple chunks."""
        profile = ChunkMemoryProfile("test")
        chunks = [
            np.arange(100, dtype=np.float32),
            np.arange(200, dtype=np.float32),
            np.arange(50, dtype=np.float32),
        ]

        for i, chunk in enumerate(chunks):
            profile.record_chunk(f"chunk_{i}", chunk)

        assert len(profile.chunk_sizes) == 3
        assert profile.peak_memory == chunks[1].nbytes

    def test_get_stats(self):
        """Test getting profile statistics."""
        profile = ChunkMemoryProfile("test")

        # Empty profile
        stats = profile.get_stats()
        assert stats["n_chunks"] == 0
        assert stats["total_bytes"] == 0

        # Add some chunks
        for i in range(3):
            data = np.arange(100 * (i + 1), dtype=np.float64)
            profile.record_chunk(f"chunk_{i}", data)

        stats = profile.get_stats()
        assert stats["n_chunks"] == 3
        assert stats["max_bytes"] > stats["min_bytes"]
        assert stats["mean_bytes"] > 0

    def test_memory_safety_check(self):
        """Test memory safety checks."""
        profile = ChunkMemoryProfile("test", warn_threshold=0.8)
        available_mem = 1000  # bytes

        # Safe chunk
        profile.record_chunk("safe", np.zeros(10, dtype=np.float64))
        is_safe, msg = profile.check_memory_safety(available_mem)
        assert is_safe
        assert msg is None

        # Unsafe chunk (exceeds threshold)
        profile2 = ChunkMemoryProfile("test2", warn_threshold=0.5)
        profile2.record_chunk("unsafe", np.zeros(100, dtype=np.float64))
        is_safe, msg = profile2.check_memory_safety(available_mem)
        assert not is_safe
        assert msg is not None
        assert "exceeds" in msg

    def test_track_peak_memory(self):
        """Test peak memory tracking."""
        profile = ChunkMemoryProfile("test", track_peak=True)

        chunks = [
            np.zeros(100, dtype=np.float64),
            np.zeros(200, dtype=np.float64),
            np.zeros(50, dtype=np.float64),
        ]

        for i, chunk in enumerate(chunks):
            profile.record_chunk(f"chunk_{i}", chunk)

        expected_peak = chunks[1].nbytes
        assert profile.peak_memory == expected_peak

    def test_execution_time_tracking(self):
        """Test execution time tracking."""
        profile = ChunkMemoryProfile("test")
        data = np.arange(100, dtype=np.float64)

        profile.record_chunk("chunk_0", data, duration=0.5)
        profile.record_chunk("chunk_1", data, duration=1.0)

        assert profile.execution_times["chunk_0"] == 0.5
        assert profile.execution_times["chunk_1"] == 1.0

        stats = profile.get_stats()
        assert stats["mean_duration_s"] == 0.75


class TestMemoryProfiler:
    """Tests for global MemoryProfiler."""

    def teardown_method(self):
        """Clear profiles after each test."""
        MemoryProfiler.clear()
        MemoryProfiler.disable()

    def test_singleton_pattern(self):
        """Test that MemoryProfiler is a singleton."""
        profiler1 = MemoryProfiler()
        profiler2 = MemoryProfiler()
        assert profiler1 is profiler2

    def test_enable_disable(self):
        """Test enabling and disabling profiling."""
        profiler = MemoryProfiler()

        assert not profiler.is_enabled()
        profiler.enable()
        assert profiler.is_enabled()
        profiler.disable()
        assert not profiler.is_enabled()

    def test_register_profile(self):
        """Test registering profiles."""
        profiler = MemoryProfiler()
        profile = ChunkMemoryProfile("test_profile")

        profiler.register("test", profile)
        retrieved = profiler.get_profile("test")
        assert retrieved is profile

    def test_get_report(self):
        """Test getting profiling report."""
        profiler = MemoryProfiler()
        profiler.enable()

        profile = ChunkMemoryProfile("test")
        profile.record_chunk("chunk_0", np.arange(100, dtype=np.float64))
        profiler.register("test", profile)

        report = profiler.get_report()
        assert report["enabled"]
        assert "test" in report["profiles"]
        assert report["profiles"]["test"]["stats"]["n_chunks"] == 1


class TestProfileChunkMemoryDecorator:
    """Tests for @profile_chunk_memory decorator."""

    def teardown_method(self):
        """Clean up after each test."""
        MemoryProfiler.clear()
        MemoryProfiler.disable()

    def test_decorator_basic(self):
        """Test basic decorator functionality."""
        profiler = MemoryProfiler()
        profiler.enable()

        @profile_chunk_memory()
        def process_chunk(x):
            return x * 2

        result = process_chunk(np.arange(100, dtype=np.float64))
        assert result.shape == (100,)
        assert hasattr(process_chunk, "_memory_profile")

    def test_decorator_custom_name(self):
        """Test decorator with custom profile name."""
        profiler = MemoryProfiler()
        profiler.enable()

        @profile_chunk_memory(profile_name="my_processor")
        def process_chunk(x):
            return x * 2

        result = process_chunk(np.arange(100, dtype=np.float64))
        assert profiler.get_profile("my_processor") is not None

    def test_decorator_preserves_function(self):
        """Test that decorator preserves function properties."""

        @profile_chunk_memory()
        def my_function(x):
            """My docstring."""
            return x

        assert my_function.__name__ == "my_function"
        assert "My docstring" in my_function.__doc__


class TestEstimateChunkMemory:
    """Tests for estimate_chunk_memory function."""

    def test_basic_estimation(self):
        """Test basic memory estimation."""
        shape = (1000, 1000)
        chunks = (100, 100)
        dtype = np.float64

        result = estimate_chunk_memory(shape, chunks, dtype)

        expected_chunk_bytes = 100 * 100 * 8  # 8 bytes for float64
        assert result["chunk_bytes"] == expected_chunk_bytes
        assert result["overhead_bytes"] == int(expected_chunk_bytes * 0.5)
        assert (
            result["total_per_chunk_bytes"]
            == expected_chunk_bytes + result["overhead_bytes"]
        )

    def test_different_dtypes(self):
        """Test estimation with different dtypes."""
        shape = (1000, 1000)
        chunks = (100, 100)

        result_f64 = estimate_chunk_memory(shape, chunks, np.float64)
        result_f32 = estimate_chunk_memory(shape, chunks, np.float32)

        # float32 should be half the size of float64
        assert result_f32["chunk_bytes"] == result_f64["chunk_bytes"] / 2

    def test_custom_overhead(self):
        """Test estimation with custom overhead factor."""
        shape = (1000, 1000)
        chunks = (100, 100)
        dtype = np.float64

        result_1x = estimate_chunk_memory(shape, chunks, dtype, overhead_factor=1.0)
        result_2x = estimate_chunk_memory(shape, chunks, dtype, overhead_factor=2.0)

        assert result_1x["overhead_bytes"] == 0
        assert result_2x["overhead_bytes"] == result_2x["chunk_bytes"]


class TestSuggestChunkSize:
    """Tests for suggest_chunk_size function."""

    def test_basic_suggestion(self):
        """Test basic chunk size suggestion."""
        shape = (10000, 10000)
        dtype = np.float64
        available_memory = 100_000_000  # 100 MB

        suggestions = suggest_chunk_size(shape, dtype, available_memory)

        assert 0 in suggestions
        assert 1 in suggestions
        assert suggestions[0] > 0
        assert suggestions[1] > 0

    def test_respects_shape_bounds(self):
        """Test that suggestions don't exceed array shape."""
        shape = (100, 100)
        dtype = np.float64
        available_memory = 1_000_000_000  # 1 GB

        suggestions = suggest_chunk_size(shape, dtype, available_memory)

        assert suggestions[0] <= 100
        assert suggestions[1] <= 100

    def test_different_dimensions(self):
        """Test suggestions for different dimensionalities."""
        dtype = np.float64
        available_memory = 100_000_000

        # 2D
        suggestions_2d = suggest_chunk_size((1000, 1000), dtype, available_memory)
        assert len(suggestions_2d) == 2

        # 3D
        suggestions_3d = suggest_chunk_size((100, 100, 100), dtype, available_memory)
        assert len(suggestions_3d) == 3

    def test_safety_factor(self):
        """Test that safety factor reduces suggested chunk size."""
        shape = (10000, 10000)
        dtype = np.float64
        available_memory = 100_000_000

        suggestions_safe = suggest_chunk_size(
            shape, dtype, available_memory, safety_factor=0.5
        )
        suggestions_risky = suggest_chunk_size(
            shape, dtype, available_memory, safety_factor=0.9
        )

        # Safe suggestion should be smaller
        assert (
            suggestions_safe[0] * suggestions_safe[1]
            < suggestions_risky[0] * suggestions_risky[1]
        )

    def test_minimum_chunk_size(self):
        """Test that at least 1-element chunks are suggested."""
        shape = (10, 10)
        dtype = np.float64
        available_memory = 10000  # Enough for a few floats

        suggestions = suggest_chunk_size(shape, dtype, available_memory)

        assert all(v >= 1 for v in suggestions.values())

    def test_insufficient_memory_error(self):
        """Test error when available memory is too small."""
        shape = (10000, 10000)
        dtype = np.float64
        available_memory = 1  # 1 byte is impossibly small

        with pytest.raises(ValueError, match="too small"):
            suggest_chunk_size(shape, dtype, available_memory)


class TestIntegration:
    """Integration tests for memory profiling system."""

    def teardown_method(self):
        """Clean up after each test."""
        MemoryProfiler.clear()
        MemoryProfiler.disable()

    def test_end_to_end_profiling(self):
        """Test complete profiling workflow."""
        profiler = MemoryProfiler()
        profiler.enable()

        # Create and profile chunks
        profile = ChunkMemoryProfile("integration_test")
        for i in range(5):
            chunk = np.random.randn(100, 100).astype(np.float32)
            profile.record_chunk(f"chunk_{i}", chunk)

        profiler.register("integration_test", profile)

        # Get report
        report = profiler.get_report()
        assert report["enabled"]
        assert report["profiles"]["integration_test"]["stats"]["n_chunks"] == 5

    def test_memory_awareness_workflow(self):
        """Test memory-aware chunking workflow."""
        shape = (10000, 10000)
        dtype = np.float64
        available_memory = 50_000_000  # 50 MB

        # Get suggestion
        suggestions = suggest_chunk_size(shape, dtype, available_memory)

        # Estimate with suggested chunks
        chunk_size = math.prod(suggestions.values())
        estimation = estimate_chunk_memory(shape, tuple(suggestions.values()), dtype)

        # Verify it fits in memory
        safety_margin = 0.7
        assert estimation["total_per_chunk_bytes"] < available_memory * safety_margin
