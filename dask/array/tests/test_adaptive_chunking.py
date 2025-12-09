"""
Tests for Adaptive Chunking Engine.

Tests cover:
- Chunk profiling and metrics collection
- Memory pressure detection
- Chunk size recommendations
- Engine orchestration
- Integration with Dask arrays
"""

import pytest

import dask.array as da
from dask.array.adaptive_chunking import (
    AdaptiveChunkingEngine,
    ChunkMemoryMetric,
    ChunkProfiler,
    ChunkSizeRecommender,
    MemoryPressure,
    disable_adaptive_chunking,
    enable_adaptive_chunking,
    get_adaptive_engine,
)


class TestChunkMemoryMetric:
    """Tests for ChunkMemoryMetric dataclass."""

    def test_creation(self):
        """Test creating a metric."""
        metric = ChunkMemoryMetric(
            chunk_key=("x", 0, 0),
            chunk_shape=(100, 100),
            element_count=10000,
            dtype_size=8,
            estimated_bytes=80000,
            compute_time_ms=50.0,
            memory_peak_before_mb=500,
            memory_peak_after_mb=1500,
        )

        assert metric.chunk_key == ("x", 0, 0)
        assert metric.element_count == 10000
        assert metric.memory_delta_mb == pytest.approx(1000.0)

    def test_throughput_calculation(self):
        """Test throughput calculation."""
        metric = ChunkMemoryMetric(
            chunk_key=("x", 0),
            chunk_shape=(1000,),
            element_count=1000,
            dtype_size=8,
            estimated_bytes=8000,  # ~7.6 KB
            compute_time_ms=1000.0,  # 1 second
        )

        # 8000 bytes = ~7.6 MB/s
        assert metric.throughput_mb_per_sec == pytest.approx(8000 / (1024 * 1024))

    def test_zero_compute_time(self):
        """Test throughput with zero compute time."""
        metric = ChunkMemoryMetric(
            chunk_key=("x", 0),
            chunk_shape=(100,),
            element_count=100,
            dtype_size=4,
            estimated_bytes=400,
            compute_time_ms=0.0,
        )

        assert metric.throughput_mb_per_sec == 0.0


class TestMemoryPressure:
    """Tests for MemoryPressure snapshot."""

    def test_current_snapshot(self):
        """Test capturing current memory snapshot."""
        mp = MemoryPressure.current()

        assert mp.available_mb > 0
        assert mp.used_mb >= 0
        assert 0 <= mp.percent_used <= 100

    def test_pressure_threshold(self):
        """Test pressure detection."""
        # Simulate low pressure
        mp_low = MemoryPressure(available_mb=1000, used_mb=100, percent_used=50.0)
        assert not mp_low.is_pressure(threshold_percent=80.0)

        # Simulate high pressure
        mp_high = MemoryPressure(available_mb=100, used_mb=400, percent_used=85.0)
        assert mp_high.is_pressure(threshold_percent=80.0)


class TestChunkProfiler:
    """Tests for ChunkProfiler."""

    def test_record_chunk(self):
        """Test recording a single chunk."""
        profiler = ChunkProfiler(enabled=True)
        profiler.record_chunk(
            chunk_key=("array-1", 0, 0),
            chunk_shape=(100, 100),
            dtype_size=8,
            compute_time_ms=10.0,
        )

        assert ("array-1", 0, 0) in profiler.metrics
        metric = profiler.metrics[("array-1", 0, 0)][0]
        assert metric.element_count == 10000
        assert metric.estimated_bytes == 80000

    def test_record_multiple_chunks(self):
        """Test recording multiple chunks."""
        profiler = ChunkProfiler(enabled=True)

        for i in range(5):
            profiler.record_chunk(
                chunk_key=("x", i),
                chunk_shape=(100,),
                dtype_size=4,
                compute_time_ms=5.0 * (i + 1),
            )

        stats = profiler.get_stats()
        assert stats["total_chunks"] == 5

    def test_disabled_profiler(self):
        """Test that disabled profiler doesn't record."""
        profiler = ChunkProfiler(enabled=False)
        profiler.record_chunk(
            chunk_key=("x", 0),
            chunk_shape=(100,),
            dtype_size=8,
        )

        assert len(profiler.metrics) == 0
        stats = profiler.get_stats()
        assert stats["total_chunks"] == 0

    def test_get_stats(self):
        """Test aggregation of profiling statistics."""
        profiler = ChunkProfiler(enabled=True)

        # Add diverse chunks
        sizes = [100, 200, 150]
        for i, size in enumerate(sizes):
            profiler.record_chunk(
                chunk_key=("x", i),
                chunk_shape=(size,),
                dtype_size=8,
                compute_time_ms=10.0,
            )

        stats = profiler.get_stats()
        assert stats["total_chunks"] == 3
        assert stats["avg_chunk_size_mb"] > 0
        assert stats["avg_throughput_mb_per_sec"] > 0

    def test_clear(self):
        """Test clearing profiler data."""
        profiler = ChunkProfiler(enabled=True)
        profiler.record_chunk(
            chunk_key=("x", 0),
            chunk_shape=(100,),
            dtype_size=8,
        )

        assert len(profiler.metrics) > 0
        profiler.clear()
        assert len(profiler.metrics) == 0


class TestChunkSizeRecommender:
    """Tests for ChunkSizeRecommender."""

    def test_basic_recommendation(self):
        """Test generating a recommendation."""
        recommender = ChunkSizeRecommender(
            target_memory_percent=50.0,
            min_chunk_bytes=1024 * 1024,
            safety_factor=0.8,
        )

        profiler = ChunkProfiler(enabled=True)
        # Record a few chunks to establish baseline
        for i in range(3):
            profiler.record_chunk(
                chunk_key=("x", i),
                chunk_shape=(100, 100),
                dtype_size=8,
            )

        recommendation = recommender.recommend_chunk_size(
            profiler,
            array_shape=(10000, 10000),
            dtype_size=8,
            num_dimensions=2,
        )

        assert recommendation is not None
        assert len(recommendation) == 2
        assert all(r > 0 for r in recommendation)

    def test_recommendation_respects_shape(self):
        """Test that recommendation respects array bounds."""
        recommender = ChunkSizeRecommender()

        profiler = ChunkProfiler(enabled=True)
        for i in range(2):
            profiler.record_chunk(
                chunk_key=("x", i),
                chunk_shape=(10, 10),
                dtype_size=8,
            )

        recommendation = recommender.recommend_chunk_size(
            profiler,
            array_shape=(100, 100),
            dtype_size=8,
            num_dimensions=2,
        )

        assert recommendation is not None
        assert recommendation[0] <= 100
        assert recommendation[1] <= 100

    def test_no_data_returns_none(self):
        """Test that empty profiler returns None."""
        recommender = ChunkSizeRecommender()
        profiler = ChunkProfiler(enabled=True)

        recommendation = recommender.recommend_chunk_size(
            profiler,
            array_shape=(1000, 1000),
            dtype_size=8,
        )

        assert recommendation is None


class TestAdaptiveChunkingEngine:
    """Tests for AdaptiveChunkingEngine orchestrator."""

    def test_engine_creation(self):
        """Test creating an engine."""
        engine = AdaptiveChunkingEngine(enabled=True, profile_chunks=True)

        assert engine.enabled
        assert engine.profile_chunks
        assert engine.profiler is not None
        assert engine.recommender is not None

    def test_profile_array(self):
        """Test starting array profiling."""
        engine = AdaptiveChunkingEngine()
        array = da.ones((1000, 1000), chunks=(100, 100))

        profile_id = engine.profile_array(array)

        assert profile_id in engine._tracked_arrays
        assert engine._tracked_arrays[profile_id]["shape"] == (1000, 1000)

    def test_get_profile(self):
        """Test retrieving profiling data."""
        engine = AdaptiveChunkingEngine()
        array = da.ones((1000, 1000), chunks=(100, 100))

        profile_id = engine.profile_array(array)

        # Add some metrics to profiler
        engine.profiler.record_chunk(
            chunk_key=("x", 0, 0),
            chunk_shape=(100, 100),
            dtype_size=8,
        )

        profile = engine.get_profile(profile_id)

        assert profile is not None
        assert profile["array_shape"] == (1000, 1000)
        assert "profiler_stats" in profile
        assert "recommended_chunks" in profile

    def test_apply_recommendation(self):
        """Test applying chunk size recommendation."""
        engine = AdaptiveChunkingEngine()
        array = da.ones((1000, 1000), chunks=(100, 100))

        rechunked = engine.apply_recommendation(array, recommended_chunks=(200, 200))

        assert rechunked is not None
        # Check that rechunking was applied (first chunk should be larger)
        assert rechunked.chunks[0][0] >= 100

    def test_apply_none_recommendation(self):
        """Test applying None recommendation."""
        engine = AdaptiveChunkingEngine()
        array = da.ones((1000, 1000), chunks=(100, 100))

        result = engine.apply_recommendation(array, recommended_chunks=None)

        assert result is None

    def test_clear_engine(self):
        """Test clearing engine state."""
        engine = AdaptiveChunkingEngine()
        array = da.ones((100, 100), chunks=(10, 10))

        engine.profile_array(array)
        engine.profiler.record_chunk(
            chunk_key=("x", 0),
            chunk_shape=(10,),
            dtype_size=8,
        )

        assert len(engine._tracked_arrays) > 0
        assert len(engine.profiler.metrics) > 0

        engine.clear()

        assert len(engine._tracked_arrays) == 0
        assert len(engine.profiler.metrics) == 0

    def test_get_telemetry(self):
        """Test retrieving engine telemetry."""
        engine = AdaptiveChunkingEngine(enabled=True, profile_chunks=True)
        array = da.ones((500, 500), chunks=(50, 50))

        engine.profile_array(array)
        engine.profiler.record_chunk(
            chunk_key=("x", 0, 0),
            chunk_shape=(50, 50),
            dtype_size=8,
        )

        telemetry = engine.get_telemetry()

        assert telemetry["engine_enabled"]
        assert telemetry["profiling_active"]
        assert telemetry["tracked_arrays"] == 1
        assert "profiler_stats" in telemetry
        assert "memory_pressure" in telemetry


class TestGlobalEngineInterface:
    """Tests for global engine singleton."""

    def test_get_engine(self):
        """Test getting global engine."""
        engine1 = get_adaptive_engine(create=True)
        engine2 = get_adaptive_engine(create=True)

        assert engine1 is engine2

    def test_enable_disable(self):
        """Test global enable/disable."""
        engine = get_adaptive_engine(create=True)

        enable_adaptive_chunking()
        assert engine.enabled

        disable_adaptive_chunking()
        assert not engine.enabled

        enable_adaptive_chunking()
        assert engine.enabled


class TestIntegrationWithDaskArray:
    """Integration tests with Dask arrays."""

    def test_end_to_end_profiling(self):
        """Test complete profiling workflow with real array."""
        engine = AdaptiveChunkingEngine(enabled=True)

        # Create a realistic array
        array = da.random.random((5000, 5000), chunks=(100, 100))
        profile_id = engine.profile_array(array)

        # Simulate chunk execution
        for i in range(10):
            engine.profiler.record_chunk(
                chunk_key=("x", i // 5, i % 5),
                chunk_shape=(100, 100),
                dtype_size=8,
                compute_time_ms=50.0,
            )

        profile = engine.get_profile(profile_id)
        assert profile is not None
        assert profile["profiler_stats"]["total_chunks"] == 10

    def test_memory_aware_chunking_flow(self):
        """Test memory-aware chunking workflow."""
        engine = AdaptiveChunkingEngine(profile_chunks=True)

        # Create array
        array = da.ones((10000, 10000), chunks=(100, 100))
        profile_id = engine.profile_array(array)

        # Collect profiling data
        for i in range(5):
            engine.profiler.record_chunk(
                chunk_key=("x", i),
                chunk_shape=(100, 100),
                dtype_size=8,
            )

        # Get recommendation
        profile = engine.get_profile(profile_id)
        recommendation = profile.get("recommended_chunks")

        # Apply if available
        if recommendation:
            rechunked = engine.apply_recommendation(array, recommendation)
            assert rechunked is not None

        engine.clear()
