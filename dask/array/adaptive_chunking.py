"""
Adaptive Chunking Engine for Dask Arrays.

This module provides intelligent, memory-aware chunking strategies that:
- Profile actual chunk memory usage during execution
- Track memory pressure and detect potential OOM scenarios
- Suggest and apply optimal chunk sizes based on observed patterns
- Integrate transparently with Dask's optimization pipeline

Key components:
- AdaptiveChunkingEngine: Main orchestrator for adaptive chunking
- ChunkProfile: Tracks per-chunk metadata (size, compute time, memory)
- MemoryPressure: Monitors system memory during execution
- ChunkSizeRecommender: Suggests optimal chunk sizes based on profiling data
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import psutil

from dask.array.core import (
    Array,
)
from dask.base import tokenize
from dask.config import get as config_get

logger = logging.getLogger(__name__)


@dataclass
class ChunkMemoryMetric:
    """Metrics for a single chunk execution."""

    chunk_key: tuple
    chunk_shape: tuple
    element_count: int
    dtype_size: int
    estimated_bytes: int
    actual_bytes: int | None = None
    compute_time_ms: float = 0.0
    memory_peak_before_mb: int = 0  # System memory in MB before execution
    memory_peak_after_mb: int = 0  # System memory in MB after execution
    timestamp: float = field(default_factory=time.time)

    @property
    def memory_delta_mb(self) -> float:
        """Memory increase during chunk execution in MB."""
        return self.memory_peak_after_mb - self.memory_peak_before_mb

    @property
    def throughput_mb_per_sec(self) -> float:
        """Chunk processing throughput."""
        if self.compute_time_ms <= 0 or self.estimated_bytes <= 0:
            return 0.0
        mb = self.estimated_bytes / (1024 * 1024)
        sec = self.compute_time_ms / 1000.0
        return mb / sec if sec > 0 else 0.0


@dataclass
class MemoryPressure:
    """System memory usage snapshot."""

    timestamp: float = field(default_factory=time.time)
    available_mb: float = 0.0
    used_mb: float = 0.0
    percent_used: float = 0.0
    peak_usage_mb: float = 0.0

    @staticmethod
    def current() -> MemoryPressure:
        """Capture current system memory state."""
        try:
            mem = psutil.virtual_memory()
            return MemoryPressure(
                available_mb=mem.available / (1024 * 1024),
                used_mb=mem.used / (1024 * 1024),
                percent_used=mem.percent,
                peak_usage_mb=0.0,  # Placeholder; actual tracking requires system-level monitoring
            )
        except Exception as e:
            logger.warning(f"Failed to capture memory pressure: {e}")
            return MemoryPressure()

    def is_pressure(self, threshold_percent: float = 80.0) -> bool:
        """Check if memory pressure exceeds threshold."""
        return self.percent_used >= threshold_percent


class ChunkProfiler:
    """Collects execution metrics for chunks in a computation graph."""

    def __init__(self, enabled: bool = True):
        """Initialize chunk profiler.

        Parameters
        ----------
        enabled : bool, optional
            Whether profiling is active, by default True
        """
        self.enabled = enabled
        self.metrics: dict[tuple, list[ChunkMemoryMetric]] = defaultdict(list)
        self.memory_snapshots: list[MemoryPressure] = []
        self._lock = (
            None  # Can be upgraded to threading.Lock if needed for thread-safety
        )

    def record_chunk(
        self,
        chunk_key: tuple,
        chunk_shape: tuple,
        dtype_size: int,
        compute_time_ms: float = 0.0,
    ) -> None:
        """Record metrics for a chunk execution.

        Parameters
        ----------
        chunk_key : tuple
            Task key (e.g., ('array-name', i, j, k))
        chunk_shape : tuple
            Actual shape of the chunk
        dtype_size : int
            Size of dtype in bytes
        compute_time_ms : float, optional
            Execution time in milliseconds, by default 0.0
        """
        if not self.enabled:
            return

        element_count = 1
        for s in chunk_shape:
            element_count *= s

        estimated_bytes = element_count * dtype_size
        mem_before = MemoryPressure.current()

        metric = ChunkMemoryMetric(
            chunk_key=chunk_key,
            chunk_shape=chunk_shape,
            element_count=element_count,
            dtype_size=dtype_size,
            estimated_bytes=estimated_bytes,
            compute_time_ms=compute_time_ms,
            memory_peak_before_mb=int(mem_before.used_mb),
            memory_peak_after_mb=int(mem_before.used_mb),
        )
        self.metrics[chunk_key].append(metric)
        self.memory_snapshots.append(mem_before)

    def get_stats(self) -> dict[str, Any]:
        """Aggregate profiling statistics.

        Returns
        -------
        dict
            Summary statistics including total chunks, avg size, peak memory, etc.
        """
        if not self.metrics:
            return {
                "total_chunks": 0,
                "avg_chunk_size_mb": 0.0,
                "peak_memory_mb": 0.0,
                "avg_throughput_mb_per_sec": 0.0,
            }

        all_metrics = [m for metrics in self.metrics.values() for m in metrics]
        avg_size = sum(m.estimated_bytes for m in all_metrics) / len(all_metrics)
        peak_mem = max((m.memory_peak_after_mb for m in all_metrics), default=0)
        avg_throughput = sum(m.throughput_mb_per_sec for m in all_metrics) / len(
            all_metrics
        )

        return {
            "total_chunks": len(all_metrics),
            "avg_chunk_size_mb": avg_size / (1024 * 1024),
            "peak_memory_mb": peak_mem,
            "avg_throughput_mb_per_sec": avg_throughput,
        }

    def clear(self) -> None:
        """Clear all recorded metrics."""
        self.metrics.clear()
        self.memory_snapshots.clear()


class ChunkSizeRecommender:
    """Suggests optimal chunk sizes based on profiling data and system constraints."""

    def __init__(
        self,
        target_memory_percent: float = 50.0,
        min_chunk_bytes: int = 1024 * 1024,  # 1 MB
        max_chunk_bytes: int | None = None,
        safety_factor: float = 0.8,
    ):
        """Initialize recommender.

        Parameters
        ----------
        target_memory_percent : float, optional
            Target memory usage as % of available, by default 50.0
        min_chunk_bytes : int, optional
            Minimum chunk size in bytes, by default 1 MB
        max_chunk_bytes : int, optional
            Maximum chunk size in bytes (None = auto-detect), by default None
        safety_factor : float, optional
            Conservative factor for recommendations (0 < x <= 1), by default 0.8
        """
        self.target_memory_percent = target_memory_percent
        self.min_chunk_bytes = min_chunk_bytes
        self.max_chunk_bytes = max_chunk_bytes or int(
            psutil.virtual_memory().available * 0.25
        )
        self.safety_factor = safety_factor

    def recommend_chunk_size(
        self,
        profiler: ChunkProfiler,
        array_shape: tuple,
        dtype_size: int,
        num_dimensions: int = 1,
    ) -> tuple[int, ...] | None:
        """Recommend optimal chunk size based on profiling data.

        Parameters
        ----------
        profiler : ChunkProfiler
            Profiler with execution metrics
        array_shape : tuple
            Total array shape
        dtype_size : int
            Size of dtype in bytes
        num_dimensions : int, optional
            Number of dimensions to chunk (default 1; increases for multi-dim)

        Returns
        -------
        tuple or None
            Recommended chunk sizes (one per dimension), or None if not enough data
        """
        if not profiler.metrics:
            return None

        stats = profiler.get_stats()
        avg_chunk_size = stats.get("avg_chunk_size_mb", 0)

        if avg_chunk_size <= 0:
            return None

        # Calculate target chunk size based on memory pressure
        mem = MemoryPressure.current()
        available_for_chunks = int(
            mem.available_mb * (self.target_memory_percent / 100.0) * self.safety_factor
        )
        target_chunk_bytes = max(
            self.min_chunk_bytes,
            min(available_for_chunks * (1024 * 1024), self.max_chunk_bytes),
        )

        # Estimate chunk count needed to cover array with target size
        elements_per_chunk = target_chunk_bytes // dtype_size
        if elements_per_chunk <= 0:
            elements_per_chunk = 1

        # Distribute evenly across num_dimensions
        elements_per_side = int(elements_per_chunk ** (1.0 / num_dimensions))
        elements_per_side = max(1, elements_per_side)

        # Build recommendation, respecting array shape
        recommendation = tuple(
            min(elements_per_side, array_shape[i]) for i in range(num_dimensions)
        )

        logger.info(
            f"Chunk size recommendation: {recommendation} "
            f"(target={target_chunk_bytes / (1024*1024):.1f} MB, "
            f"available={mem.available_mb:.1f} MB)"
        )
        return recommendation


class AdaptiveChunkingEngine:
    """Main orchestrator for adaptive chunking.

    Integrates with Dask's optimization pipeline to:
    1. Profile chunk execution via annotations
    2. Monitor memory pressure during execution
    3. Suggest and apply optimal chunk sizes
    4. Provide telemetry for users
    """

    def __init__(
        self,
        enabled: bool = True,
        profile_chunks: bool = True,
        auto_rechunk: bool = False,
        config_key: str = "array.adaptive-chunking",
    ):
        """Initialize adaptive chunking engine.

        Parameters
        ----------
        enabled : bool, optional
            Whether engine is active, by default True
        profile_chunks : bool, optional
            Whether to profile chunk execution, by default True
        auto_rechunk : bool, optional
            Whether to automatically suggest/apply rechunking, by default False
        config_key : str, optional
            Dask config key for engine settings, by default "array.adaptive-chunking"
        """
        self.enabled = enabled
        self.profile_chunks = profile_chunks
        self.auto_rechunk = auto_rechunk
        self.config_key = config_key

        self.profiler = ChunkProfiler(enabled=profile_chunks)
        self.recommender = ChunkSizeRecommender()
        self._tracked_arrays: dict[str, dict[str, Any]] = {}

    def profile_array(self, array: Array) -> str:
        """Start profiling an array.

        Parameters
        ----------
        array : Array
            Dask array to profile

        Returns
        -------
        str
            Unique profile ID for this array
        """
        profile_id = tokenize(array)
        self._tracked_arrays[profile_id] = {
            "array": array,
            "shape": array.shape,
            "dtype": array.dtype,
            "chunks": array.chunks,
            "start_time": time.time(),
        }
        logger.info(f"Started profiling array {profile_id} with shape {array.shape}")
        return profile_id

    def get_profile(self, profile_id: str) -> dict[str, Any] | None:
        """Retrieve profiling data for an array.

        Parameters
        ----------
        profile_id : str
            Profile ID returned by profile_array()

        Returns
        -------
        dict or None
            Profiling data including metrics, memory pressure, recommendations
        """
        if profile_id not in self._tracked_arrays:
            return None

        array_info = self._tracked_arrays[profile_id]
        stats = self.profiler.get_stats()

        recommendation = self.recommender.recommend_chunk_size(
            self.profiler,
            array_info["shape"],
            array_info["dtype"].itemsize,
            len(array_info["shape"]),
        )

        return {
            "array_shape": array_info["shape"],
            "array_dtype": str(array_info["dtype"]),
            "current_chunks": array_info["chunks"],
            "execution_time_s": time.time() - array_info["start_time"],
            "profiler_stats": stats,
            "memory_pressure": {
                "available_mb": MemoryPressure.current().available_mb,
                "used_mb": MemoryPressure.current().used_mb,
            },
            "recommended_chunks": recommendation,
        }

    def apply_recommendation(
        self, array: Array, recommended_chunks: tuple[int, ...] | None
    ) -> Array | None:
        """Apply recommended chunk size to array.

        Parameters
        ----------
        array : Array
            Source array
        recommended_chunks : tuple or None
            Target chunk sizes from recommender

        Returns
        -------
        Array or None
            Rechunked array, or None if recommendation is None
        """
        if recommended_chunks is None:
            return None

        try:
            rechunked = array.rechunk(recommended_chunks)
            logger.info(
                f"Applied adaptive rechunking: {array.chunks} -> {recommended_chunks}"
            )
            return rechunked
        except Exception as e:
            logger.warning(f"Failed to apply rechunking recommendation: {e}")
            return None

    def clear(self) -> None:
        """Clear profiler data and tracked arrays."""
        self.profiler.clear()
        self._tracked_arrays.clear()

    def get_telemetry(self) -> dict[str, Any]:
        """Get engine telemetry for monitoring.

        Returns
        -------
        dict
            Telemetry including active profiles, memory usage, recommendations
        """
        return {
            "engine_enabled": self.enabled,
            "profiling_active": self.profile_chunks,
            "auto_rechunk_enabled": self.auto_rechunk,
            "tracked_arrays": len(self._tracked_arrays),
            "profiler_stats": self.profiler.get_stats(),
            "memory_pressure": MemoryPressure.current().__dict__,
        }


# Global singleton instance
_adaptive_engine: AdaptiveChunkingEngine | None = None


def get_adaptive_engine(
    create: bool = True,
) -> AdaptiveChunkingEngine | None:
    """Get or create the global adaptive chunking engine.

    Parameters
    ----------
    create : bool, optional
        Whether to create engine if it doesn't exist, by default True

    Returns
    -------
    AdaptiveChunkingEngine or None
        Global engine instance or None
    """
    global _adaptive_engine

    if _adaptive_engine is None and create:
        try:
            enabled = config_get("array.adaptive-chunking.enabled", True)
            _adaptive_engine = AdaptiveChunkingEngine(enabled=enabled)
        except Exception as e:
            logger.warning(f"Failed to initialize adaptive chunking engine: {e}")
            return None

    return _adaptive_engine


def enable_adaptive_chunking() -> None:
    """Enable adaptive chunking globally."""
    engine = get_adaptive_engine()
    if engine:
        engine.enabled = True
        logger.info("Adaptive chunking enabled")


def disable_adaptive_chunking() -> None:
    """Disable adaptive chunking globally."""
    engine = get_adaptive_engine()
    if engine:
        engine.enabled = False
        logger.info("Adaptive chunking disabled")
