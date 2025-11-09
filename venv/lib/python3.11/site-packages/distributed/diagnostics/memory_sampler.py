from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any

from distributed.compatibility import PeriodicCallback

if TYPE_CHECKING:
    # Optional runtime dependencies
    import pandas as pd

    # Circular dependencies
    from distributed.client import Client
    from distributed.scheduler import Scheduler


class MemorySampler:
    """Sample cluster-wide memory usage every <interval> seconds.

    **Usage**

    .. code-block:: python

       client = Client()
       ms = MemorySampler()

       with ms.sample("run 1"):
           <run first workflow>
       with ms.sample("run 2"):
           <run second workflow>
       ...
       ms.plot()

    or with an asynchronous client:

    .. code-block:: python

       client = await Client(asynchronous=True)
       ms = MemorySampler()

       async with ms.sample("run 1"):
           <run first workflow>
       async with ms.sample("run 2"):
           <run second workflow>
       ...
       ms.plot()
    """

    samples: dict[str, list[tuple[float, int]]]

    def __init__(self):
        self.samples = {}

    def sample(
        self,
        label: str | None = None,
        *,
        client: Client | None = None,
        measure: str = "process",
        interval: float = 0.5,
    ) -> Any:
        """Context manager that records memory usage in the cluster.
        This is synchronous if the client is synchronous and
        asynchronous if the client is asynchronous.

        The samples are recorded in ``self.samples[<label>]``.

        Parameters
        ==========
        label: str, optional
            Tag to record the samples under in the self.samples dict.
            Default: automatically generate a random label
        client: Client, optional
            client used to connect to the scheduler.
            Default: use the global client
        measure: str, optional
            One of the measures from :class:`distributed.scheduler.MemoryState`.
            Default: sample process memory
        interval: float, optional
            sampling interval, in seconds.
            Default: 0.5
        """
        if not client:
            from distributed.client import get_client

            client = get_client()

        if client.asynchronous:
            return self._sample_async(label, client, measure, interval)
        else:
            return self._sample_sync(label, client, measure, interval)

    @contextmanager
    def _sample_sync(
        self, label: str | None, client: Client, measure: str, interval: float
    ) -> Iterator[None]:
        key = client.sync(
            client.scheduler.memory_sampler_start,
            client=client.id,
            measure=measure,
            interval=interval,
        )
        try:
            yield
        finally:
            samples = client.sync(client.scheduler.memory_sampler_stop, key=key)
            self.samples[label or key] = samples

    @asynccontextmanager
    async def _sample_async(
        self, label: str | None, client: Client, measure: str, interval: float
    ) -> AsyncIterator[None]:
        key = await client.scheduler.memory_sampler_start(
            client=client.id, measure=measure, interval=interval
        )
        try:
            yield
        finally:
            samples = await client.scheduler.memory_sampler_stop(key=key)
            self.samples[label or key] = samples

    def to_pandas(self, *, align: bool = False) -> pd.DataFrame:
        """Return the data series as a pandas.Dataframe.

        Parameters
        ==========
        align : bool, optional
            If True, change the absolute timestamps into time deltas from the first
            sample of each series, so that different series can be visualized side by
            side. If False (the default), use absolute timestamps.
        """
        import pandas as pd

        ss = {}
        for label, s_list in self.samples.items():
            assert s_list  # There's always at least one sample
            s = pd.DataFrame(s_list).set_index(0)[1]
            s.index = pd.to_datetime(s.index, unit="s")
            s.name = label
            if align:
                # convert datetime to timedelta from the first sample
                s.index -= s.index[0]
            ss[label] = s[~s.index.duplicated()]  # type: ignore[attr-defined]

        df = pd.DataFrame(ss)

        if len(ss) > 1:
            # Forward-fill NaNs in the middle of a series created either by overlapping
            # sampling time range or by align=True. Do not ffill series beyond their
            # last sample.
            df = df.ffill().where(~pd.isna(df.bfill()))

        return df

    def plot(self, *, align: bool = False, **kwargs: Any) -> Any:
        """Plot data series collected so far

        Parameters
        ==========
        align : bool (optional)
            See :meth:`~distributed.diagnostics.MemorySampler.to_pandas`
        kwargs
            Passed verbatim to :meth:`pandas.DataFrame.plot`

        Returns
        =======
        Output of :meth:`pandas.DataFrame.plot`
        """
        df = self.to_pandas(align=align)
        resampled = df.resample("1s").nearest() / 2**30
        # If resampling collapses data onto one point, we'll run into
        # https://stackoverflow.com/questions/58322744/matplotlib-userwarning-attempting-to-set-identical-left-right-737342-0
        # This should only happen in tests since users typically sample for more
        # than a second
        if len(resampled) == 1:
            resampled = df.resample("1ms").nearest() / 2**30

        return resampled.plot(
            xlabel="time",
            ylabel="Cluster memory (GiB)",
            **kwargs,
        )


class MemorySamplerExtension:
    """Scheduler extension - server side of MemorySampler"""

    scheduler: Scheduler
    samples: dict[str, list[tuple[float, int]]]

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers["memory_sampler_start"] = self.start
        self.scheduler.handlers["memory_sampler_stop"] = self.stop
        self.samples = {}

    def start(self, client: str, measure: str, interval: float) -> str:
        """Start periodically sampling memory"""
        assert not measure.startswith("_")
        assert isinstance(getattr(self.scheduler.memory, measure), int)

        key = str(uuid.uuid4())
        self.samples[key] = []

        def sample():
            if client in self.scheduler.clients:
                ts = datetime.now().timestamp()
                nbytes = getattr(self.scheduler.memory, measure)
                self.samples[key].append((ts, nbytes))
            else:
                self.stop(key)

        pc = PeriodicCallback(sample, interval * 1000)
        self.scheduler.periodic_callbacks["MemorySampler-" + key] = pc
        pc.start()

        # Immediately collect the first sample; this also ensures there's always at
        # least one sample
        sample()

        return key

    def stop(self, key: str) -> list[tuple[float, int]]:
        """Stop sampling and return the samples"""
        pc = self.scheduler.periodic_callbacks.pop("MemorySampler-" + key, None)
        if pc is not None:  # Race condition with scheduler shutdown
            pc.stop()
        return self.samples.pop(key)
