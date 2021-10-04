from __future__ import annotations

import asyncio
import concurrent.futures
import itertools
import operator
import os
import struct
import threading
import typing
import weakref
from collections import defaultdict

import msgpack
import pandas as pd

from distributed import Worker, get_client, get_worker
from distributed.protocol import deserialize, serialize, to_serialize
from distributed.utils import log_errors, nbytes, sync

from dask.utils import format_bytes

from .core import _concat

### Task functions
##################


def transfer(partition: pd.DataFrame, service: ShuffleService) -> None:
    """Split and send a pandas dataframe to the shuffle service

    This is a task in the task graph

    Parameters
    ----------
    partition: pd.DataFrame
    service: ShuffleService
        The thing we use to do the splits
    """
    service.start()  # idempotent
    service.add_partition(partition)
    service.maybe_send()


def unpack(service: ShuffleService, i: int, barrier_token=None) -> pd.DataFrame:
    """
    Pick up the nicely shuffled output partitions

    This is a task in the task graph
    """
    return service.get(i)


### ShuffleService
##################


class ShuffleReceiver:
    """A small worker extension to receive shuffle results"""

    def __init__(self, dask_worker: Worker, rows_per_collect: int):
        self.worker = dask_worker
        self.worker.handlers["shuffle_receive"] = self.receive
        self.worker.shuffler = self
        self.lock = threading.Lock()
        self.collect_rows = rows_per_collect
        self.ready_to_collect = threading.Event()

        self.gathered_shards: list[pd.DataFrame] = []
        self.gathered_rows = 0

    def receive(self, comm, data=None):
        with self.lock:
            self.gathered_rows += len(data)
            self.gathered_shards.append(data)
            if self.gathered_rows >= self.collect_rows:
                self.ready_to_collect.set()

    def collect(self, flush: bool = False) -> list[pd.DataFrame]:
        with self.lock:
            if not flush and self.gathered_rows < self.collect_rows:
                return []

            self.ready_to_collect.clear()
            shards = self.gathered_shards
            self.gathered_shards = []
            self.gathered_rows = 0
            return shards

    def uninstall(self) -> None:
        assert not self.gathered_rows
        assert not self.gathered_shards
        self.worker.handlers.pop("shuffle_receive")


def install_receiver(rows_per_collect: int, dask_worker: Worker):
    """
    This installs the ShuffleReceiver on the worker

    It gets called with client.run
    """
    ShuffleReceiver(dask_worker, rows_per_collect)


class ShuffleService:
    """Shuffle a Dask dataframe between workers with direct worker-to-worker transfers.

    A ShuffleService instance gets copied to every worker in the cluster
    to hold intermediate results and manage the coroutines and threads sending
    and receiving data.

    The ShuffleService gets its input data from dask tasks in a thread pool, and needs
    to do blocking pandas and disk operations in threads, but also uses async comms
    on the worker's event loop. Managing these different forms of concurrency is complicated.

    At startup, the ShuffleService launches 4 async coroutines on the worker's event loop,
    which wait for outgoing data and send it over comms to peer workers.

    It also steals half of the threads from the worker's thread pool to handle incoming data
    from comms and write it to disk.
    It's essential that the `transfer` tasks (at the dask level) can block when necessary,
    otherwise the worker would keep running input tasks and run out of memory. However,
    receiving shuffled data from other workers and writing it to disk must never stop,
    otherwise new data from the network would pile up in memory. That's why sending and
    receiving run in separate threads like this.

    Attributes
    ----------
    column
        The name of an integer column in each partition, which contains the output
        partition number that row belongs to. Always ``"_partitions"`` during normal
        use from `shuffle` or `set_index`.
    n_partitions
        Total number of input/output partitions for the shuffle
    empty
        Empty DataFrame to return when an output partition has no contents.
        (This happens if, say, there is 1 unique value in the whole DataFrame
        but 5 output partitions are requested.)
    row_size
        The approximate size, in bytes, of a single row of data. Used for estimating how
        much memory is used by various buffers in the shuffling process. Because of object
        dtypes (strings), this may be inaccurate.
    workers:
        List of addresses of all the other workers in the cluster.
    shards
        For each peer worker, a list of DataFrames ("shards") waiting to be sent to it.
        Each shard contains only rows which should end up on that worker, though they
        may belong to different final output partitions. Keys are indices into the
        `workers` list.

        Once enough shards have accumulated for a single worker (currently 5MB),
        all of those shards are concatenated and sent to that worker.

        Concurrent access must be protected by `lock_shards`.
    shards_lengths
        Parallel mapping to `shards` tracking the total number of rows accumulated per worker.

        Concurrent access must be protected by `lock_shards`.
    q
        Asyncio queue of concatenated shards waiting to be sent over comms.
        This acts as the bridge between the threads (which do blocking CPU work on DataFrames)
        and coroutines which send over async comms. Messages are added in `maybe_send`,
        and popped off by the `watch_and_send` coroutines.
    send_capacity
        Semaphore bounding the capacity of `q`. Rather than using a bounded queue,
        the semaphore lets us reserve capacity before doing the memory-copying operations
        to make the thing we want to put in the queue, and wait to release capacity until
        after the message has been fully sent (and the item has already been popped from the queue).
        This helps prevent memory spikes.
    error
        An exception that occurred in one of the `watch_and_send` coroutines or
        `process_received_shards` threads. The next `transfer` task will re-raise
        this error, letting dask see that the task has failed, and getting the error to users.
    done_receiving
        When set, tells the `process_received_shards` threads to stop.
    files
        Cache of open file objects (up to `num_fds` items). Keys are output partition numbers,
        which are also the file paths.
        All rows written to each file belong to the same final output partition. Data is written
        in a similar format to distributed's wire protocol, with a msgpack-serialized header
        followed by serialized frames. Multiple of these partial result bundles are appended to
        a file; at the end in `get`, they're all read and concatenated to produce a final output partition.

        Each file has a moderately large buffer (`file_buffer_size`, currently 2MB) to improve disk
        performance, since each write to the file is typically small.

        Concurrent access to the `files` dict itself must be protected by `files_dicts_lock`.
        Concurrent access to the file must be protected by the corresponding lock in `file_locks`.
    file_locks
        Lock for each file *path*. Note that the length of `file_locks` is unbounded and may be much larger
        than `files`. All operations on this path (open, close, write) must be protected by the lock.

        Concurrent access to the `file_locks` dict itself must be protected by `files_dicts_lock`.
    See Also
    --------
    dask.dataframe.shuffle.rearrange_by_column_service
    """

    _instances: weakref.WeakSet[ShuffleService] = weakref.WeakSet()

    def __init__(
        self, column: str, n_partitions: int, empty: pd.DataFrame, row_size: int
    ):
        self._init()

        self.column = column
        self.n_partitions = n_partitions
        assert len(empty) == 0, f"ShuffleService empty DataFrame has {len(empty)} rows"
        self.empty = empty
        self.row_size = row_size
        self.key = self.worker.get_current_task()

        # The shuffle task graph ensures exactly one ShuffleService instance is created at first (us).
        # This `client.run` installs the receiver on all the other workers (so they can start receiving before
        # a `ShuffleService` instance has been transferred to them) and gets all the worker addresses.
        # It's important we only get peer addresses once (instead of each worker repeating it) to avoid race conditions
        # if new workers are joining while the ShuffleServices are starting up on each worker.
        self.workers: list[str] = list(
            get_client().run(install_receiver, 200_000_000 / self.row_size)
        )

    def _init(self):
        "Initialize all the empty instance variables; shared by ``__init__`` and ``__setstate__``"
        self._started = False
        self.worker: Worker = get_worker()

        self.shards: defaultdict[int, list[pd.DataFrame]] = defaultdict(list)
        self.shards_length: int = 0
        self.shards_lengths: defaultdict[int, int] = defaultdict(int)
        self.lock_shards = threading.Lock()

        self.error: Exception | None = None
        self.done_receiving: bool = False

        self.files: dict[int, typing.BinaryIO] = {}
        self.file_locks: dict[int, threading.Lock] = {}
        self.files_dicts_lock = threading.Lock()
        # TODO set # of open files (and file buffer size) dynamically based on memory targets and num partitions?
        # This currently lets us keep 100MiB of open files
        self.num_fds = 50
        self.file_buffer_size = 2 * 1024 ** 2  # 2 MB

        self.lock_final_gather = threading.Lock()

        # Counters tracking memory use, etc.---not used for actual logic
        self.file_amount_written: dict[int, list[int]] = {}
        self.send_length: int = 0
        self.disk_write_bytes: int = 0
        self.file_buffers_bytes: int = 0
        self.write_sizes = []
        self.write_amounts_at_close = []
        self.subpart_counts = []
        self.subpart_sizes = []
        self.file_cache_hits = 0
        self.file_cache_misses = 0

        ShuffleService._instances.add(self)

    def __getstate__(self):
        return {
            "column": self.column,
            "n_partitions": self.n_partitions,
            "empty": self.empty,
            "key": self.key,
            "workers": self.workers,
            "row_size": self.row_size,
        }

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init()

    def start(self):
        if self._started:
            return
        return sync(self.worker.loop, self._start)

    async def _start(self):
        if self._started:
            return
        else:
            self.worker.handlers["shuffle_finish_send"] = self.finish_send

            self.q: asyncio.Queue[dict] = asyncio.Queue()
            # ^ ideally this would happen in `_init`, but since the `loop=` parameter is deprecated,
            # we wait until the coroutine to ensure it's connected to the correct event loop.
            self.send_coroutines = [
                asyncio.ensure_future(self.watch_and_send())
                for _ in range(4)  # TODO how many?
            ]
            self.send_capacity = threading.Semaphore(len(self.send_coroutines))

            # Borrow half the worker's threads for processing received shards
            # TODO create an API for this in distributed
            executor: concurrent.futures.Executor = self.worker.executors["default"]
            nthreads = self.worker.nthreads
            # TODO launch our own thread(s) if nthreads == 1
            assert nthreads is not None and nthreads >= 2, (
                f"At least 2 threads per worker required for shuffling, "
                f"not {nthreads}"
            )
            assert "ThreadPoolExecutor" in str(type(executor)), (
                f"Shuffle service requires ThreadPoolExecutor as "
                f"default worker executor, not {type(executor)}"
            )

            self.retrieve_futures = [
                executor.submit(self.process_received_shards)
                for _ in range(nthreads // 2)
            ]

            # Add the futures as placeholders to the worker's executing set
            # so it doesn't over-produce input tasks
            if not hasattr(self.worker, "_executing"):
                # TODO remove once worker state machine refactor is released,
                # and check distributed version here + in client code.
                raise RuntimeError(
                    "`shuffle='service'` requires the latest, unreleased version of distributed installed from git. "
                    "Run `pip install -U git+https://github.com/dask/distributed` to install."
                )
            for future in self.retrieve_futures:
                self.worker._executing.add(future)
                future.add_done_callback(
                    lambda f: self.worker.loop.add_callback(
                        self.worker._executing.discard, f
                    )
                )

            # self.report_size_coroutine = asyncio.ensure_future(self.report_size())
            self._started = True

    @typing.overload
    def worker_holding(self, partition_number: pd.Series) -> pd.Series:
        ...

    @typing.overload
    def worker_holding(self, partition_number: int) -> float:
        ...

    def worker_holding(self, partition_number):
        """
        Which worker index should hold this partition?

        This method intentionally works with both scalars and Pandas series.
        Because of this you should probably convert the result to integer type
        """
        return partition_number * len(self.workers) / self.n_partitions

    def add_partition(self, partition: pd.DataFrame) -> None:
        """
        Insert a new input partition to transfer into the service

        We split the rows of the input DataFrame based on the worker where they
        will end up. This combines multiple output partitions into the same group,
        but results in fewer, larger partitions to transfer.

        Note that ``partition[self.column]`` must be an integer Series
        of the partition number of each row.
        """
        grouper = self.worker_holding(partition[self.column]).astype(
            partition[self.column].dtype
        )

        # Split into groups up front to reduce contention on the lock
        groups = list(partition.groupby(grouper))
        del grouper, partition
        with self.lock_shards:
            for worker, group in groups:
                assert len(group)
                self.shards_length += len(group)
                self.shards_lengths[worker] += len(group)
                self.shards[worker].append(group)

    def maybe_send(self):
        """
        Maybe pack up and send some shards to a peer

        We find the largest bundle of data to send, concat that data, and put it
        on the queue to be sent out to a worker. We continue doing this until
        no worker has enough shards accumulated to be worth sending (and we have
        few enough shards total that we can afford to let more pile up).

        This method gets called by the `transfer` tasks during a shuffle, so
        this code all gets run in the ThreadPoolExecutor as part of a normal
        task.

        Each concat + queue operation acquires capacity on the `send_capacity` semaphore,
        which is later released in `watch_and_send` after the message has been sent.
        Because `maybe_send` is called within tasks, this applies network backpressure
        up to dask tasks, by blocking in `maybe_send` until a previous send has completed.
        """
        while True:
            if self.error:
                self.stop()
                raise self.error

            if not self.send_capacity.acquire(timeout=2):
                continue

            with self.lock_shards:
                # Pick largest bundle
                worker_i, length = max(
                    self.shards_lengths.items(),
                    key=operator.itemgetter(1),
                    default=(None, 0),
                )
                if worker_i is None or (
                    # largest shard isn't a good sending size
                    length * self.row_size < 5e6  # 5MB
                    # enough total shards we can afford to accumulate more
                    and self.shards_length * self.row_size < 1e8  # 100MB
                ):
                    self.send_capacity.release()
                    return
                part = self.shards.pop(worker_i)
                self.shards_lengths.pop(worker_i)
                self.shards_length -= length

            # Concat and send that bundle along to be sent to the correct worker
            part = _concat(part)
            self.worker.loop.add_callback(
                self.q.put_nowait,
                {
                    "op": "send",
                    "worker": self.workers[worker_i],
                    "part": to_serialize(part),
                    "len": len(part),
                },
            )

    async def watch_and_send(self):
        """
        Pull data from ``self.q`` and send it to a peer worker.

        Once the message has sent, a spot on ``self.send_capacity`` is
        released, allowing a blocked `maybe_send` to enqueue another message.

        We run a few of these concurrently so that we send to multiple
        workers in parallel. However, we don't want a ton of these
        running, because then we'll use too much memory, plus they'll
        split up bandwidth too finely. At the time of writing we default
        to four running at once.
        """
        try:
            with log_errors():
                while True:
                    msg = await self.q.get()
                    op = msg["op"]
                    if op == "send":
                        self.send_length += msg["len"]
                        for i in itertools.count():
                            try:
                                await self.worker.rpc(msg["worker"]).shuffle_receive(
                                    data=msg["part"]
                                )
                            except OSError:
                                # TODO better transient error logic (how does this interact with comm-level retries?)
                                if i >= 3:
                                    raise
                            else:
                                break

                        self.send_length -= msg["len"]

                    del msg
                    self.send_capacity.release()

                    if op == "close":
                        return

        except Exception as e:
            self.error = e
            raise

    def process_received_shards(self):
        """
        This runs in a thread borrowed from the worker's ThreadPoolExecutor.

        1. Wait until enough shards have been received
        2. Concatenate in order to reduce fragmentation
        3. Group them into output partitions
        4. Write each group to disk
        """
        with log_errors():
            try:
                while True:
                    if self.done_receiving:
                        shards = self.worker.shuffler.collect(flush=True)
                        if not shards:
                            return
                    else:
                        if not self.worker.shuffler.ready_to_collect.wait(5):
                            continue
                        shards = self.worker.shuffler.collect()
                        if not shards:
                            continue

                    self.disk_write_bytes += sum(map(len, shards)) * self.row_size
                    result = _concat(shards)
                    del shards
                    groups = list(result.groupby(self.column))
                    del result

                    while groups:
                        # NOTE: this awkward syntax allows each group DataFrame to be released
                        # early by `send_partial_result_to_disk`, since we don't hold a reference
                        # to the group within this function.
                        self.write_partial_result_to_disk(*groups.pop())
            except Exception as e:
                self.error = e
                raise

    def write_partial_result_to_disk(self, partition: int, part: pd.DataFrame):
        """
        Serialize and write a part of an output partition to disk.

        This method is complex because it avoids doing IO operations while holding any global locks,
        while maintaining a bounded cache of open file objects.

        Arguments
        ---------
        partition:
            The output partition number this fragment belongs to
        part:
            Fragment of an output partition
        """
        nrows = len(part)
        header, frames = serialize(part)
        # TODO maybe_compress
        del part
        header = msgpack.dumps(header)

        prelude = {"lengths": [nbytes(header)] + list(map(nbytes, frames))}
        prelude = msgpack.dumps(prelude)
        to_write = [struct.pack("Q", nbytes(prelude)), prelude, header] + frames

        # First, get exclusive access to this filename (protects create, read, and close)
        with self.files_dicts_lock:
            try:
                single_file_lock = self.file_locks[partition]
            except KeyError:
                single_file_lock = threading.Lock()
                self.file_locks[partition] = single_file_lock

        # Then, get or create the actual file object for that name
        with single_file_lock:
            old_file = old_file_lock = old_amount_written = None
            with self.files_dicts_lock:
                file = self.files.get(partition, None)
                amount_written = self.file_amount_written.get(partition, [0])
                if file is None:
                    self.file_cache_misses += 1
                    # Close an existing file if necessary to make room for the new one
                    if len(self.files) >= self.num_fds:
                        # remove the first file added; hopefully this is a good heuristic
                        old_key = next(iter(self.files))
                        old_file_lock = self.file_locks[old_key]
                        old_amount_written = self.file_amount_written[old_key]
                        old_file = self.files.pop(old_key)
                else:
                    self.file_cache_hits += 1

            if old_file is not None:
                # Closing the old file is slow if it has buffer to flush, so we want to
                # release `self.files_dicts_lock` first.
                # Holding `old_file_lock` ensures no other thread is trying to re-create
                # or write to the file at the same time we're closing it.
                assert old_file_lock is not None
                assert old_amount_written is not None
                with old_file_lock:
                    old_file.close()
                    self.write_amounts_at_close.append(old_amount_written[0])
                    self.file_buffers_bytes -= min(
                        old_amount_written[0], self.file_buffer_size
                    )
                    old_amount_written[0] = 0

            if file is None:
                # Open the new file
                local_dir = self.worker.local_directory
                path = os.path.join(local_dir, str(partition))
                file = open(path, mode="ab", buffering=self.file_buffer_size)

                with self.files_dicts_lock:
                    self.files[partition] = file
                    self.file_amount_written[partition] = amount_written

            file.writelines(to_write)
            size = sum(map(nbytes, to_write))
            self.write_sizes.append(size)
            prev_amount_written = amount_written[0]
            amount_written[0] = prev_amount_written + size
            if prev_amount_written < self.file_buffer_size:
                # In CPython, file buffers use incrementally more space until they reach capacity,
                # then hold onto that capacity forever until closed.
                self.file_buffers_bytes += min(
                    size, self.file_buffer_size - prev_amount_written
                )

            # NOTE: this may be quite different from `size` if the `row_size` estimation is off,
            # but we incremented using `rows_size`, so we must decrement using the same.
            self.disk_write_bytes -= nrows * self.row_size

    def barrier(self, _):
        """
        This gets called after all of our input data has been processed,
        but before we start producing output chunks.

        We call `finish_send` on every relevant worker to ensure all queued sends
        are flushed, then we tell the scheduler where to run the final pickup tasks,
        since only one worker actually has the data on disk that they need.

        This is a task in the task graph.

        Parameters
        ----------
        _: list[None]
            We take some tokens from the input tasks to form a barrier
        """
        sync(self.worker.loop, self._barrier)

    async def _barrier(self):
        key = self.key.replace("service", "unpack")

        # See who has the ShuffleService task
        # These are the workers that have participated in the shuffle
        # TODO why can't we just use `self.workers` here? how could it not match?
        who_has = await self.worker.scheduler.who_has(keys=[self.key])
        workers = who_has[self.key]

        # Start a finish_send call on every worker.
        # When this is done everything is shuffled.
        await asyncio.gather(
            *[self.worker.rpc(worker).shuffle_finish_send() for worker in workers]
        )

        # Let the scheduler know where to pick up each partition
        restrictions = {}
        for i in range(self.n_partitions):
            restrictions[str((key, i))] = [self.workers[int(self.worker_holding(i))]]

        await self.worker.scheduler.set_restrictions(worker=restrictions)

    async def finish_send(self, comm):
        """
        Finish up sending. This is called as an RPC by the `barrier` task.

        After all the `transfer` tasks have run, we need to flush any remaining
        data from ``self.shards`` and send it to peers.

        Then we put final "close" messages into the queues to stop the
        `watch_and_send` coroutines.

        See Also
        --------
        ShuffleService.barrier
        """
        # Put all of the remaining shards on the queue
        while self.shards:
            key, L = self.shards.popitem()
            length = self.shards_lengths.pop(key)
            self.shards_length -= length
            data = _concat(L)
            del L

            await self.q.put(
                {
                    "op": "send",
                    "worker": self.workers[key],
                    "part": to_serialize(data),
                    "len": len(data),
                }
            )
            del data

        # Send a close signal for every coroutine running
        for _ in self.send_coroutines:
            await self.q.put({"op": "close"})

        # wait until the wait_and_send coroutines have sent everything we just gave them
        await asyncio.gather(*self.send_coroutines)
        self.send_coroutines.clear()

    def get(self, part: int) -> pd.DataFrame:
        """Get the output partition

        All of the shards have been transferred at this point, and are mostly or entirely
        written to disk. Here, we read the file for the given partition, concatenate all the
        sub-parts within it, and return the final DataFrame.

        The first time `get` is called, we ensure that any data buffered in the worker's
        `ShuffleReceiver` or buffered file objects is first flushed to disk. We also clean up
        the `process_received_shards` threads, the comm handler we added to the worker,
        and the `ShuffleReceiver`.

        Parameters
        ----------
        part:
            The index of the partition

        Returns
        -------
        result:
            The actual output partition
        """
        assert (
            not self.shards
        ), f"{len(self.shards)} shards to send remaining during `get`"
        with self.lock_final_gather:
            if not self.done_receiving:
                # Flush data and clean up the receive handlers, now that we know all sends are done.
                # Do this only on the very first `get`.
                self.done_receiving = True
                # ^ this causes the `process_received_shards` threads to flush any data out
                # of `worker.shuffler`, then stop.
                concurrent.futures.wait(self.retrieve_futures)
                self.retrieve_futures.clear()

                self.worker.handlers.pop("shuffle_finish_send")
                self.worker.shuffler.uninstall()
                del self.worker.shuffler

                # Flush all open files. We have `lock_final_gather` so we can safely ignore
                # the other file locks (`get` should never be called until all shards have already
                # been written to files).
                # TODO parallelize / split between threads
                for k, f in self.files.items():
                    f.close()
                    amount_written = self.file_amount_written.pop(k)[0]
                    self.file_buffers_bytes -= min(
                        amount_written, self.file_buffer_size
                    )
                self.files.clear()
                self.file_locks.clear()
                assert (
                    not self.file_amount_written
                ), f"{len(self.file_amount_written)} keys in `file_amount_written` after closing all files"
                assert (
                    self.file_buffers_bytes == 0
                ), f"{format_bytes(self.file_buffers_bytes)} in `file_buffers_bytes` after flush"

        # Since all sends are complete, at this point all result parts for all partitions should be on disk.

        parts = []
        local_dir = self.worker.local_directory
        path = os.path.join(local_dir, str(part))
        if not os.path.exists(path):
            # There was no data for this partition
            return self.empty
        with open(path, mode="rb") as f:  # TODO buffering?
            # Read the entire file into memory at once, then parse it
            filesize = os.fstat(f.fileno()).st_size
            data = MemoryviewReader(memoryview(bytearray(filesize)))
            f.readinto(data.raw)
        os.remove(path)

        with data:
            while True:
                nb = data.read(8)
                if not nb:
                    break
                assert len(nb) == 8, "Unexpected EOF while reading prelude"
                (n,) = struct.unpack("Q", nb)

                prelude = msgpack.loads(data.read(n))
                lengths = prelude["lengths"]

                frames = [data.read(l) for l in lengths]

                header = msgpack.loads(frames[0])
                frames = frames[1:]
                # TODO decompress

                part = deserialize(header, frames)
                parts.append(part)
                self.subpart_sizes.append(len(part) * self.row_size)
                del nb, frames, part

        self.subpart_counts.append(len(parts))
        if not parts:
            return self.empty
        else:
            return _concat(parts)

    def stop(self):
        # TODO not threadsafe, nor robust to broken state that could be causing
        # `stop` to get called in the first place.
        if not self._started:
            return

        for t in self.send_coroutines:
            t.cancel()
        for f in self.retrieve_futures:
            f.cancel()
        # self.report_size_coroutine.cancel()

        self.shards.clear()

        local_dir = self.worker.local_directory
        for k, f in self.files.items():
            # NOTE: if the file is removed before the file object is closed, the file isn't
            # recreated and (hopefully) the buffer is just dropped without flushing.
            os.remove(os.path.join(local_dir, str(k)))

        self.files.clear()

        self.worker.handlers.pop("shuffle_finish_send", None)
        try:
            try:
                self.worker.shuffler.uninstall()
            except (KeyError, AssertionError):
                pass
            del self.worker.shuffler
        except AttributeError:
            pass
        try:
            del self.q
        except AttributeError:
            pass

    def __del__(self):
        self.stop()

    ### Debugging stuff
    ###################

    async def report_size(self):
        from dask.distributed import print

        while True:
            await asyncio.sleep(5)
            try:
                print(self.get_bytes())
                print(
                    self.get_stats(
                        "send_sizes",
                        "write_sizes",
                        "write_amounts_at_close",
                        "subpart_sizes",
                        "subpart_counts",
                    )
                )
            except AttributeError as e:
                print(e)
                break

    def get_stats(self, *stats):
        import numpy as np

        res = {}
        for stat in stats:
            vs = getattr(self, stat)
            setattr(self, stat, [])
            if vs:
                vs = np.array(vs)
                res.update(
                    {
                        f"{stat} mean": format_bytes(np.mean(vs)),
                        f"{stat} p1": format_bytes(np.percentile(vs, 1)),
                        f"{stat} p10": format_bytes(np.percentile(vs, 10)),
                        f"{stat} p50": format_bytes(np.percentile(vs, 50)),
                        f"{stat} p90": format_bytes(np.percentile(vs, 90)),
                        f"{stat} p99": format_bytes(np.percentile(vs, 99)),
                    }
                )
        return res

    def get_cache_rate(self):
        hits = self.file_cache_hits
        misses = self.file_cache_misses

        return f"hit rate: {hits / (hits + misses):.1%} {hits=} {misses=}"

    def get_bytes(self):
        import psutil

        from dask.utils import format_bytes

        return {
            "managed": format_bytes(self.worker.data.fast.total_weight),
            "shards": format_bytes(self.row_size * self.shards_length),
            "gathered_shards": (
                format_bytes(self.row_size * self.worker.shuffler.gathered_rows)
                if hasattr(self.worker, "shuffler")
                else None
            ),
            "queue": format_bytes(
                self.row_size * sum(len(d["part"].data) for d in self.q._queue)
            ),
            "send": format_bytes(self.row_size * self.send_length),
            "disk": format_bytes(self.disk_write_bytes),
            "file_buffers": format_bytes(self.file_buffers_bytes),
            "rss": format_bytes(psutil.Process().memory_info().rss),
        }


### Utility
###########

# TODO are we getting any value from this? It was intended to pair with https://github.com/dask/distributed/pull/5208
# but may be insignificant compared to GIL contention
# during `pd.concat` https://github.com/pandas-dev/pandas/issues/43155#issuecomment-915689887
class MemoryviewReader:
    "File-like object to read from a memoryview. Like BytesIO but zero-copy."

    def __init__(self, mv: memoryview) -> None:
        self.mv = mv
        self.cursor = 0

    def read(self, n: int = -1) -> memoryview:
        if self.mv is None:
            raise ValueError("read on closed MemoryviewReader")
        part = self.mv[self.cursor : None if n == -1 else (self.cursor + n)]
        self.cursor += len(part)
        return part

    def close(self):
        self.mv = None

    @property
    def closed(self) -> bool:
        return self.mv is None

    @property
    def raw(self) -> memoryview:
        if self.mv is None:
            raise ValueError("closed MemoryviewReader has no raw buffer")
        return self.mv

    def __enter__(self) -> MemoryviewReader:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
