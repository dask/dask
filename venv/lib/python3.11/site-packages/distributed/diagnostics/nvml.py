from __future__ import annotations

import os
from enum import IntEnum, auto
from platform import uname
from typing import NamedTuple

from packaging.version import parse as parse_version

import dask
from dask.utils import ensure_unicode

try:
    import pynvml
except ImportError:
    pynvml = None


class NVMLState(IntEnum):
    UNINITIALIZED = auto()
    """No attempt yet made to initialize PyNVML"""
    INITIALIZED = auto()
    """PyNVML was successfully initialized"""
    DISABLED_PYNVML_NOT_AVAILABLE = auto()
    """PyNVML not installed"""
    DISABLED_CONFIG = auto()
    """PyNVML diagnostics disabled by ``distributed.diagnostics.nvml`` config setting"""
    DISABLED_LIBRARY_NOT_FOUND = auto()
    """PyNVML available, but NVML not installed"""
    DISABLED_WSL_INSUFFICIENT_DRIVER = auto()
    """PyNVML and NVML available, but on WSL and the driver version is insufficient"""


class CudaDeviceInfo(NamedTuple):
    # Older versions of pynvml returned bytes, newer versions return str.
    uuid: str | bytes | None = None
    device_index: int | None = None
    mig_index: int | None = None


class CudaContext(NamedTuple):
    has_context: bool
    device_info: CudaDeviceInfo | None = None


# Initialisation must occur per-process, so an initialised state is a
# (state, pid) pair
NVML_STATE = (
    NVMLState.DISABLED_PYNVML_NOT_AVAILABLE
    if pynvml is None
    else NVMLState.UNINITIALIZED
)
"""Current initialization state"""

NVML_OWNER_PID = None
"""PID of process that successfully called pynvml.nvmlInit"""

MINIMUM_WSL_VERSION = "512.15"


def is_initialized():
    """Is pynvml initialized on this process?"""
    return NVML_STATE == NVMLState.INITIALIZED and NVML_OWNER_PID == os.getpid()


def _in_wsl():
    """Check if we are in Windows Subsystem for Linux; some PyNVML queries are not supported there.
    Taken from https://www.scivision.dev/python-detect-wsl/
    """
    return "microsoft-standard" in uname().release


def init_once():
    """Idempotent (per-process) initialization of PyNVML

    Notes
    -----

    Modifies global variables NVML_STATE and NVML_OWNER_PID"""
    global NVML_STATE, NVML_OWNER_PID

    if NVML_STATE in {
        NVMLState.DISABLED_PYNVML_NOT_AVAILABLE,
        NVMLState.DISABLED_CONFIG,
        NVMLState.DISABLED_LIBRARY_NOT_FOUND,
        NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER,
    }:
        return
    elif NVML_STATE == NVMLState.INITIALIZED and NVML_OWNER_PID == os.getpid():
        return
    elif NVML_STATE == NVMLState.UNINITIALIZED and not dask.config.get(
        "distributed.diagnostics.nvml"
    ):
        NVML_STATE = NVMLState.DISABLED_CONFIG
        return
    elif (
        NVML_STATE == NVMLState.INITIALIZED and NVML_OWNER_PID != os.getpid()
    ) or NVML_STATE == NVMLState.UNINITIALIZED:
        try:
            pynvml.nvmlInit()
        except (
            pynvml.NVMLError_LibraryNotFound,
            pynvml.NVMLError_DriverNotLoaded,
            pynvml.NVMLError_Unknown,
        ):
            NVML_STATE = NVMLState.DISABLED_LIBRARY_NOT_FOUND
            return

        if _in_wsl() and parse_version(
            ensure_unicode(pynvml.nvmlSystemGetDriverVersion())
        ) < parse_version(MINIMUM_WSL_VERSION):
            NVML_STATE = NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER
            return
        else:
            from distributed.worker import add_gpu_metrics

            # initialization was successful
            NVML_STATE = NVMLState.INITIALIZED
            NVML_OWNER_PID = os.getpid()
            add_gpu_metrics()
    else:
        raise RuntimeError(
            f"Unhandled initialisation state ({NVML_STATE=}, {NVML_OWNER_PID=})"
        )


def device_get_count():
    init_once()
    if not is_initialized():
        return 0
    else:
        return pynvml.nvmlDeviceGetCount()


def _pynvml_handles():
    count = device_get_count()
    if NVML_STATE == NVMLState.DISABLED_PYNVML_NOT_AVAILABLE:
        raise RuntimeError("NVML monitoring requires PyNVML and NVML to be installed")
    if NVML_STATE == NVMLState.DISABLED_LIBRARY_NOT_FOUND:
        raise RuntimeError("PyNVML is installed, but NVML is not")
    if NVML_STATE == NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER:
        raise RuntimeError(
            "Outdated NVIDIA drivers for WSL, please upgrade to "
            f"{MINIMUM_WSL_VERSION} or newer"
        )
    if NVML_STATE == NVMLState.DISABLED_CONFIG:
        raise RuntimeError(
            "PyNVML monitoring disabled by 'distributed.diagnostics.nvml' "
            "config setting"
        )
    if count == 0:
        raise RuntimeError("No GPUs available")

    device = 0
    cuda_visible_devices = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    if cuda_visible_devices:
        device = _parse_cuda_visible_device(cuda_visible_devices.split(",")[0])
    return _get_handle(device)


# Port from https://github.com/rapidsai/dask-cuda/blob/0f34116c4f3cdf5dfc0df0dbfeba92655f686716/dask_cuda/utils.py#L403-L437
def _parse_cuda_visible_device(dev):
    """Parses a single CUDA device identifier

    A device identifier must either be an integer, a string containing an
    integer or a string containing the device's UUID, beginning with prefix
    'GPU-' or 'MIG-'.

    >>> parse_cuda_visible_device(2)
    2
    >>> parse_cuda_visible_device('2')
    2
    >>> parse_cuda_visible_device('GPU-9baca7f5-0f2f-01ac-6b05-8da14d6e9005')
    'GPU-9baca7f5-0f2f-01ac-6b05-8da14d6e9005'
    >>> parse_cuda_visible_device('Foo')
    Traceback (most recent call last):
    ...
    ValueError: Devices in CUDA_VISIBLE_DEVICES must be comma-separated integers or
    strings beginning with 'GPU-' or 'MIG-' prefixes.
    """
    try:
        return int(dev)
    except ValueError:
        if any(
            dev.startswith(prefix)
            for prefix in [
                "GPU-",
                "MIG-",
            ]
        ):
            return dev
        else:
            raise ValueError(
                "Devices in CUDA_VISIBLE_DEVICES must be comma-separated integers "
                "or strings beginning with 'GPU-' or 'MIG-' prefixes."
            )


def _running_process_matches(handle):
    """Check whether the current process is same as that of handle

    Parameters
    ----------
    handle : pyvnml.nvml.LP_struct_c_nvmlDevice_t
        NVML handle to CUDA device

    Returns
    -------
    out : bool
        Whether the device handle has a CUDA context on the running process.
    """
    init_once()
    if hasattr(pynvml, "nvmlDeviceGetComputeRunningProcesses_v2"):
        running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses_v2(handle)
    else:
        running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)
    return any(os.getpid() == proc.pid for proc in running_processes)


def has_cuda_context():
    """Check whether the current process already has a CUDA context created.

    Returns
    -------
    out : CudaContext
        Object containing information as to whether the current process has a CUDA
        context created, and in the positive case containing also information about
        the device the context belongs to.
    """
    init_once()
    if is_initialized():
        for index in range(device_get_count()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            try:
                mig_current_mode, mig_pending_mode = pynvml.nvmlDeviceGetMigMode(handle)
            except pynvml.NVMLError_NotSupported:
                mig_current_mode = pynvml.NVML_DEVICE_MIG_DISABLE
            if mig_current_mode == pynvml.NVML_DEVICE_MIG_ENABLE:
                for mig_index in range(pynvml.nvmlDeviceGetMaxMigDeviceCount(handle)):
                    try:
                        mig_handle = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(
                            handle, mig_index
                        )
                    except pynvml.NVMLError_NotFound:
                        # No MIG device with that index
                        continue
                    if _running_process_matches(mig_handle):
                        uuid = pynvml.nvmlDeviceGetUUID(mig_handle)
                        return CudaContext(
                            has_context=True,
                            device_info=CudaDeviceInfo(
                                uuid=uuid, device_index=index, mig_index=mig_index
                            ),
                        )
            else:
                if _running_process_matches(handle):
                    uuid = pynvml.nvmlDeviceGetUUID(handle)
                    return CudaContext(
                        has_context=True,
                        device_info=CudaDeviceInfo(uuid=uuid, device_index=index),
                    )
    return CudaContext(has_context=False)


def get_device_index_and_uuid(device):
    """Get both device index and UUID from device index or UUID

    Parameters
    ----------
    device : int, bytes or str
        An ``int`` with the index of a GPU, or ``bytes`` or ``str`` with the UUID
        of a CUDA (either GPU or MIG) device.

    Returns
    -------
    out : CudaDeviceInfo
        Object containing information about the device.

    Examples
    --------
    >>> get_device_index_and_uuid(0)  # doctest: +SKIP
    {'device-index': 0, 'uuid': 'GPU-e1006a74-5836-264f-5c26-53d19d212dfe'}

    >>> get_device_index_and_uuid('GPU-e1006a74-5836-264f-5c26-53d19d212dfe')  # doctest: +SKIP
    {'device-index': 0, 'uuid': 'GPU-e1006a74-5836-264f-5c26-53d19d212dfe'}

    >>> get_device_index_and_uuid('MIG-7feb6df5-eccf-5faa-ab00-9a441867e237')  # doctest: +SKIP
    {'device-index': 0, 'uuid': 'MIG-7feb6df5-eccf-5faa-ab00-9a441867e237'}
    """
    init_once()
    try:
        device_index = int(device)
        device_handle = pynvml.nvmlDeviceGetHandleByIndex(device_index)
        uuid = pynvml.nvmlDeviceGetUUID(device_handle)
    except ValueError:
        uuid = device if isinstance(device, bytes) else bytes(device, "utf-8")

        # Validate UUID, get index and UUID as seen with `nvidia-smi -L`
        uuid_handle = pynvml.nvmlDeviceGetHandleByUUID(uuid)
        device_index = pynvml.nvmlDeviceGetIndex(uuid_handle)
        uuid = pynvml.nvmlDeviceGetUUID(uuid_handle)

    return CudaDeviceInfo(uuid=uuid, device_index=device_index)


def get_device_mig_mode(device):
    """Get MIG mode for a device index or UUID

    Parameters
    ----------
    device: int, bytes or str
        An ``int`` with the index of a GPU, or ``bytes`` or ``str`` with the UUID
        of a CUDA (either GPU or MIG) device.

    Returns
    -------
    out : list
        A ``list`` with two integers ``[current_mode, pending_mode]``.
    """
    init_once()
    handle = _get_handle(device)
    try:
        return pynvml.nvmlDeviceGetMigMode(handle)
    except pynvml.NVMLError_NotSupported:
        return [0, 0]


def _get_handle(device):
    try:
        device_index = int(device)
        return pynvml.nvmlDeviceGetHandleByIndex(device_index)
    except ValueError:
        uuid = device if isinstance(device, bytes) else bytes(device, "utf-8")
        return pynvml.nvmlDeviceGetHandleByUUID(uuid)


def _get_utilization(h):
    try:
        return pynvml.nvmlDeviceGetUtilizationRates(h).gpu
    except pynvml.NVMLError_NotSupported:
        return None


def _get_memory_used(h):
    try:
        return pynvml.nvmlDeviceGetMemoryInfo(h).used
    except pynvml.NVMLError_NotSupported:
        return None


def _get_memory_total(h):
    try:
        return pynvml.nvmlDeviceGetMemoryInfo(h).total
    except pynvml.NVMLError_NotSupported:
        return None


def _get_name(h):
    try:
        return pynvml.nvmlDeviceGetName(h).decode()
    except AttributeError:
        return pynvml.nvmlDeviceGetName(h)
    except pynvml.NVMLError_NotSupported:
        return None


def real_time():
    h = _pynvml_handles()
    return {
        "utilization": _get_utilization(h),
        "memory-used": _get_memory_used(h),
    }


def one_time():
    h = _pynvml_handles()
    return {
        "memory-total": _get_memory_total(h),
        "name": _get_name(h),
    }
