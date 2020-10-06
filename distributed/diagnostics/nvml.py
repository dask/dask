import os
import pynvml

nvmlInit = None


def init_once():
    global nvmlInit
    if nvmlInit is not None:
        return

    from pynvml import nvmlInit as _nvmlInit

    nvmlInit = _nvmlInit
    nvmlInit()


def _pynvml_handles():
    count = pynvml.nvmlDeviceGetCount()
    try:
        cuda_visible_devices = [
            int(idx) for idx in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",")
        ]
    except ValueError:
        # CUDA_VISIBLE_DEVICES is not set
        cuda_visible_devices = False
    if not cuda_visible_devices:
        cuda_visible_devices = list(range(count))
    gpu_idx = cuda_visible_devices[0]
    return pynvml.nvmlDeviceGetHandleByIndex(gpu_idx)


def real_time():
    init_once()
    h = _pynvml_handles()
    return {
        "utilization": pynvml.nvmlDeviceGetUtilizationRates(h).gpu,
        "memory-used": pynvml.nvmlDeviceGetMemoryInfo(h).used,
    }


def one_time():
    init_once()
    h = _pynvml_handles()
    return {
        "memory-total": pynvml.nvmlDeviceGetMemoryInfo(h).total,
        "name": pynvml.nvmlDeviceGetName(h).decode(),
    }
