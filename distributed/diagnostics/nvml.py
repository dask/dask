import pynvml

handles = None


def _pynvml_handles():
    global handles
    if handles is None:
        pynvml.nvmlInit()
        count = pynvml.nvmlDeviceGetCount()
        handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(count)]
    return handles


def real_time():
    handles = _pynvml_handles()
    return {
        "utilization": [pynvml.nvmlDeviceGetUtilizationRates(h).gpu for h in handles],
        "memory-used": [pynvml.nvmlDeviceGetMemoryInfo(h).used for h in handles],
    }


def one_time():
    handles = _pynvml_handles()
    return {
        "memory-total": [pynvml.nvmlDeviceGetMemoryInfo(h).total for h in handles],
        "name": [pynvml.nvmlDeviceGetName(h).decode() for h in handles],
    }
