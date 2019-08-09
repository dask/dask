import pynvml

pynvml.nvmlInit()
count = pynvml.nvmlDeviceGetCount()

handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(count)]


def real_time():
    return {
        "utilization": [pynvml.nvmlDeviceGetUtilizationRates(h).gpu for h in handles],
        "memory-used": [pynvml.nvmlDeviceGetMemoryInfo(h).used for h in handles],
    }


def one_time():
    return {
        "memory-total": [pynvml.nvmlDeviceGetMemoryInfo(h).total for h in handles],
        "name": [pynvml.nvmlDeviceGetName(h).decode() for h in handles],
    }
