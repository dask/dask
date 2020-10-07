import pytest
import os

pynvml = pytest.importorskip("pynvml")

from distributed.diagnostics import nvml
from distributed.utils_test import gen_cluster


def test_one_time():
    output = nvml.one_time()
    assert "memory-total" in output
    assert "name" in output

    assert len(output["name"]) > 0


def test_1_visible_devices():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    output = nvml.one_time()
    h = nvml._pynvml_handles()
    assert output["memory-total"] == pynvml.nvmlDeviceGetMemoryInfo(h).total


@pytest.mark.parametrize("CVD", ["1,0", "0,1"])
def test_2_visible_devices(CVD):
    if pynvml.nvmlDeviceGetCount() <= 1:
        pytest.skip("Machine only has a single GPU")

    os.environ["CUDA_VISIBLE_DEVICES"] = CVD
    idx = int(CVD.split(",")[0])

    h = nvml._pynvml_handles()
    h2 = pynvml.nvmlDeviceGetHandleByIndex(idx)

    s = pynvml.nvmlDeviceGetSerial(h)
    s2 = pynvml.nvmlDeviceGetSerial(h2)

    assert s == s2


@gen_cluster()
async def test_gpu_metrics(s, a, b):
    h = nvml._pynvml_handles()

    assert "gpu" in a.metrics
    assert (
        s.workers[a.address].metrics["gpu"]["memory-used"]
        == pynvml.nvmlDeviceGetMemoryInfo(h).used
    )
    assert "gpu" in a.startup_information
    assert (
        s.workers[a.address].extra["gpu"]["name"]
        == pynvml.nvmlDeviceGetName(h).decode()
    )
