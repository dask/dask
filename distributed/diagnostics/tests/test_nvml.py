import pytest
import os

pynvml = pytest.importorskip("pynvml")

from distributed.diagnostics import nvml


def test_one_time():
    output = nvml.one_time()
    assert "memory-total" in output
    assert "name" in output

    assert len(output["name"]) > 0


def test_1_visible_devices():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    output = nvml.one_time()
    assert len(output["memory-total"]) == 1


@pytest.mark.parametrize("CVD", ["1,0", "0,1"])
def test_2_visible_devices(CVD):
    os.environ["CUDA_VISIBLE_DEVICES"] = CVD
    idx = int(CVD.split(",")[0])

    h = nvml._pynvml_handles()
    h2 = pynvml.nvmlDeviceGetHandleByIndex(idx)

    s = pynvml.nvmlDeviceGetSerial(h)
    s2 = pynvml.nvmlDeviceGetSerial(h2)

    assert s == s2
