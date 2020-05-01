import pytest
from time import sleep

import dask
from dask.utils import format_bytes
from distributed import Client
from distributed.utils_test import gen_test, loop, inc, cleanup, popen  # noqa: 401
from distributed.utils import get_ip
from distributed.comm.ucx import _scrub_ucx_config

try:
    HOST = get_ip()
except Exception:
    HOST = "127.0.0.1"

ucp = pytest.importorskip("ucp")
rmm = pytest.importorskip("rmm")


@pytest.mark.asyncio
async def test_ucx_config(cleanup):

    ucx = {
        "nvlink": True,
        "infiniband": True,
        "rdmacm": False,
        "net-devices": "",
        "tcp": True,
        "cuda_copy": True,
    }

    with dask.config.set(ucx=ucx):
        ucx_config = _scrub_ucx_config()
        assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"
        assert ucx_config.get("NET_DEVICES") is None

    ucx = {
        "nvlink": False,
        "infiniband": True,
        "rdmacm": False,
        "net-devices": "mlx5_0:1",
        "tcp": True,
        "cuda_copy": False,
    }

    with dask.config.set(ucx=ucx):
        ucx_config = _scrub_ucx_config()
        assert ucx_config.get("TLS") == "rc,tcp,sockcm"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"
        assert ucx_config.get("NET_DEVICES") == "mlx5_0:1"

    ucx = {
        "nvlink": False,
        "infiniband": True,
        "rdmacm": True,
        "net-devices": "all",
        "MEMTYPE_CACHE": "y",
        "tcp": True,
        "cuda_copy": True,
    }

    with dask.config.set(ucx=ucx):
        ucx_config = _scrub_ucx_config()
        assert ucx_config.get("TLS") == "rc,tcp,rdmacm,cuda_copy"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "rdmacm"
        assert ucx_config.get("MEMTYPE_CACHE") == "y"


def test_ucx_config_w_env_var(cleanup, loop, monkeypatch):
    size = "1000.00 MB"
    monkeypatch.setenv("DASK_RMM__POOL_SIZE", size)

    dask.config.refresh()

    port = "13339"
    sched_addr = "ucx://%s:%s" % (HOST, port)

    with popen(
        ["dask-scheduler", "--no-dashboard", "--protocol", "ucx", "--port", port]
    ) as sched:
        with popen(
            [
                "dask-worker",
                sched_addr,
                "--no-dashboard",
                "--protocol",
                "ucx",
                "--no-nanny",
            ]
        ) as w:
            with Client(sched_addr, loop=loop, timeout=10) as c:
                while not c.scheduler_info()["workers"]:
                    sleep(0.1)

                # configured with 1G pool
                rmm_usage = c.run_on_scheduler(rmm.get_info)
                assert size == format_bytes(rmm_usage.free)

                # configured with 1G pool
                worker_addr = list(c.scheduler_info()["workers"])[0]
                worker_rmm_usage = c.run(rmm.get_info)
                rmm_usage = worker_rmm_usage[worker_addr]
                assert size == format_bytes(rmm_usage.free)
