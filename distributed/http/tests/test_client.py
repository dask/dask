from distributed.http.client import (scheduler_status_str,
        scheduler_status_widget)

d = {"address": "SCHEDULER_ADDRESS:9999",
     "ready": 5,
     "ncores": {"192.168.1.107:44544": 4,
                "192.168.1.107:36441": 4},
     "in-memory": 30, "waiting": 20,
     "processing": {"192.168.1.107:44544": {'inc': 3, 'add': 1},
                    "192.168.1.107:36441": {'inc': 2}},
     "tasks": 70,
     "failed": 9,
     "bytes": {"192.168.1.107:44544": 1000,
               "192.168.1.107:36441": 2000}}

def test_scheduler_status_str():
    result = scheduler_status_str(d)
    expected = """
Scheduler: SCHEDULER_ADDRESS:9999

             Count                                  Progress
Tasks
waiting         20  +++++++++++
ready            5  ++
failed           9  +++++
in-progress      6  +++
in-memory       30  +++++++++++++++++
total           70  ++++++++++++++++++++++++++++++++++++++++

                     Ncores  Bytes  Processing
Workers
192.168.1.107:36441       4   2000       [inc]
192.168.1.107:44544       4   1000  [add, inc]
""".strip()
    assert result == expected
