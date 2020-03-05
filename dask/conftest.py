import pytest
from distributed.utils_test import check_process_leak


@pytest.fixture(autouse=True)
def transact():
    with check_process_leak():
        yield
