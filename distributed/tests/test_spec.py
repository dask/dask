from distributed.deploy.spec import ProcessInterface


def test_address_default_none():
    p = ProcessInterface()
    assert p.address is None


def test_child_address_persists():
    class Child(ProcessInterface):
        def __init__(self, address=None):
            self.address = address
            super().__init__()

    c = Child()
    assert c.address is None
    c = Child("localhost")
    assert c.address == "localhost"
