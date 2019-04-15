from distributed.protocol import serialize, deserialize
import pytest

np = pytest.importorskip("numpy")
torch = pytest.importorskip("torch")


def test_tensor():
    x = np.arange(10)
    t = torch.Tensor(x)
    header, frames = serialize(t)
    assert header["serializer"] == "dask"
    t2 = deserialize(header, frames)
    assert (x == t2.numpy()).all()


def test_grad():
    x = np.arange(10)
    t = torch.Tensor(x)
    t.grad = torch.zeros_like(t) + 1

    t2 = deserialize(*serialize(t))
    assert (t2.numpy() == x).all()
    assert (t2.grad.numpy() == 1).all()


def test_resnet():
    torchvision = pytest.importorskip("torchvision")
    model = torchvision.models.resnet.resnet18()

    header, frames = serialize(model)
    model2 = deserialize(header, frames)
    assert str(model) == str(model2)


def test_deserialize_grad():
    a = np.random.rand(8, 1)
    t = torch.tensor(a, requires_grad=True, dtype=torch.float)
    t2 = deserialize(*serialize(t))
    assert t2.requires_grad
    assert np.allclose(a, t2.detach_().numpy())
