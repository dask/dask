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


@pytest.mark.parametrize("requires_grad", [True, False])
def test_grad(requires_grad):
    x = np.arange(10)
    t = torch.tensor(x, dtype=torch.float, requires_grad=requires_grad)

    if requires_grad:
        t.grad = torch.zeros_like(t) + 1

    t2 = deserialize(*serialize(t))

    assert t2.requires_grad is requires_grad
    assert t.requires_grad is requires_grad
    assert np.allclose(t2.detach().numpy(), x)

    if requires_grad:
        assert np.allclose(t2.grad.numpy(), 1)


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
