from __future__ import annotations

import torch

from distributed.protocol.serialize import (
    dask_deserialize,
    dask_serialize,
    deserialize,
    register_generic,
    serialize,
)


@dask_serialize.register(torch.Tensor)
def serialize_torch_Tensor(t):
    requires_grad_ = t.requires_grad

    if requires_grad_:
        sub_header, frames = serialize(t.detach().numpy())
    else:
        sub_header, frames = serialize(t.numpy())

    header = {"sub-header": sub_header}
    if t.grad is not None:
        grad_header, grad_frames = serialize(t.grad.numpy())
        header["grad"] = {"header": grad_header, "start": len(frames)}
        frames += grad_frames
    header["requires_grad"] = requires_grad_
    header["device"] = t.device.type
    return header, frames


@dask_deserialize.register(torch.Tensor)
def deserialize_torch_Tensor(header, frames):
    if header.get("grad", False):
        i = header["grad"]["start"]
        frames, grad_frames = frames[:i], frames[i:]
        grad = deserialize(header["grad"]["header"], grad_frames)
    else:
        grad = None

    x = deserialize(header["sub-header"], frames)
    if header["device"] == "cpu":
        t = torch.from_numpy(x)
        if header["requires_grad"]:
            t = t.requires_grad_(True)
    else:
        t = torch.tensor(
            data=x, device=header["device"], requires_grad=header["requires_grad"]
        )
    if grad is not None:
        t.grad = torch.from_numpy(grad)
    return t


@dask_serialize.register(torch.nn.Parameter)
def serialize_torch_Parameters(p):
    sub_header, frames = serialize(p.detach())
    header = {"sub-header": sub_header}
    header["requires_grad"] = p.requires_grad
    return header, frames


@dask_deserialize.register(torch.nn.Parameter)
def deserialize_torch_Parameters(header, frames):
    t = deserialize(header["sub-header"], frames)
    return torch.nn.Parameter(data=t, requires_grad=header["requires_grad"])


register_generic(torch.nn.Module)
