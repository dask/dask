"""Utilities for serialization/deserializaiton of objects that can be
exchanged between Azure Functions backend and the client that will
schedule Functions on this.
"""
import base64
import cloudpickle


def serialize(obj):
    """Serialize using cloudpickle and base64 encode the output.
    """
    pickled = cloudpickle.dumps(obj)
    encoded = base64.b64encode(pickled)
    return encoded


def deserialize(bstr):
    """Base64 decode the input and then deserialize using cloudpickle.
    """
    pickled = base64.b64decode(bstr)
    obj = cloudpickle.loads(pickled)
    return obj
