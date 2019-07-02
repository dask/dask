from . import core
from fsspec.implementations.http import HTTPFileSystem

core._filesystems["http"] = HTTPFileSystem
core._filesystems["https"] = HTTPFileSystem
