from __future__ import print_function, division, absolute_import

from glob import glob
import os

from . import core
from ..base import tokenize
from fsspec.implementations.local import LocalFileSystem

core._filesystems["file"] = LocalFileSystem
