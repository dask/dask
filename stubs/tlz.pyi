from __future__ import annotations
from typing import TypeVar, Iterable, Generic, Callable, Any

S = TypeVar("S")
T = TypeVar("T")

def concat(*args: Iterable[T]) -> Iterable[T]:
    ...

class curry(Generic[T]):
    def __init__(self, fn: Callable[..., T]):
        ...

    def __call__(self, *args, **kwargs) -> Curry | T:
        ...

def __getattr__(name) -> Any: ...
