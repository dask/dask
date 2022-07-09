from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

if TYPE_CHECKING:
    import inspect

    # from functools import partial

partial: Any

S = TypeVar("S")
T = TypeVar("T")

CurryRet = TypeVar("CurryRet")
CurryCall = Callable[..., CurryRet]

#: The type of instances of the class being annotated
InstancePropertyParent = TypeVar("InstancePropertyParent")
#: The type returned by this property, if invoked on an instance
InstancePropertyInstanceRet = TypeVar("InstancePropertyInstanceRet")
#: The type returned by this property, if invoked on a class
InstancePropertyClassRet = TypeVar("InstancePropertyClassRet")

@overload
def instanceproperty(fget:Callable[[InstancePropertyParent], InstancePropertyInstanceRet], fset=None, fdel=None, doc=None, classval: Optional[InstancePropertyClassRet]=None) -> "InstanceProperty"[
    InstancePropertyParent,
    InstancePropertyInstanceRet,
    InstancePropertyClassRet
]:
    ...
@overload
def instanceproperty(fset=None, fdel=None, doc=None, classval=None) -> partial:
    ...
# def instanceproperty(fget=None, fset=None, fdel=None, doc=None, classval=None):
#     ...


class InstanceProperty(property, Generic[InstancePropertyParent, InstancePropertyInstanceRet, InstancePropertyClassRet]):
    def __init__(self, fget: Optional[Callable[[InstancePropertyParent], InstancePropertyInstanceRet]]=None, fset: Optional[Callable[[InstancePropertyParent, InstancePropertyInstanceRet], None]]=None, fdel: Optional[Callable[[InstancePropertyParent], None]]=None, doc: Optional[str]=None, classval: Optional[InstancePropertyClassRet]=None):
        ...

    @overload
    def __get__(self, obj: InstancePropertyParent, type=None) -> InstancePropertyInstanceRet: ...

    @overload
    def __get__(self, obj: Type[InstanceProperty], type=None) -> InstancePropertyClassRet:
        ...

    def __reduce__(self):
        state = (self.fget, self.fset, self.fdel, self.__doc__, self.classval)
        return instanceproperty, state


class curry(Generic[CurryRet]):
    def __init__(self, func: Callable[..., CurryRet], *args, **kwargs):
        ...

    @instanceproperty
    def func(self) -> CurryCall:
        ...

    @instanceproperty
    def __signature__(self) -> inspect.Signature:
        ...

    @instanceproperty
    def args(self) -> tuple:
        ...

    @instanceproperty
    def keywords(self) -> dict:
        ...

    @instanceproperty
    def func_name(self) -> str:
        ...

    def __str__(self):
        ...

    def __repr__(self):
        ...

    def __hash__(self):
        ...

    def __eq__(self, other):
        ...

    def __ne__(self, other):
        ...

    def __call__(self, *args, **kwargs) -> Union[CurryRet, "curry"]:
        ...

    def _should_curry(self, args, kwargs, exc: Any=None) -> bool:
        ...

    def bind(self, *args, **kwargs) -> "curry":
        ...

    def call(self, *args, **kwargs) -> CurryRet:
        ...

    # Should return Self, once supported by mypy
    @overload
    def __get__(self, instance: None, owner: Any) -> "curry":
        ...
    @overload
    def __get__(self, instance: Any, owner: Any) -> "curry":
        ...

    def __reduce__(self) -> Tuple[
        Callable,
        Tuple[
            Type["curry"],
            Union[CurryCall, str],
            tuple,
            dict,
            tuple,
            Optional[bool]
        ]
    ]:
        ...

def __getattr__(name) -> Any: ...
