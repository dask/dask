from matchpy import Arity, Operation
from matchpy.expressions.expressions import _OperationMeta


class _APIMeta(_OperationMeta):
    def __call__(cls, *args, variable_name=None, **kwargs):
        operands = list(args)
        for parameter in cls._parameters[len(operands) :]:
            operands.append(kwargs.pop(parameter, cls._defaults[parameter]))
        assert not kwargs
        return super().__call__(*operands, variable_name=None)


class API(Operation, metaclass=_APIMeta):
    commutative = False
    associative = False
    _parameters = []

    def __getattr__(self, key):
        if key in type(self)._parameters:
            idx = type(self)._parameters.index(key)
            return self.operands[idx]
        else:
            return object.__getattribute__(self, key)

    def __getitem__(self, columns):
        return Projection(self, columns)

    def __add__(self, other):
        return Add(self, other)

    def __sub__(self, other):
        return Sub(self, other)

    def __mul__(self, other):
        return Mul(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __rsub__(self, other):
        return Sub(other, self)

    def __rmul__(self, other):
        return Mul(other, self)

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None, min_count=0):
        return Sum(self, axis, skipna, level, numeric_only, min_count)


class Blockwise(API):
    arity = Arity.variadic
    commutative = False
    associative = False


class Elemwise(Blockwise):
    pass


class Filter(Blockwise):
    pass


class Projection(Elemwise):
    _parameters = ["frame", "columns"]
    pass


class Binop(Elemwise):
    arity = Arity.binary


class Add(Binop):
    commutative = True

    def __str__(self):
        return "{} + {}".format(*self.operands)


class Mul(Binop):
    commutative = True

    def __str__(self):
        return "{} * {}".format(*self.operands)


class Sub(Binop):
    commutative = False

    def __str__(self):
        return "{} - {}".format(*self.operands)


class Reduction(API):
    arity = Arity.variadic
    associative = False
    commutative = False


class Sum(Reduction):
    _parameters = ["frame", "axis", "skipna", "level", "numeric_only", "min_count"]
    _defaults = {
        "axis": None,
        "skipna": True,
        "level": None,
        "numeric_only": None,
        "min_count": 0,
    }


class IO(API):
    pass


class ReadParquet(IO):
    _parameters = ["filename", "columns", "filters"]
    _defaults = {"columns": None, "filters": None}


class ReadCSV(IO):
    _parameters = ["filename", "usecols", "header"]
    _defaults = {"usecols": None, "header": None}


df = ReadParquet("myfile.parquet", columns=("a", "b", "c"))
expr = (df - 1)["a"].sum(skipna=False)
