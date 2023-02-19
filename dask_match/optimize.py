import functools
import numbers

from matchpy import CustomConstraint, Pattern, ReplacementRule, Wildcard, replace_all

from dask_match import (
    EQ,
    GE,
    GT,
    LE,
    LT,
    NE,
    Add,
    Blockwise,
    Count,
    Filter,
    Max,
    Min,
    Mul,
    Projection,
    ReadParquet,
    Sub,
    Sum,
    optimize,
)

_ = Wildcard.dot()
a, b, c, d, e, f, g = map(Wildcard.dot, "abcdefg")
x = Wildcard.dot("x")
y = Wildcard.dot("y")
blockwise = Wildcard.symbol(Blockwise)
read_parquet = Wildcard.symbol(ReadParquet)

rules = [
    ReplacementRule(
        Pattern(Add(x, x)),
        lambda x: Mul(2, x),
    ),
    ReplacementRule(
        Pattern(ReadParquet(a, columns=b, filters=c)[d]),
        lambda a, b, c, d: ReadParquet(a, columns=d, filters=c),
    ),
    ReplacementRule(
        Pattern(
            Mul(a, Mul(b, c)),
            CustomConstraint(
                lambda a, b, c: isinstance(a, numbers.Number)
                and isinstance(b, numbers.Number)
            ),
        ),
        lambda a, b, c: Mul(a * b, c),
    ),
]

# Column Projection
for op in [Add, Mul, Sub]:

    def transform(a, b, c, op=op):
        return op(Projection(a, c), Projection(b, c))

    rule = ReplacementRule(
        Pattern(Projection(op(a, b), c)),
        transform,
    )
    rules.append(rule)

# Predicate pushdown to parquet
df = ReadParquet(a, columns=b, filters=c)

for op in [LE, LT, GE, GT, EQ, NE]:

    def predicate_pushdown(a, b, c, d, e, op=None):
        return ReadParquet(
            a, columns=b, filters=(c or []) + [(op._operator_repr, d, e)]
        )

    rule = ReplacementRule(
        Pattern(
            Filter(
                ReadParquet(a, columns=b, filters=c),
                op(ReadParquet(a, columns=_, filters=c)[d], e),
            )
        ),
        functools.partial(predicate_pushdown, op=op),
    )
    rules.append(rule)

    def predicate_pushdown(a, b, c, d, e, op=None):
        return ReadParquet(
            a, columns=b, filters=(c or []) + [(op._operator_repr, e, d)]
        )

    rule = ReplacementRule(
        Pattern(
            Filter(
                ReadParquet(a, columns=b, filters=c),
                op(e, ReadParquet(a, columns=_, filters=c)[d]),
            )
        ),
        functools.partial(predicate_pushdown, op=op),
    )
    rules.append(rule)

    def predicate_pushdown(a, b, c, d, e, op=None):
        return ReadParquet(
            a, columns=b, filters=(c or []) + [(op._operator_repr, d, e)]
        )

    rule = ReplacementRule(
        Pattern(
            Filter(
                ReadParquet(a, columns=b, filters=c),
                op(ReadParquet(a, columns=d, filters=_), e),
            ),
            CustomConstraint(lambda d: isinstance(d, str)),
        ),
        functools.partial(predicate_pushdown, op=op),
    )
    rules.append(rule)

    def predicate_pushdown(a, b, c, d, e, op=None):
        return ReadParquet(
            a, columns=b, filters=(c or []) + [(op._operator_repr, e, d)]
        )

    rule = ReplacementRule(
        Pattern(
            Filter(
                ReadParquet(a, columns=b, filters=c),
                op(e, ReadParquet(a, columns=d, filters=_)),
            ),
            CustomConstraint(lambda d: isinstance(d, str)),
        ),
        functools.partial(predicate_pushdown, op=op),
    )
    rules.append(rule)

# push column projections down through reductions
for op in [Min, Max, Sum, Count]:
    args = map(Wildcard.dot, "abcdefghijklmnopqrstuv"[len(op._parameters) - 1])
    rule = ReplacementRule(
        Pattern(Min(x, *args)[y]),
        lambda x, y, *args: Min(x[y], *args),
    )
    rules.append(rule)


def optimize(expr):
    last = None
    import dask_match.core

    dask_match.core.matching = True  # take over ==/!= when optimizing
    try:
        while str(expr) != str(last):
            last = expr
            expr = replace_all(expr, rules)
    finally:
        dask_match.core.matching = False
    return expr
