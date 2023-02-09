import numbers

from matchpy import CustomConstraint, Pattern, ReplacementRule, Wildcard, replace_all

from dask_match import Add, Blockwise, Mul, Projection, ReadParquet, Sub, Sum, optimize

_ = Wildcard.dot()
a, b, c, d, e, f, g = map(Wildcard.dot, "abcdefg")
x = Wildcard.dot("x")
y = Wildcard.dot("y")
blockwise = Wildcard.symbol(Blockwise)
read_parquet = Wildcard.symbol(ReadParquet)

rules = [
    ReplacementRule(
        Pattern(ReadParquet(a, columns=b, filters=c)[d]),
        lambda a, b, c, d: ReadParquet(a, columns=d, filters=c),
    ),
    ReplacementRule(
        Pattern(Add(x, x)),
        lambda x: Mul(2, x),
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
    ReplacementRule(
        Pattern(Sum(a, b, c, d, e, f)[g]),
        lambda a, b, c, d, e, f, g: Sum(a[g], b, c, d, e, f),
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


def optimize(expr):
    last = None
    while str(expr) != str(last):
        last = expr
        expr = replace_all(expr, rules)
    return expr
