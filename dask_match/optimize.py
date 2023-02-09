from matchpy import Pattern, ReplacementRule, Wildcard, replace_all

from dask_match import Add, Blockwise, Mul, ReadParquet, optimize

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
]


def optimize(expr):
    return replace_all(expr, rules)
