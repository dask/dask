class Expr:
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        return self.__class__.__name__ + "(" + ', '.join(map(repr, self.args)) + ")"

    __str__ = __repr__
    # def __eq__(self, other):
    #     return type(self) == type(other) and self.args == other.args
