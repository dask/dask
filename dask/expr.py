class Expr:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        func = self.__class__.__name__
        args = ', '.join(map(repr, self.args))
        kwargs = ', '.join(f"{key}={val}" for key, val in self.kwargs.items())
        sep = ', ' if args and kwargs else ''
        return f"{func}({args}{sep}{kwargs})"

    def __str__(self):
        return self.__repr__()

    # def __eq__(self, other):
    #     return type(self) == type(other) and self.args == other.args
