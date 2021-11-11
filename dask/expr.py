class Expr:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.func_name = self.__class__.__name__

    def __repr__(self):
        args = ', '.join(map(repr, self.args))
        kwargs = ', '.join(f"{key}={val}" for key, val in self.kwargs.items())
        sep = ', ' if args and kwargs else ''
        return f"{self.func_name}({args}{sep}{kwargs})"

    def __str__(self):
        return self.__repr__()

    # def __eq__(self, other):
    #     return type(self) == type(other) and self.args == other.args
